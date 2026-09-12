"""Servicio de Riesgo y Prevención de Fraude.

Dueño exclusivo de risk_db. Valida reglas operativas y límites diarios.
Su compensación revoca la aprobación y libera el cupo diario consumido.
"""
import asyncio
import contextlib
from decimal import Decimal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from .common import consume, engine, publish, recall, remember, step_delay

app = FastAPI(title="NovaBank · Risk & Fraud")


class AssessRequest(BaseModel):
    saga_id: str
    from_account: str
    amount: Decimal = Field(gt=0)
    force_fraud: bool = False  # switch de caos del frontend (CP-03)


class CompensateRequest(BaseModel):
    saga_id: str


def _assess(saga_id: str, account: str, amount: Decimal, force_fraud: bool) -> dict:
    with engine.begin() as conn:
        cached = recall(conn, saga_id, "assess")
        if cached:
            return {**cached, "idempotent_replay": True}

        def reject(reason: str, extra: dict | None = None) -> None:
            conn.execute(
                text("INSERT INTO risk_decisions "
                     "(saga_id, account_id, amount, decision, reason) "
                     "VALUES (:s, :a, :m, 'REJECTED', :r)"),
                {"s": saga_id, "a": account, "m": amount, "r": reason},
            )
            detail = {"code": "RISK_REJECTED", "reason": reason, **(extra or {})}
            remember(conn, saga_id, "assess", {"approved": False, **detail})
            raise HTTPException(409, detail)

        if force_fraud:
            reject("FRAUD_RULE_TRIGGERED",
                   {"rule": "patrón inusual detectado por el simulador de caos"})

        row = conn.execute(
            text("SELECT max_per_tx, max_per_day, used_today, limit_date "
                 "FROM daily_limits WHERE account_id = :a FOR UPDATE"),
            {"a": account},
        ).fetchone()
        if row is None:
            reject("NO_RISK_PROFILE")

        max_per_tx, max_per_day, used_today, limit_date = row

        # Reinicio del cupo si cambió el día.
        used = conn.execute(
            text("UPDATE daily_limits SET used_today = CASE "
                 "  WHEN limit_date < CURRENT_DATE THEN 0 ELSE used_today END, "
                 "  limit_date = CURRENT_DATE "
                 "WHERE account_id = :a RETURNING used_today"),
            {"a": account},
        ).scalar()

        if amount > max_per_tx:
            reject("PER_TX_LIMIT_EXCEEDED",
                   {"max_per_tx": str(max_per_tx), "requested": str(amount)})
        if used + amount > max_per_day:
            reject("DAILY_LIMIT_EXCEEDED",
                   {"max_per_day": str(max_per_day), "used_today": str(used),
                    "requested": str(amount)})

        conn.execute(
            text("UPDATE daily_limits SET used_today = used_today + :m "
                 "WHERE account_id = :a"),
            {"m": amount, "a": account},
        )
        conn.execute(
            text("INSERT INTO risk_decisions "
                 "(saga_id, account_id, amount, decision, reason) "
                 "VALUES (:s, :a, :m, 'APPROVED', 'ALL_RULES_PASSED')"),
            {"s": saga_id, "a": account, "m": amount},
        )
        response = {"approved": True, "account": account,
                    "daily_used_after": str(used + amount),
                    "max_per_day": str(max_per_day)}
        remember(conn, saga_id, "assess", response)
        return response


def _revoke(saga_id: str) -> dict:
    """Compensación: anula la aprobación y devuelve el cupo diario."""
    with engine.begin() as conn:
        cached = recall(conn, saga_id, "revoke")
        if cached:
            return {**cached, "idempotent_replay": True}

        row = conn.execute(
            text("SELECT account_id, amount FROM risk_decisions "
                 "WHERE saga_id = :s AND decision = 'APPROVED' "
                 "ORDER BY id DESC LIMIT 1"),
            {"s": saga_id},
        ).fetchone()
        if row is None:
            response = {"compensated": False, "reason": "NO_APPROVAL_TO_REVOKE"}
            remember(conn, saga_id, "revoke", response)
            return response

        account, amount = row
        conn.execute(
            text("UPDATE daily_limits "
                 "SET used_today = GREATEST(used_today - :m, 0) "
                 "WHERE account_id = :a"),
            {"m": amount, "a": account},
        )
        conn.execute(
            text("INSERT INTO risk_decisions "
                 "(saga_id, account_id, amount, decision, reason) "
                 "VALUES (:s, :a, :m, 'REVOKED', 'SAGA_COMPENSATION')"),
            {"s": saga_id, "a": account, "m": amount},
        )
        response = {"compensated": True, "account": account,
                    "quota_released": str(amount)}
        remember(conn, saga_id, "revoke", response)
        return response


# --------------------------------------------------------------------------
# API HTTP (saga ORQUESTADA)
# --------------------------------------------------------------------------
@app.post("/assess")
async def assess(req: AssessRequest):
    delay = await step_delay()
    result = _assess(req.saga_id, req.from_account, req.amount, req.force_fraud)
    return {**result, "delay_s": delay}


@app.post("/compensate/revoke")
async def compensate_revoke(req: CompensateRequest):
    delay = await step_delay()
    return {**_revoke(req.saga_id), "delay_s": delay}


@app.get("/decisions/{saga_id}")
async def decisions(saga_id: str):
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT decision, reason, amount, created_at FROM risk_decisions "
                 "WHERE saga_id = :s ORDER BY id"),
            {"s": saga_id},
        ).mappings().all()
    return [dict(r) | {"amount": str(r["amount"])} for r in rows]


@app.get("/limits")
async def limits():
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT account_id, max_per_tx, max_per_day, used_today, limit_date "
                 "FROM daily_limits ORDER BY account_id")
        ).mappings().all()
    return [
        {k: (str(v) if isinstance(v, Decimal) else v) for k, v in r.items()}
        for r in rows
    ]


@app.get("/health")
async def health():
    return {"service": "risk", "status": "up"}


# --------------------------------------------------------------------------
# Consumidor de eventos (saga COREOGRAFIADA)
# --------------------------------------------------------------------------
async def on_event(event_type: str, saga_id: str, payload: dict) -> None:
    if event_type == "BalanceDebited":
        await step_delay()
        try:
            result = _assess(saga_id, payload["from_account"],
                             Decimal(str(payload["amount"])),
                             bool(payload.get("force_fraud")))
        except HTTPException as exc:
            # Accounts reacciona a esto reembolsando su débito.
            await publish("RiskRejected", saga_id, {"detail": exc.detail})
            return
        await publish("RiskApproved", saga_id, {**payload, "result": result})

    # CP-04: la pasarela falló. Compensamos lo nuestro y avisamos;
    # ese aviso es lo que dispara el reembolso de accounts (orden inverso).
    elif event_type == "ClearingFailed":
        await step_delay()
        result = _revoke(saga_id)
        await publish("RiskRevoked", saga_id, {"result": result})


@app.on_event("startup")
async def start_consumer():
    app.state.consumer = asyncio.create_task(consume(
        group="risk-service",
        handler=on_event,
        interested={"BalanceDebited", "ClearingFailed"},
    ))


@app.on_event("shutdown")
async def stop_consumer():
    app.state.consumer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await app.state.consumer
