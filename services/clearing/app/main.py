"""Pasarela Interbancaria (Clearing Gateway).

Dueño exclusivo de clearing_db. Simula la liquidación externa de fondos,
incluida la caída de red del CP-04. Su compensación anula la liquidación.
"""
import asyncio
import contextlib
import uuid
from decimal import Decimal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from .common import consume, engine, publish, recall, remember, step_delay

app = FastAPI(title="NovaBank · Clearing Gateway")

# Cuánto "cuelga" la red antes de rendirse, en el escenario CP-04.
NETWORK_TIMEOUT_S = 3.0


class SettleRequest(BaseModel):
    saga_id: str
    from_account: str
    to_account: str
    amount: Decimal = Field(gt=0)
    force_timeout: bool = False  # switch de caos del frontend (CP-04)


class CompensateRequest(BaseModel):
    saga_id: str


def _record_failure(saga_id: str, from_account: str, to_account: str,
                    amount: Decimal) -> dict:
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO settlements "
                 "(saga_id, from_account, to_account, amount, status) "
                 "VALUES (:s, :f, :t, :m, 'FAILED') "
                 "ON CONFLICT (saga_id) DO UPDATE "
                 "SET status = 'FAILED', updated_at = now()"),
            {"s": saga_id, "f": from_account, "t": to_account, "m": amount},
        )
        conn.execute(
            text("INSERT INTO clearing_attempts (saga_id, outcome, detail) "
                 "VALUES (:s, 'TIMEOUT', 'la red interbancaria no respondió')"),
            {"s": saga_id},
        )
        detail = {"code": "CLEARING_TIMEOUT",
                  "detail": "la red interbancaria no respondió",
                  "waited_s": NETWORK_TIMEOUT_S}
        remember(conn, saga_id, "settle", {"settled": False, **detail})
        return detail


def _settle_ok(saga_id: str, from_account: str, to_account: str,
               amount: Decimal) -> dict:
    external_ref = f"CLR-{uuid.uuid4().hex[:10].upper()}"
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO settlements "
                 "(saga_id, from_account, to_account, amount, status, external_ref) "
                 "VALUES (:s, :f, :t, :m, 'SETTLED', :r) "
                 "ON CONFLICT (saga_id) DO UPDATE "
                 "SET status = 'SETTLED', external_ref = EXCLUDED.external_ref, "
                 "    updated_at = now()"),
            {"s": saga_id, "f": from_account, "t": to_account,
             "m": amount, "r": external_ref},
        )
        conn.execute(
            text("INSERT INTO clearing_attempts (saga_id, outcome, detail) "
                 "VALUES (:s, 'SETTLED', :d)"),
            {"s": saga_id, "d": f"liquidado con referencia {external_ref}"},
        )
        response = {"settled": True, "external_ref": external_ref,
                    "amount": str(amount)}
        remember(conn, saga_id, "settle", response)
        return response


def _cached_settle(saga_id: str) -> dict | None:
    with engine.connect() as conn:
        return recall(conn, saga_id, "settle")


def _cancel(saga_id: str) -> dict:
    """Compensación: anula la liquidación previamente confirmada."""
    with engine.begin() as conn:
        cached = recall(conn, saga_id, "cancel")
        if cached:
            return {**cached, "idempotent_replay": True}

        row = conn.execute(
            text("SELECT status, external_ref FROM settlements "
                 "WHERE saga_id = :s FOR UPDATE"),
            {"s": saga_id},
        ).fetchone()
        if row is None or row[0] != "SETTLED":
            response = {"compensated": False, "reason": "NO_SETTLEMENT_TO_CANCEL"}
            remember(conn, saga_id, "cancel", response)
            return response

        conn.execute(
            text("UPDATE settlements SET status = 'CANCELLED', updated_at = now() "
                 "WHERE saga_id = :s"),
            {"s": saga_id},
        )
        conn.execute(
            text("INSERT INTO clearing_attempts (saga_id, outcome, detail) "
                 "VALUES (:s, 'CANCELLED', 'reversa solicitada por la saga')"),
            {"s": saga_id},
        )
        response = {"compensated": True, "cancelled_ref": row[1]}
        remember(conn, saga_id, "cancel", response)
        return response


# --------------------------------------------------------------------------
# API HTTP (saga ORQUESTADA)
# --------------------------------------------------------------------------
@app.post("/settle")
async def settle(req: SettleRequest):
    cached = _cached_settle(req.saga_id)
    if cached:
        if not cached.get("settled", False):
            raise HTTPException(504, {**cached, "idempotent_replay": True})
        return {**cached, "idempotent_replay": True}

    delay = await step_delay()

    if req.force_timeout:
        # La red externa cuelga y acabamos rindiéndonos.
        await asyncio.sleep(NETWORK_TIMEOUT_S)
        detail = _record_failure(req.saga_id, req.from_account,
                                 req.to_account, req.amount)
        raise HTTPException(504, detail)

    result = _settle_ok(req.saga_id, req.from_account, req.to_account, req.amount)
    return {**result, "delay_s": delay}


@app.post("/compensate/cancel")
async def compensate_cancel(req: CompensateRequest):
    delay = await step_delay()
    return {**_cancel(req.saga_id), "delay_s": delay}


@app.get("/settlements/{saga_id}")
async def settlement(saga_id: str):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT saga_id, status, external_ref, amount, updated_at "
                 "FROM settlements WHERE saga_id = :s"),
            {"s": saga_id},
        ).mappings().fetchone()
        attempts = conn.execute(
            text("SELECT outcome, detail, created_at FROM clearing_attempts "
                 "WHERE saga_id = :s ORDER BY id"),
            {"s": saga_id},
        ).mappings().all()
    if row is None:
        raise HTTPException(404, {"code": "NO_SETTLEMENT"})
    return {"settlement": dict(row) | {"amount": str(row["amount"])},
            "attempts": [dict(a) for a in attempts]}


@app.get("/health")
async def health():
    return {"service": "clearing", "status": "up"}


# --------------------------------------------------------------------------
# Consumidor de eventos (saga COREOGRAFIADA)
# --------------------------------------------------------------------------
async def on_event(event_type: str, saga_id: str, payload: dict) -> None:
    if event_type != "RiskApproved":
        return

    await step_delay()
    amount = Decimal(str(payload["amount"]))

    if payload.get("force_timeout"):
        await asyncio.sleep(NETWORK_TIMEOUT_S)
        detail = _record_failure(saga_id, payload["from_account"],
                                 payload["to_account"], amount)
        # Riesgo reacciona a esto revocando; luego accounts reembolsa.
        await publish("ClearingFailed", saga_id, {"detail": detail})
        return

    result = _settle_ok(saga_id, payload["from_account"],
                        payload["to_account"], amount)
    await publish("Settled", saga_id, {**payload, "result": result})


@app.on_event("startup")
async def start_consumer():
    app.state.consumer = asyncio.create_task(consume(
        group="clearing-service",
        handler=on_event,
        interested={"RiskApproved"},
    ))


@app.on_event("shutdown")
async def stop_consumer():
    app.state.consumer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await app.state.consumer
