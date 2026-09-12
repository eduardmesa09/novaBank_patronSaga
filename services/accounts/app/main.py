"""Servicio de Cuentas y Saldos (Account & Ledger).

Dueño exclusivo de accounts_db. Expone las transacciones locales
(débito, crédito) y su transacción de compensación (reembolso).
"""
import asyncio
import contextlib
from decimal import Decimal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from .common import consume, engine, publish, recall, remember, step_delay

app = FastAPI(title="NovaBank · Accounts & Ledger")


class DebitRequest(BaseModel):
    saga_id: str
    from_account: str
    amount: Decimal = Field(gt=0)


class CreditRequest(BaseModel):
    saga_id: str
    to_account: str
    amount: Decimal = Field(gt=0)


class CompensateRequest(BaseModel):
    saga_id: str


# --------------------------------------------------------------------------
# Transacciones locales
# --------------------------------------------------------------------------
def _debit(saga_id: str, account: str, amount: Decimal) -> dict:
    with engine.begin() as conn:
        cached = recall(conn, saga_id, "debit")
        if cached:
            return {**cached, "idempotent_replay": True}

        row = conn.execute(
            text("SELECT balance FROM accounts WHERE id = :a FOR UPDATE"),
            {"a": account},
        ).fetchone()
        if row is None:
            raise HTTPException(404, {"code": "ACCOUNT_NOT_FOUND", "account": account})

        balance = row[0]
        if balance < amount:
            rejection = {"code": "INSUFFICIENT_FUNDS", "balance": str(balance),
                         "requested": str(amount)}
            # CP-05: el rechazo también es idempotente.
            remember(conn, saga_id, "debit", {"accepted": False, **rejection})
            raise HTTPException(409, rejection)

        conn.execute(
            text("UPDATE accounts SET balance = balance - :m, updated_at = now() "
                 "WHERE id = :a"),
            {"m": amount, "a": account},
        )
        conn.execute(
            text("INSERT INTO ledger_entries (saga_id, account_id, kind, amount) "
                 "VALUES (:s, :a, 'DEBIT', :m)"),
            {"s": saga_id, "a": account, "m": amount},
        )
        response = {"accepted": True, "account": account, "debited": str(amount),
                    "balance_after": str(balance - amount)}
        remember(conn, saga_id, "debit", response)
        return response


def _credit(saga_id: str, account: str, amount: Decimal) -> dict:
    with engine.begin() as conn:
        cached = recall(conn, saga_id, "credit")
        if cached:
            return {**cached, "idempotent_replay": True}

        row = conn.execute(
            text("SELECT balance FROM accounts WHERE id = :a FOR UPDATE"),
            {"a": account},
        ).fetchone()
        if row is None:
            raise HTTPException(404, {"code": "ACCOUNT_NOT_FOUND", "account": account})

        conn.execute(
            text("UPDATE accounts SET balance = balance + :m, updated_at = now() "
                 "WHERE id = :a"),
            {"m": amount, "a": account},
        )
        conn.execute(
            text("INSERT INTO ledger_entries (saga_id, account_id, kind, amount) "
                 "VALUES (:s, :a, 'CREDIT', :m)"),
            {"s": saga_id, "a": account, "m": amount},
        )
        response = {"accepted": True, "account": account, "credited": str(amount),
                    "balance_after": str(row[0] + amount)}
        remember(conn, saga_id, "credit", response)
        return response


def _refund(saga_id: str) -> dict:
    """Compensación del débito. No borra el asiento: escribe su contrario."""
    with engine.begin() as conn:
        cached = recall(conn, saga_id, "refund")
        if cached:
            return {**cached, "idempotent_replay": True}

        rows = conn.execute(
            text("SELECT account_id, amount FROM ledger_entries "
                 "WHERE saga_id = :s AND kind = 'DEBIT'"),
            {"s": saga_id},
        ).fetchall()
        if not rows:
            response = {"compensated": False, "reason": "NOTHING_TO_REFUND"}
            remember(conn, saga_id, "refund", response)
            return response

        total = Decimal("0")
        for account, amount in rows:
            conn.execute(
                text("UPDATE accounts SET balance = balance + :m, updated_at = now() "
                     "WHERE id = :a"),
                {"m": amount, "a": account},
            )
            conn.execute(
                text("INSERT INTO ledger_entries (saga_id, account_id, kind, amount) "
                     "VALUES (:s, :a, 'REFUND', :m)"),
                {"s": saga_id, "a": account, "m": amount},
            )
            total += amount

        response = {"compensated": True, "refunded": str(total), "entries": len(rows)}
        remember(conn, saga_id, "refund", response)
        return response


# --------------------------------------------------------------------------
# API HTTP (la usa la saga ORQUESTADA)
# --------------------------------------------------------------------------
@app.post("/debit")
async def debit(req: DebitRequest):
    delay = await step_delay()
    return {**_debit(req.saga_id, req.from_account, req.amount), "delay_s": delay}


@app.post("/credit")
async def credit(req: CreditRequest):
    delay = await step_delay()
    return {**_credit(req.saga_id, req.to_account, req.amount), "delay_s": delay}


@app.post("/compensate/refund")
async def compensate_refund(req: CompensateRequest):
    delay = await step_delay()
    return {**_refund(req.saga_id), "delay_s": delay}


@app.get("/accounts")
async def list_accounts():
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, holder, balance, currency FROM accounts ORDER BY id")
        ).mappings().all()
    return [dict(r) | {"balance": str(r["balance"])} for r in rows]


@app.get("/ledger/{saga_id}")
async def ledger(saga_id: str):
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT kind, account_id, amount, created_at FROM ledger_entries "
                 "WHERE saga_id = :s ORDER BY id"),
            {"s": saga_id},
        ).mappings().all()
    return [dict(r) | {"amount": str(r["amount"])} for r in rows]


@app.get("/health")
async def health():
    return {"service": "accounts", "status": "up"}


# --------------------------------------------------------------------------
# Consumidor de eventos (la usa la saga COREOGRAFIADA)
# Sin coordinador: este servicio decide por sí mismo qué hacer con cada evento.
# --------------------------------------------------------------------------
async def on_event(event_type: str, saga_id: str, payload: dict) -> None:
    if event_type == "TransferRequested":
        await step_delay()
        try:
            result = _debit(saga_id, payload["from_account"],
                            Decimal(str(payload["amount"])))
        except HTTPException as exc:
            await publish("InsufficientFunds", saga_id, {"detail": exc.detail})
            return
        await publish("BalanceDebited", saga_id, {**payload, "result": result})

    # CP-03: riesgo rechaza -> compensamos nuestro débito.
    # CP-04: la cadena llega aquí DESPUÉS de que riesgo revocó su aprobación,
    #        garantizando el orden inverso sin necesidad de un coordinador.
    elif event_type in ("RiskRejected", "RiskRevoked"):
        await step_delay()
        result = _refund(saga_id)
        await publish("DebitRefunded", saga_id,
                      {"result": result, "triggered_by": event_type})

    elif event_type == "Settled":
        await step_delay()
        result = _credit(saga_id, payload["to_account"],
                         Decimal(str(payload["amount"])))
        await publish("TransferConfirmed", saga_id, {"result": result})


@app.on_event("startup")
async def start_consumer():
    app.state.consumer = asyncio.create_task(consume(
        group="accounts-service",
        handler=on_event,
        interested={"TransferRequested", "RiskRejected", "RiskRevoked", "Settled"},
    ))


@app.on_event("shutdown")
async def stop_consumer():
    app.state.consumer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await app.state.consumer
