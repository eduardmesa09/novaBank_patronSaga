"""API Gateway.

Punto de entrada único: recibe la petición del frontend, emite el UUID de
idempotencia y despacha la saga en el modo elegido (orquestación o coreografía).

Además expone la bitácora por SSE para el dashboard en tiempo real, y ejecuta
un consumidor de AUDITORÍA que observa el bus de eventos. Ese consumidor no
coordina nada: solo registra lo que los servicios deciden por su cuenta.
"""
import asyncio
import contextlib
import json
import os
import uuid
from decimal import Decimal

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from . import audit
from .common import consume, publish
from .orchestrator import transfer_saga

ACCOUNTS_URL = os.getenv("ACCOUNTS_URL", "http://accounts:8001")
RISK_URL = os.getenv("RISK_URL", "http://risk:8002")
CLEARING_URL = os.getenv("CLEARING_URL", "http://clearing:8003")

app = FastAPI(title="NovaBank International · Saga Gateway")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # taller local
    allow_methods=["*"],
    allow_headers=["*"],
)


class TransferRequest(BaseModel):
    from_account: str
    to_account: str
    amount: Decimal = Field(gt=0)
    mode: str = Field(default="ORCHESTRATION",
                      pattern="^(ORCHESTRATION|CHOREOGRAPHY)$")
    # Simulador de caos
    force_fraud: bool = False
    force_timeout: bool = False
    # CP-05: si el cliente reenvía el mismo saga_id, es un reintento.
    saga_id: str | None = None


@app.post("/transfers", status_code=202)
async def create_transfer(req: TransferRequest):
    saga_id = req.saga_id or str(uuid.uuid4())
    chaos = {"force_fraud": req.force_fraud, "force_timeout": req.force_timeout}

    is_new = audit.create_saga(saga_id, req.mode, req.from_account,
                               req.to_account, req.amount, chaos)

    if not is_new:
        # CP-05: duplicado reconocido en la puerta, sin doble cobro.
        existing = audit.get_saga(saga_id)
        audit.log(saga_id, "IDEMPOTENCY", "gateway", "REPLAY",
                  detail="saga_id ya procesado: la petición no se reejecuta")
        return {"saga_id": saga_id, "duplicate": True,
                "status": existing["status"] if existing else "UNKNOWN",
                "message": "reintento reconocido, no se ejecutó de nuevo"}

    modo = "orquestación" if req.mode == "ORCHESTRATION" else "coreografía"
    audit.log(saga_id, "REQUEST", "gateway", "SUCCESS",
              detail=f"transferencia recibida · modo {modo}",
              payload={"amount": str(req.amount), **chaos})

    if req.mode == "ORCHESTRATION":
        audit.set_status(saga_id, "EN_EJECUCION")
        # El flow corre en segundo plano; el frontend sigue el avance por SSE.
        asyncio.create_task(transfer_saga(
            saga_id=saga_id,
            from_account=req.from_account,
            to_account=req.to_account,
            amount=str(req.amount),
            force_fraud=req.force_fraud,
            force_timeout=req.force_timeout,
        ))
    else:
        audit.set_status(saga_id, "EN_EJECUCION")
        # Un solo evento al bus. A partir de aquí nadie manda: los servicios reaccionan.
        await publish("TransferRequested", saga_id, {
            "from_account": req.from_account,
            "to_account": req.to_account,
            "amount": str(req.amount),
            "force_fraud": req.force_fraud,
            "force_timeout": req.force_timeout,
        })

    return {"saga_id": saga_id, "duplicate": False, "mode": req.mode,
            "status": "EN_EJECUCION"}


@app.get("/sagas")
async def list_sagas():
    return audit.recent_sagas()


@app.get("/sagas/{saga_id}")
async def get_saga(saga_id: str):
    saga = audit.get_saga(saga_id)
    if saga is None:
        raise HTTPException(404, {"code": "SAGA_NOT_FOUND"})
    return {"saga": saga, "log": audit.get_log(saga_id)}


TERMINAL_PREFIXES = ("CONFIRMADO", "RECHAZADO")


@app.get("/sagas/{saga_id}/stream")
async def stream_saga(saga_id: str):
    """SSE: empuja cada nuevo paso de la bitácora al dashboard."""
    async def event_source():
        last_id = 0
        idle_after_terminal = 0

        while True:
            saga = audit.get_saga(saga_id)
            if saga is None:
                yield f"event: error\ndata: {json.dumps({'code': 'NOT_FOUND'})}\n\n"
                return

            for row in audit.get_log(saga_id, after_id=last_id):
                last_id = row["id"]
                yield f"event: step\ndata: {json.dumps(row, default=str)}\n\n"

            yield f"event: saga\ndata: {json.dumps(saga, default=str)}\n\n"

            terminal = saga["status"].startswith(TERMINAL_PREFIXES)
            if terminal:
                # Margen para que lleguen las compensaciones pendientes.
                idle_after_terminal += 1
                if idle_after_terminal > 12:
                    yield "event: done\ndata: {}\n\n"
                    return
            else:
                idle_after_terminal = 0

            await asyncio.sleep(0.4)

    return StreamingResponse(event_source(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


@app.get("/accounts")
async def accounts_snapshot():
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(f"{ACCOUNTS_URL}/accounts")
    return response.json()


@app.get("/limits")
async def limits_snapshot():
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(f"{RISK_URL}/limits")
    return response.json()


@app.get("/health")
async def health():
    return {"service": "gateway", "status": "up"}


# --------------------------------------------------------------------------
# Consumidor de AUDITORÍA para la saga coreografiada.
# Observador puro: traduce eventos del bus a filas de saga_log.
# --------------------------------------------------------------------------
EVENT_MAP = {
    # TransferRequested no está aquí: ya lo registra create_transfer al publicarlo.
    "BalanceDebited": ("DEBIT", "accounts", "SUCCESS", False, None),
    "InsufficientFunds": ("DEBIT", "accounts", "FAILED", False, "RECHAZADO_FONDOS"),
    "RiskApproved": ("RISK", "risk", "SUCCESS", False, None),
    "RiskRejected": ("RISK", "risk", "FAILED", False, "RECHAZADO_RIESGO"),
    "Settled": ("CLEARING", "clearing", "SUCCESS", False, None),
    "ClearingFailed": ("CLEARING", "clearing", "FAILED", False, "RECHAZADO_RED"),
    "RiskRevoked": ("RISK", "risk", "COMPENSATED", True, None),
    "DebitRefunded": ("DEBIT", "accounts", "COMPENSATED", True, None),
    "TransferConfirmed": ("CREDIT", "accounts", "SUCCESS", False, "CONFIRMADO"),
}


async def on_event(event_type: str, saga_id: str, payload: dict) -> None:
    mapping = EVENT_MAP.get(event_type)
    if mapping is None:
        return
    step, service, status, is_comp, final_status = mapping

    audit.log(saga_id, step, service, status, is_compensation=is_comp,
              detail=f"evento {event_type}", payload=payload)

    if final_status:
        audit.set_status(saga_id, final_status)

    # La última compensación de la cadena cierra la saga como compensada.
    if event_type == "DebitRefunded":
        saga = audit.get_saga(saga_id)
        if saga and saga["status"].startswith("RECHAZADO"):
            audit.set_status(saga_id, f"{saga['status']}_COMPENSADO")


@app.on_event("startup")
async def start_audit_consumer():
    app.state.consumer = asyncio.create_task(consume(
        group="gateway-audit",
        handler=on_event,
        interested=set(EVENT_MAP),
    ))


@app.on_event("shutdown")
async def stop_audit_consumer():
    app.state.consumer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await app.state.consumer
