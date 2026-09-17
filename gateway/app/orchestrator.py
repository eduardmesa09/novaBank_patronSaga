"""Saga ORQUESTADA — flow de Prefect.

Un coordinador central conoce la secuencia completa y, ante un fallo,
invoca explícitamente las compensaciones en ORDEN INVERSO.

Contrasta con la coreografía: aquí el conocimiento del flujo está
concentrado en un solo lugar (acoplamiento al orquestador, control total),
mientras que en coreografía está distribuido entre los servicios
(bajo acoplamiento, control emergente).
"""
import json
import os
from decimal import Decimal

import httpx
from prefect import flow, task

from . import audit

ACCOUNTS_URL = os.getenv("ACCOUNTS_URL", "http://accounts:8001")
RISK_URL = os.getenv("RISK_URL", "http://risk:8002")
CLEARING_URL = os.getenv("CLEARING_URL", "http://clearing:8003")

# Debe superar el delay del paso (hasta 4 s) más el timeout simulado de red (3 s).
HTTP_TIMEOUT = httpx.Timeout(30.0)


class StepFailed(Exception):
    """Fallo de negocio de un paso: dispara la compensación en reversa."""

    def __init__(self, step: str, final_status: str, detail):
        super().__init__(f"{step}: {detail}")
        self.step = step
        self.final_status = final_status
        self.detail = detail


async def _post(url: str, body: dict) -> tuple[int, dict]:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        response = await client.post(url, json=body)
    try:
        payload = response.json()
    except ValueError:
        payload = {"raw": response.text}
    return response.status_code, payload


def _humanize(detail) -> str:
    """El detalle llega como dict; en pantalla debe leerse como una frase."""
    if not isinstance(detail, dict):
        return str(detail)
    code = detail.get("code")
    reason = detail.get("detail") or detail.get("reason")
    extras = {k: v for k, v in detail.items()
              if k not in {"code", "detail", "reason"}}
    parts = [p for p in (code, reason) if p]
    text = " · ".join(parts) if parts else json.dumps(detail, ensure_ascii=False)
    if extras:
        text += " (" + ", ".join(f"{k}: {v}" for k, v in extras.items()) + ")"
    return text


async def _run_step(saga_id: str, step: str, service: str, url: str, body: dict,
                    final_status_on_error: str) -> dict:
    audit.log(saga_id, step, service, "RUNNING")
    status_code, payload = await _post(url, body)

    if status_code >= 400:
        detail = payload.get("detail", payload)
        audit.log(saga_id, step, service, "FAILED", detail=_humanize(detail),
                  payload=payload)
        raise StepFailed(step, final_status_on_error, detail)

    if payload.get("idempotent_replay"):
        audit.log(saga_id, step, service, "REPLAY",
                  detail="duplicado reconocido, no se reejecutó", payload=payload)
    else:
        audit.log(saga_id, step, service, "SUCCESS", payload=payload)
    return payload


# --------------------------------------------------------------------------
# Transacciones locales
# --------------------------------------------------------------------------
@task(name="1 · Débito contable", retries=0)
async def debit(saga_id: str, from_account: str, amount: Decimal) -> dict:
    return await _run_step(
        saga_id, "DEBIT", "accounts", f"{ACCOUNTS_URL}/debit",
        {"saga_id": saga_id, "from_account": from_account, "amount": str(amount)},
        final_status_on_error="RECHAZADO_FONDOS",
    )


@task(name="2 · Evaluación de riesgo", retries=0)
async def assess_risk(saga_id: str, from_account: str, amount: Decimal,
                      force_fraud: bool) -> dict:
    return await _run_step(
        saga_id, "RISK", "risk", f"{RISK_URL}/assess",
        {"saga_id": saga_id, "from_account": from_account,
         "amount": str(amount), "force_fraud": force_fraud},
        final_status_on_error="RECHAZADO_RIESGO",
    )


@task(name="3 · Liquidación interbancaria", retries=0)
async def settle(saga_id: str, from_account: str, to_account: str,
                 amount: Decimal, force_timeout: bool) -> dict:
    return await _run_step(
        saga_id, "CLEARING", "clearing", f"{CLEARING_URL}/settle",
        {"saga_id": saga_id, "from_account": from_account,
         "to_account": to_account, "amount": str(amount),
         "force_timeout": force_timeout},
        final_status_on_error="RECHAZADO_RED",
    )


@task(name="4 · Crédito al destino", retries=0)
async def credit(saga_id: str, to_account: str, amount: Decimal) -> dict:
    return await _run_step(
        saga_id, "CREDIT", "accounts", f"{ACCOUNTS_URL}/credit",
        {"saga_id": saga_id, "to_account": to_account, "amount": str(amount)},
        final_status_on_error="RECHAZADO_CREDITO",
    )


# --------------------------------------------------------------------------
# Transacciones de compensación
# --------------------------------------------------------------------------
async def _compensate(saga_id: str, step: str, service: str, url: str) -> None:
    audit.log(saga_id, step, service, "COMPENSATING", is_compensation=True)
    status_code, payload = await _post(url, {"saga_id": saga_id})
    if status_code >= 400:
        # Compensación fallida: queda marcada para intervención manual.
        audit.log(saga_id, step, service, "FAILED", is_compensation=True,
                  detail=f"compensación fallida: {payload}", payload=payload)
        return
    audit.log(saga_id, step, service, "COMPENSATED", is_compensation=True,
              payload=payload)


@task(name="↩ Anular liquidación", retries=0)
async def cancel_settlement(saga_id: str) -> None:
    await _compensate(saga_id, "CLEARING", "clearing",
                      f"{CLEARING_URL}/compensate/cancel")


@task(name="↩ Revocar aprobación de riesgo", retries=0)
async def revoke_risk(saga_id: str) -> None:
    await _compensate(saga_id, "RISK", "risk", f"{RISK_URL}/compensate/revoke")


@task(name="↩ Reembolsar débito", retries=0)
async def refund_debit(saga_id: str) -> None:
    await _compensate(saga_id, "DEBIT", "accounts",
                      f"{ACCOUNTS_URL}/compensate/refund")


# --------------------------------------------------------------------------
# El flow
# --------------------------------------------------------------------------
@flow(name="NovaBank · Saga Orquestada", log_prints=True)
async def transfer_saga(saga_id: str, from_account: str, to_account: str,
                        amount: str, force_fraud: bool = False,
                        force_timeout: bool = False) -> dict:
    monto = Decimal(amount)

    # Pila de compensaciones: se vacía en orden inverso al de ejecución (LIFO).
    compensations: list = []

    try:
        await debit(saga_id, from_account, monto)
        compensations.append(refund_debit)

        await assess_risk(saga_id, from_account, monto, force_fraud)
        compensations.append(revoke_risk)

        await settle(saga_id, from_account, to_account, monto, force_timeout)
        compensations.append(cancel_settlement)

        await credit(saga_id, to_account, monto)

        audit.set_status(saga_id, "CONFIRMADO")
        print(f"[{saga_id}] saga confirmada")
        return {"saga_id": saga_id, "status": "CONFIRMADO"}

    except StepFailed as failure:
        audit.set_status(saga_id, failure.final_status)
        print(f"[{saga_id}] fallo en {failure.step}: {failure.detail}")

        if not compensations:
            # CP-02: el primer paso falló, no hay nada que revertir.
            audit.log(saga_id, "COMPENSATION", "orchestrator", "SKIPPED",
                      detail="ningún paso previo tuvo éxito: nada que compensar")
            return {"saga_id": saga_id, "status": failure.final_status,
                    "compensations": 0}

        # Compensación en orden estrictamente inverso.
        for compensate_step in reversed(compensations):
            await compensate_step(saga_id)

        audit.set_status(saga_id, f"{failure.final_status}_COMPENSADO")
        return {"saga_id": saga_id, "status": failure.final_status,
                "compensations": len(compensations)}
