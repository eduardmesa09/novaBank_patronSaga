"""Bitácora de la saga (saga_db).

Es la fuente del dashboard en tiempo real. Importante: en modo coreografía
esta bitácora la escribe un OBSERVADOR del bus de eventos, no un coordinador.
Si se apaga, la saga sigue funcionando igual.
"""
import json
from decimal import Decimal

from sqlalchemy import text

from .common import engine


def create_saga(saga_id: str, mode: str, from_account: str, to_account: str,
                amount: Decimal, chaos: dict) -> bool:
    """Devuelve False si la saga ya existía (reintento con el mismo UUID, CP-05)."""
    with engine.begin() as conn:
        result = conn.execute(
            text("INSERT INTO sagas "
                 "(saga_id, mode, from_account, to_account, amount, status, chaos) "
                 "VALUES (:s, :mo, :f, :t, :a, 'RECEIVED', CAST(:c AS jsonb)) "
                 "ON CONFLICT (saga_id) DO NOTHING"),
            {"s": saga_id, "mo": mode, "f": from_account, "t": to_account,
             "a": amount, "c": json.dumps(chaos)},
        )
        return result.rowcount == 1


def set_status(saga_id: str, status: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE sagas SET status = :st, updated_at = now() "
                 "WHERE saga_id = :s"),
            {"st": status, "s": saga_id},
        )


def log(saga_id: str, step: str, service: str, status: str,
        detail: str | None = None, payload: dict | None = None,
        is_compensation: bool = False) -> None:
    with engine.begin() as conn:
        seq = conn.execute(
            text("SELECT COALESCE(MAX(seq), 0) + 1 FROM saga_log WHERE saga_id = :s"),
            {"s": saga_id},
        ).scalar()
        conn.execute(
            text("INSERT INTO saga_log "
                 "(saga_id, seq, step, service, status, is_compensation, "
                 " detail, payload) "
                 "VALUES (:s, :q, :st, :sv, :stat, :comp, :d, CAST(:p AS jsonb))"),
            {"s": saga_id, "q": seq, "st": step, "sv": service, "stat": status,
             "comp": is_compensation, "d": detail,
             "p": json.dumps(payload or {}, default=str)},
        )


def get_saga(saga_id: str) -> dict | None:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT saga_id, mode, from_account, to_account, amount, status, "
                 "       chaos, created_at, updated_at "
                 "FROM sagas WHERE saga_id = :s"),
            {"s": saga_id},
        ).mappings().fetchone()
    if row is None:
        return None
    return dict(row) | {"amount": str(row["amount"])}


def get_log(saga_id: str, after_id: int = 0) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, seq, step, service, status, is_compensation, "
                 "       detail, payload, created_at "
                 "FROM saga_log WHERE saga_id = :s AND id > :a ORDER BY id"),
            {"s": saga_id, "a": after_id},
        ).mappings().all()
    return [dict(r) for r in rows]


def recent_sagas(limit: int = 20) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT saga_id, mode, from_account, to_account, amount, status, "
                 "       created_at "
                 "FROM sagas ORDER BY created_at DESC LIMIT :l"),
            {"l": limit},
        ).mappings().all()
    return [dict(r) | {"amount": str(r["amount"])} for r in rows]
