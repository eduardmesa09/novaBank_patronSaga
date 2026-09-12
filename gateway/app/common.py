"""Infraestructura compartida por copia (no por libreria): cada servicio es autonomo."""
import asyncio
import json
import os
import random
import uuid

from redis.asyncio import Redis
from sqlalchemy import create_engine, text

SERVICE_NAME = os.getenv("SERVICE_NAME", "unknown")
DATABASE_URL = os.getenv("DATABASE_URL", "")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
EVENT_STREAM = os.getenv("EVENT_STREAM", "novabank.events")

# Delays de 2 a 4 s exigidos por el taller: hacen visible cada micro-paso.
DELAY_MIN = float(os.getenv("STEP_DELAY_MIN", "2"))
DELAY_MAX = float(os.getenv("STEP_DELAY_MAX", "4"))

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
redis: Redis = Redis.from_url(REDIS_URL, decode_responses=True)


async def step_delay() -> float:
    """Pausa deliberada para poder apreciar el avance y la marcha atras."""
    seconds = random.uniform(DELAY_MIN, DELAY_MAX)
    await asyncio.sleep(seconds)
    return round(seconds, 2)


# --------------------------------------------------------------------------
# Idempotencia local (CP-05). Cada servicio la resuelve por si mismo.
# --------------------------------------------------------------------------
def recall(conn, saga_id: str, operation: str) -> dict | None:
    row = conn.execute(
        text("SELECT response FROM processed_operations "
             "WHERE saga_id = :s AND operation = :o"),
        {"s": saga_id, "o": operation},
    ).fetchone()
    return row[0] if row else None


def remember(conn, saga_id: str, operation: str, response: dict) -> None:
    conn.execute(
        text("INSERT INTO processed_operations (saga_id, operation, response) "
             "VALUES (:s, :o, CAST(:r AS jsonb)) ON CONFLICT DO NOTHING"),
        {"s": saga_id, "o": operation, "r": json.dumps(response)},
    )


# --------------------------------------------------------------------------
# Bus de eventos (Redis Streams) para la saga coreografiada.
# --------------------------------------------------------------------------
async def publish(event_type: str, saga_id: str, payload: dict | None = None) -> str:
    return await redis.xadd(EVENT_STREAM, {
        "event_id": str(uuid.uuid4()),
        "type": event_type,
        "saga_id": saga_id,
        "emitter": SERVICE_NAME,
        "payload": json.dumps(payload or {}),
    })


async def ensure_group(group: str) -> None:
    # id="$": un grupo nuevo arranca desde el presente. Con id="0" un servicio
    # que se reinicia reprocesaria todas las sagas historicas del stream.
    try:
        await redis.xgroup_create(EVENT_STREAM, group, id="$", mkstream=True)
    except Exception as exc:  # noqa: BLE001
        if "BUSYGROUP" not in str(exc):
            raise


async def consume(group: str, handler, interested: set[str]) -> None:
    """Bucle de consumo. Cada servicio reacciona solo a los eventos que le importan."""
    consumer = f"{group}-{uuid.uuid4().hex[:6]}"
    await ensure_group(group)

    while True:
        try:
            batches = await redis.xreadgroup(
                group, consumer, {EVENT_STREAM: ">"}, count=10, block=5000)
            for _stream, messages in batches or []:
                for msg_id, raw in messages:
                    try:
                        if raw.get("type") in interested:
                            await handler(
                                raw["type"], raw["saga_id"],
                                json.loads(raw.get("payload") or "{}"))
                    except Exception as exc:  # noqa: BLE001
                        print(f"[{group}] error procesando {msg_id}: {exc}", flush=True)
                    finally:
                        await redis.xack(EVENT_STREAM, group, msg_id)
        except Exception as exc:  # noqa: BLE001
            # Un FLUSHALL o un Redis recreado borran el grupo: hay que rehacerlo,
            # o el consumidor se queda girando en vacio para siempre.
            if "NOGROUP" in str(exc):
                print(f"[{group}] grupo perdido, recreando", flush=True)
                await ensure_group(group)
                continue
            print(f"[{group}] bus no disponible: {exc}", flush=True)
            await asyncio.sleep(2)
