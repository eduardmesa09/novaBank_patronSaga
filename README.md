# NovaBank International · Patrón Saga

Implementación del patrón Saga para transferencias interbancarias, bajo modelo
BASE, con las dos modalidades de coordinación (**orquestación** y
**coreografía**), compensaciones en orden inverso, idempotencia y observabilidad
paso a paso.

## Arquitectura

```
                    ┌──────────────────────────┐
  React + Vite ───► │  API Gateway  (FastAPI)  │
   (SSE en vivo)    │  emite el saga_id (UUID) │
                    └────┬────────────────┬────┘
                         │                │
          ORQUESTACIÓN   │                │  COREOGRAFÍA
       flow de Prefect   │                │  1 evento al bus
       manda paso a paso │                │  y nadie más manda
                         ▼                ▼
                                   ┌─────────────┐
       ┌──── HTTP ─────────────►   │ Redis       │
       │                           │ Streams     │
       │                           └──┬───┬───┬──┘
       ▼                              ▼   ▼   ▼
 ┌───────────┐  ┌───────────┐  ┌───────────────┐
 │ Accounts  │  │ Risk      │  │ Clearing      │
 │ & Ledger  │  │ & Fraud   │  │ Gateway       │
 ├───────────┤  ├───────────┤  ├───────────────┤
 │accounts_db│  │  risk_db  │  │  clearing_db  │   ← una BD por servicio
 └───────────┘  └───────────┘  └───────────────┘

                 saga_db  ← bitácora de auditoría (saga_log)
```

Cada servicio conoce **únicamente** su propia `DATABASE_URL`. No hay claves
foráneas entre bases: la correlación es por `saga_id`.

| Componente | Tecnología | Puerto |
| :--- | :--- | :--- |
| Frontend + simulador de caos | React 18 · Vite · TypeScript | 5173 |
| API Gateway | FastAPI · SSE | 8000 |
| Accounts & Ledger | FastAPI · `accounts_db` | 8001 |
| Risk & Fraud | FastAPI · `risk_db` | 8002 |
| Clearing Gateway | FastAPI · `clearing_db` | 8003 |
| Bitácora de saga | `saga_db` | — |
| Bus de eventos | Redis Streams | 6379 |
| Observabilidad de flujos | Prefect 3 | 4200 |

## Puesta en marcha

Requisitos: Docker Desktop y Node 18+.

```bash
# 1. Backend completo (4 bases de datos, 4 servicios, Redis y Prefect)
docker compose up -d --build

# 2. Frontend
cd frontend
npm install
npm run dev
```

| Interfaz | URL |
| :--- | :--- |
| Simulador | http://localhost:5173 |
| Gateway · OpenAPI | http://localhost:8000/docs |
| Prefect UI | http://localhost:4200 |

Las bases de datos se crean y se siembran solas en el primer arranque
(los scripts de [db/](db/) se ejecutan en orden).

## Uso

En el simulador: elige la modalidad (**Orquestación** o **Coreografía**), carga
un escenario de la matriz de pruebas o mueve los switches de caos, y pulsa
**Ejecutar transferencia**. La línea de tiempo se llena en vivo por SSE; cada
micro-paso tarda entre 2 y 4 segundos a propósito, para que la marcha atrás sea
visible.

El botón **CP-05 · Reintentar mismo UUID** reenvía el `saga_id` anterior y
demuestra que no hay doble cobro.

### Scripts

```bash
# Matriz completa CP-01..CP-05 con verificación de saldos
bash scripts/run_test_matrix.sh ORCHESTRATION
bash scripts/run_test_matrix.sh CHOREOGRAPHY

# Devolver el escenario a su estado semilla (antes de grabar el video)
bash scripts/reset_demo.sh
```

`run_test_matrix.sh` reinicia el escenario por su cuenta; usa `SKIP_RESET=1`
para conservar el estado actual.

## Matriz de casos de prueba

Resultado verificado en ambas modalidades (saldo de partida de `ACC-1001`:
5000.00 USD):

| Caso | Escenario | Estado final | Compensaciones | Saldo final |
| :--- | :--- | :--- | :--- | :--- |
| **CP-01** | Camino feliz | `CONFIRMADO` | ninguna | 3800.00 (−1200) |
| **CP-02** | Fondos insuficientes | `RECHAZADO_FONDOS` | ninguna (`SKIPPED`) | 3800.00 intacto |
| **CP-03** | Antifraude | `RECHAZADO_RIESGO_COMPENSADO` | reembolso del débito | 3800.00 restituido |
| **CP-04** | Caída de red externa | `RECHAZADO_RED_COMPENSADO` | revoca riesgo → reembolsa débito | 3800.00 restituido |
| **CP-05** | Reintentos | sin cambios | duplicado reconocido | inalterado |

En CP-04 el orden de la bitácora es la evidencia clave: `RISK:COMPENSATED`
aparece **antes** de `DEBIT:COMPENSATED`, es decir, exactamente el inverso de la
ejecución.

## Decisiones de diseño

**El libro contable no se borra.** Una compensación nunca hace `DELETE` ni
revierte el asiento original: escribe un asiento `REFUND` que lo neutraliza.
El saldo vuelve a su valor previo y la auditoría conserva todo lo ocurrido.

**La idempotencia es local.** Cada servicio tiene su propia tabla
`processed_operations` con clave primaria `(saga_id, operation)`. No depende del
orquestador, así que funciona igual en coreografía. Los rechazos también se
memorizan: reintentar una operación rechazada devuelve el mismo rechazo sin
reevaluar.

**La bitácora no coordina.** En coreografía, `saga_log` la escribe un consumidor
de auditoría del gateway que solo observa el bus. Si se apaga, la saga sigue
funcionando: se pierde la visibilidad, no la transacción.

**El código de infraestructura está duplicado a propósito.** `app/common.py` es
una copia en cada servicio, no una librería compartida. Una librería común
crearía un acoplamiento de despliegue entre servicios que deben poder
evolucionar por separado.

**No hay carpeta `saga/` con la coreografía.** La lógica coreografiada vive
dentro de cada servicio, en su función `on_event`. Su ausencia de un módulo
central es la demostración del patrón.

## Estructura

```
db/                      esquemas de las 4 bases (se aplican al arrancar)
gateway/app/
  main.py                API, SSE y consumidor de auditoría
  orchestrator.py        flow de Prefect · saga ORQUESTADA
  audit.py               escritura de sagas y saga_log
  common.py              delays, idempotencia, bus
services/accounts/app/main.py   débito · crédito · reembolso  + on_event
services/risk/app/main.py       riesgo · revocación           + on_event
services/clearing/app/main.py   liquidación · anulación       + on_event
frontend/src/App.tsx     simulador de caos y línea de tiempo
scripts/                 matriz de pruebas y reinicio del escenario
docs/                    comparativa orquestación vs. coreografía
```

## Migración a Supabase

Las cuatro bases están pensadas para convertirse en cuatro proyectos Supabase
sin tocar código de dominio:

1. Crear los proyectos `novabank-accounts`, `novabank-risk`,
   `novabank-clearing` y `novabank-saga`.
2. Aplicar en cada uno su script de [db/](db/) (sin las líneas `\connect`,
   `SET ROLE` ni el `00_databases.sql`).
3. Sustituir cada `DATABASE_URL` del `docker-compose.yml` por la cadena de
   **Supavisor session mode** del proyecto correspondiente (host
   `*.pooler.supabase.com`; la conexión directa al 5432 es solo IPv6).
4. Retirar el servicio `postgres` del compose.
5. Opcional: reemplazar el SSE de `/sagas/{id}/stream` por Supabase Realtime
   suscrito a `saga_log`, con RLS de solo lectura para la clave `anon`.
