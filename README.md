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
 └─────┬─────┘  └─────┬─────┘  └───────┬───────┘
       │              │                │
 ┌─────┴─────┐  ┌─────┴─────┐  ┌───────┴───────┐
 │db-accounts│  │  db-risk  │  │  db-clearing  │  ← un SERVIDOR Postgres
 │accounts_db│  │  risk_db  │  │  clearing_db  │    por servicio, con su
 └───────────┘  └───────────┘  └───────────────┘    propio volumen

              ┌──────────┐
              │ db-saga  │  ← bitácora de auditoría (saga_log)
              │ saga_db  │
              └──────────┘
```

Cada servicio conoce **únicamente** su propia `DATABASE_URL`, que apunta a su
propio contenedor de Postgres. No hay instancia compartida: un servicio no
puede alcanzar los datos de otro ni equivocándose, porque no existe credencial
ni ruta de red que se lo permita. No hay claves foráneas entre bases: la
correlación es por `saga_id`.

| Componente | Tecnología | Puerto |
| :--- | :--- | :--- |
| Frontend + simulador de caos | React 18 · Vite · TypeScript · SSE | 5173 |
| API Gateway | FastAPI · SSE | 8000 |
| Accounts & Ledger | FastAPI | 8001 |
| Risk & Fraud | FastAPI | 8002 |
| Clearing Gateway | FastAPI | 8003 |
| `db-accounts` · `accounts_db` | Postgres 16 | 5433 |
| `db-risk` · `risk_db` | Postgres 16 | 5434 |
| `db-clearing` · `clearing_db` | Postgres 16 | 5435 |
| `db-saga` · `saga_db` (bitácora) | Postgres 16 | 5436 |
| Bus de eventos | Redis Streams | 6379 |
| Observabilidad de flujos | Prefect 3 | 4200 |

## Puesta en marcha

Requisitos: Docker Desktop y Node 18+.

```bash
# 1. Backend completo (4 servidores de BD, 4 servicios, Redis y Prefect)
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

Cada contenedor de base de datos aplica su propio esquema al arrancar por
primera vez, desde su carpeta en [db/](db/). Los puertos 5433-5436 se publican
solo para poder inspeccionar cada base con un cliente SQL; los servicios se
hablan por la red interna de Compose.

## Uso

En el simulador: elige la modalidad (**Orquestación** o **Coreografía**), carga
un escenario de la matriz de pruebas o mueve los switches de caos, y pulsa
**Ejecutar transferencia**. La línea de tiempo se llena en vivo por SSE; cada
micro-paso tarda entre 2 y 4 segundos a propósito, para que la marcha atrás sea
visible. Cada paso ocupa **una sola fila que evoluciona** (en ejecución →
completado / fallido / compensado), y las compensaciones quedan marcadas con su
etiqueta y su icono de reversa.

El botón **Reintentar con el mismo identificador** reenvía el `saga_id` anterior
y demuestra que no hay doble cobro (CP-05).

La pestaña **Historial de operaciones** lista las sagas recientes con su estado;
desde ahí se abre la traza completa de cualquiera. Además, la operación activa
queda reflejada en la URL como `?saga=<uuid>`, así que una traza concreta se
puede recargar o compartir tal cual — útil para el video y para la defensa.

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

**Un servidor de base de datos por servicio, no una instancia compartida.**
Con cuatro contenedores de Postgres el aislamiento deja de depender de
permisos bien puestos y pasa a ser estructural: `risk_user` no tiene ruta de
red hacia `db-accounts`, así que un JOIN entre dominios no es que esté
prohibido, es que es imposible de escribir. Cuesta unos 200 MB de RAM más que
una instancia con cuatro bases, y a cambio la frontera es real.

**El código de infraestructura está duplicado a propósito.** `app/common.py` es
una copia en cada servicio, no una librería compartida. Una librería común
crearía un acoplamiento de despliegue entre servicios que deben poder
evolucionar por separado.

**No hay carpeta `saga/` con la coreografía.** La lógica coreografiada vive
dentro de cada servicio, en su función `on_event`. Su ausencia de un módulo
central es la demostración del patrón.

## Estructura

```
db/accounts/             esquema de accounts_db (lo aplica db-accounts)
db/risk/                 esquema de risk_db
db/clearing/             esquema de clearing_db
db/saga/                 esquema de saga_db
gateway/app/
  main.py                API, SSE y consumidor de auditoría
  orchestrator.py        flow de Prefect · saga ORQUESTADA
  audit.py               escritura de sagas y saga_log
  common.py              delays, idempotencia, bus
services/accounts/app/main.py   débito · crédito · reembolso  + on_event
services/risk/app/main.py       riesgo · revocación           + on_event
services/clearing/app/main.py   liquidación · anulación       + on_event
frontend/src/App.tsx     simulador de caos, línea de tiempo e historial
frontend/src/Icons.tsx   iconografía SVG propia
frontend/src/styles.css  sistema visual de NovaBank
scripts/                 matriz de pruebas y reinicio del escenario
docs/                    comparativa orquestación vs. coreografía
```
# Integrantes

- Eduard Meza Salazar
- Juan José Campos Covaleda   