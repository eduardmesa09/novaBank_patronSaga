#!/usr/bin/env bash
# Devuelve las cuatro bases a su estado semilla.
# Útil antes de grabar el video o de relanzar la matriz de pruebas.
#
# Cada dominio se limpia contra SU PROPIO contenedor de Postgres: no existe
# una conexión que las vea todas, y eso es precisamente la demostración del
# aislamiento de datos.
set -eu

RAIZ="$(cd "$(dirname "$0")/.." && pwd)"
cd "$RAIZ"

ejecutar() { # servicio  usuario  base  sql
  docker compose exec -T "db-$1" \
    psql -v ON_ERROR_STOP=1 -U "$2" -d "$3" -c "$4" >/dev/null
}

echo "Reiniciando el escenario"

ejecutar accounts accounts_user accounts_db \
  "TRUNCATE ledger_entries, processed_operations;"
ejecutar accounts accounts_user accounts_db "
  UPDATE accounts SET balance = v.balance, updated_at = now()
  FROM (VALUES
    ('ACC-1001',   5000.00),
    ('ACC-1002',   3200.00),
    ('ACC-1003', 150000.00),
    ('ACC-2001',  80000.00)
  ) AS v(id, balance)
  WHERE accounts.id = v.id;"
echo "  db-accounts  ·  cuentas y libro contable"

ejecutar risk risk_user risk_db "TRUNCATE risk_decisions, processed_operations;"
ejecutar risk risk_user risk_db \
  "UPDATE daily_limits SET used_today = 0, limit_date = CURRENT_DATE;"
echo "  db-risk      ·  decisiones y cupos diarios"

ejecutar clearing clearing_user clearing_db \
  "TRUNCATE settlements, clearing_attempts, processed_operations;"
echo "  db-clearing  ·  liquidaciones"

ejecutar saga saga_user saga_db "TRUNCATE saga_log, sagas;"
echo "  db-saga      ·  bitácora de sagas"

docker compose exec -T redis redis-cli FLUSHALL >/dev/null
echo "  redis        ·  bus de eventos vaciado"

echo "Escenario reiniciado. ACC-1001 vuelve a 5000.00 USD."
