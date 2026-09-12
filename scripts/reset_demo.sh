#!/usr/bin/env bash
# Devuelve las cuatro bases de datos a su estado semilla.
# Útil antes de grabar el video o de relanzar la matriz de pruebas.
set -eu

psql() { docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d "$1" -c "$2" >/dev/null; }

echo "Reiniciando accounts_db…"
psql accounts_db "TRUNCATE ledger_entries, processed_operations;"
psql accounts_db "
  UPDATE accounts SET balance = v.balance, updated_at = now()
  FROM (VALUES
    ('ACC-1001',   5000.00),
    ('ACC-1002',   3200.00),
    ('ACC-1003', 150000.00),
    ('ACC-2001',  80000.00)
  ) AS v(id, balance)
  WHERE accounts.id = v.id;"

echo "Reiniciando risk_db…"
psql risk_db "TRUNCATE risk_decisions, processed_operations;"
psql risk_db "UPDATE daily_limits SET used_today = 0, limit_date = CURRENT_DATE;"

echo "Reiniciando clearing_db…"
psql clearing_db "TRUNCATE settlements, clearing_attempts, processed_operations;"

echo "Reiniciando saga_db…"
psql saga_db "TRUNCATE saga_log, sagas;"

echo "Vaciando el bus de eventos…"
docker compose exec -T redis redis-cli FLUSHALL >/dev/null

echo "Escenario reiniciado. ACC-1001 vuelve a 5000.00 USD."
