#!/usr/bin/env bash
# Matriz de casos de prueba CP-01 .. CP-05 contra el gateway.
# Uso:  bash scripts/run_test_matrix.sh [ORCHESTRATION|CHOREOGRAPHY]
set -u

GATEWAY="${GATEWAY:-http://localhost:8000}"
MODE="${1:-ORCHESTRATION}"

# La consola de Windows usa cp1252 y no puede imprimir los símbolos de reversa.
export PYTHONIOENCODING=utf-8

# Aritmética decimal sin depender del shell.
minus() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.2f", a - b }'; }

balance() {
  curl -s "$GATEWAY/accounts" \
    | python -c "import sys,json;print(next(a['balance'] for a in json.load(sys.stdin) if a['id']=='$1'))"
}

start_saga() {  # from to amount fraud timeout [saga_id]
  local body
  body=$(printf '{"from_account":"%s","to_account":"%s","amount":"%s","mode":"%s","force_fraud":%s,"force_timeout":%s%s}' \
    "$1" "$2" "$3" "$MODE" "$4" "$5" "${6:+,\"saga_id\":\"$6\"}")
  curl -s -X POST "$GATEWAY/transfers" -H 'Content-Type: application/json' -d "$body"
}

wait_saga() {  # saga_id -> imprime estado final
  for _ in $(seq 1 40); do
    sleep 2
    local status
    status=$(curl -s "$GATEWAY/sagas/$1" \
      | python -c "import sys,json;print(json.load(sys.stdin)['saga']['status'])" 2>/dev/null)
    case "$status" in
      CONFIRMADO*|RECHAZADO*_COMPENSADO) echo "$status"; return;;
      RECHAZADO_FONDOS) echo "$status"; return;;
    esac
  done
  echo "TIMEOUT"
}

trace() {
  curl -s "$GATEWAY/sagas/$1" | python -c "
import sys, json
log = json.load(sys.stdin)['log']
for s in log:
    mark = '  ↩' if s['is_compensation'] else '   '
    print(f\"{mark} {s['seq']:>2}. {s['step']:<12} {s['service']:<10} {s['status']}\")
"
}

case_report() {  # id descripcion saga_id esperado_estado cuenta saldo_esperado
  local status saldo veredicto
  status=$(wait_saga "$3")
  saldo=$(balance "$5")
  if [[ "$status" == $4* && "$saldo" == "$6" ]]; then veredicto="PASA"; else veredicto="FALLA"; fi
  echo ""
  echo "══ $1 · $2  [$veredicto]"
  echo "   estado: $status (esperado $4*)"
  echo "   $5: $saldo (esperado $6)"
  trace "$3"
}

echo "════════════════════════════════════════════════════════"
echo " Matriz de pruebas · modo $MODE"
echo "════════════════════════════════════════════════════════"

# La matriz asume el escenario semilla: sin esto los saldos se agotan
# entre corridas y CP-03/CP-04 degeneran en fondos insuficientes.
if [[ "${SKIP_RESET:-0}" != "1" ]]; then
  bash "$(dirname "$0")/reset_demo.sh"
fi

echo "Saldo inicial ACC-1001: $(balance ACC-1001)"

B0=$(balance ACC-1001)

# CP-01 · Camino feliz
SID=$(start_saga ACC-1001 ACC-1002 1200 false false | python -c "import sys,json;print(json.load(sys.stdin)['saga_id'])")
B1=$(minus "$B0" 1200)
case_report "CP-01" "Camino feliz" "$SID" "CONFIRMADO" ACC-1001 "$B1"

# CP-02 · Fondos insuficientes (sin compensaciones)
SID=$(start_saga ACC-1001 ACC-1002 999999 false false | python -c "import sys,json;print(json.load(sys.stdin)['saga_id'])")
case_report "CP-02" "Fondos insuficientes" "$SID" "RECHAZADO_FONDOS" ACC-1001 "$B1"

# CP-03 · Fallo de riesgo -> reembolso del débito
SID=$(start_saga ACC-1001 ACC-1002 1500 true false | python -c "import sys,json;print(json.load(sys.stdin)['saga_id'])")
case_report "CP-03" "Antifraude" "$SID" "RECHAZADO_RIESGO" ACC-1001 "$B1"

# CP-04 · Timeout de red -> revoca riesgo + reembolsa débito
SID=$(start_saga ACC-1001 ACC-1002 1500 false true | python -c "import sys,json;print(json.load(sys.stdin)['saga_id'])")
case_report "CP-04" "Caída de red interbancaria" "$SID" "RECHAZADO_RED" ACC-1001 "$B1"

# CP-05 · Idempotencia: mismo saga_id reenviado
SID=$(start_saga ACC-1001 ACC-1002 800 false false | python -c "import sys,json;print(json.load(sys.stdin)['saga_id'])")
wait_saga "$SID" >/dev/null
B2=$(balance ACC-1001)
echo ""
echo "══ CP-05 · Idempotencia ante reintentos"
echo "   saldo tras la primera ejecución: $B2"
for i in 1 2 3; do
  echo "   reintento $i: $(start_saga ACC-1001 ACC-1002 800 false false "$SID")"
done
sleep 3
B3=$(balance ACC-1001)
if [[ "$B2" == "$B3" ]]; then V="PASA"; else V="FALLA"; fi
echo "   saldo tras 3 reintentos: $B3  [$V]"
echo ""
echo "Fin de la matriz · modo $MODE"
