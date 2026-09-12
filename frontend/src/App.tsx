import { useEffect, useRef, useState } from "react";

const GATEWAY = import.meta.env.VITE_GATEWAY_URL ?? "http://localhost:8000";

type Mode = "ORCHESTRATION" | "CHOREOGRAPHY";

type Step = {
  id: number;
  seq: number;
  step: string;
  service: string;
  status:
    | "RUNNING"
    | "SUCCESS"
    | "FAILED"
    | "COMPENSATING"
    | "COMPENSATED"
    | "SKIPPED"
    | "REPLAY";
  is_compensation: boolean;
  detail: string | null;
  payload: Record<string, unknown>;
  created_at: string;
};

type Saga = { saga_id: string; mode: Mode; status: string; amount: string };
type Account = { id: string; holder: string; balance: string; currency: string };

const PRESETS = [
  { id: "CP-01", label: "Camino feliz", amount: "1200", fraud: false, timeout: false },
  { id: "CP-02", label: "Fondos insuficientes", amount: "99000", fraud: false, timeout: false },
  { id: "CP-03", label: "Fallo de riesgo", amount: "1500", fraud: true, timeout: false },
  { id: "CP-04", label: "Caída de red", amount: "1500", fraud: false, timeout: true },
];

const STATUS_STYLE: Record<Step["status"], string> = {
  RUNNING: "running",
  SUCCESS: "success",
  FAILED: "failed",
  COMPENSATING: "compensating",
  COMPENSATED: "compensated",
  SKIPPED: "skipped",
  REPLAY: "replay",
};

export default function App() {
  const [from, setFrom] = useState("ACC-1001");
  const [to, setTo] = useState("ACC-1002");
  const [amount, setAmount] = useState("1200");
  const [mode, setMode] = useState<Mode>("ORCHESTRATION");
  const [forceFraud, setForceFraud] = useState(false);
  const [forceTimeout, setForceTimeout] = useState(false);

  const [saga, setSaga] = useState<Saga | null>(null);
  const [steps, setSteps] = useState<Step[]>([]);
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [lastSagaId, setLastSagaId] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [running, setRunning] = useState(false);

  const sourceRef = useRef<EventSource | null>(null);

  const loadAccounts = async () => {
    const response = await fetch(`${GATEWAY}/accounts`);
    setAccounts(await response.json());
  };

  useEffect(() => {
    loadAccounts();
    return () => sourceRef.current?.close();
  }, []);

  const subscribe = (sagaId: string) => {
    sourceRef.current?.close();
    const source = new EventSource(`${GATEWAY}/sagas/${sagaId}/stream`);
    sourceRef.current = source;

    source.addEventListener("step", (event) => {
      const step = JSON.parse((event as MessageEvent).data) as Step;
      setSteps((prev) => (prev.some((s) => s.id === step.id) ? prev : [...prev, step]));
    });
    source.addEventListener("saga", (event) => {
      setSaga(JSON.parse((event as MessageEvent).data) as Saga);
    });
    source.addEventListener("done", () => {
      source.close();
      setRunning(false);
      loadAccounts();
    });
    source.onerror = () => {
      source.close();
      setRunning(false);
    };
  };

  const submit = async (sagaId?: string) => {
    setNotice(null);
    if (!sagaId) setSteps([]);
    setRunning(true);

    const response = await fetch(`${GATEWAY}/transfers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        from_account: from,
        to_account: to,
        amount,
        mode,
        force_fraud: forceFraud,
        force_timeout: forceTimeout,
        saga_id: sagaId ?? null,
      }),
    });
    const data = await response.json();

    if (data.duplicate) {
      setNotice(
        `CP-05 · duplicado reconocido: la saga ${data.saga_id.slice(0, 8)} ya estaba en estado ${data.status}. No se ejecutó de nuevo.`,
      );
    }
    setLastSagaId(data.saga_id);
    subscribe(data.saga_id);
    loadAccounts();
  };

  const applyPreset = (preset: (typeof PRESETS)[number]) => {
    setAmount(preset.amount);
    setForceFraud(preset.fraud);
    setForceTimeout(preset.timeout);
    setNotice(`Escenario ${preset.id} cargado: ${preset.label}. Pulsa «Ejecutar transferencia».`);
  };

  const finalStatus = saga?.status ?? "—";
  const isTerminal = finalStatus.startsWith("CONFIRMADO") || finalStatus.startsWith("RECHAZADO");

  return (
    <div className="page">
      <header>
        <h1>NovaBank International</h1>
        <p>Simulador del Patrón Saga · orquestación vs. coreografía</p>
      </header>

      <main>
        <section className="panel">
          <h2>Transferencia interbancaria</h2>

          <div className="field-row">
            <label>
              Cuenta origen
              <input value={from} onChange={(e) => setFrom(e.target.value)} />
            </label>
            <label>
              Cuenta destino
              <input value={to} onChange={(e) => setTo(e.target.value)} />
            </label>
            <label>
              Importe
              <input
                type="number"
                min="1"
                step="0.01"
                value={amount}
                onChange={(e) => setAmount(e.target.value)}
              />
            </label>
          </div>

          <h3>Modalidad de saga</h3>
          <div className="mode-toggle">
            <button
              className={mode === "ORCHESTRATION" ? "active" : ""}
              onClick={() => setMode("ORCHESTRATION")}
            >
              Orquestación
              <small>coordinador central (Prefect)</small>
            </button>
            <button
              className={mode === "CHOREOGRAPHY" ? "active" : ""}
              onClick={() => setMode("CHOREOGRAPHY")}
            >
              Coreografía
              <small>eventos, sin coordinador</small>
            </button>
          </div>

          <h3>Simulador de caos</h3>
          <div className="switches">
            <label className="switch">
              <input
                type="checkbox"
                checked={forceFraud}
                onChange={(e) => setForceFraud(e.target.checked)}
              />
              <span>Forzar alerta de fraude · CP-03</span>
            </label>
            <label className="switch">
              <input
                type="checkbox"
                checked={forceTimeout}
                onChange={(e) => setForceTimeout(e.target.checked)}
              />
              <span>Forzar timeout interbancario · CP-04</span>
            </label>
          </div>

          <h3>Escenarios de la matriz de pruebas</h3>
          <div className="presets">
            {PRESETS.map((preset) => (
              <button key={preset.id} className="preset" onClick={() => applyPreset(preset)}>
                <strong>{preset.id}</strong>
                {preset.label}
              </button>
            ))}
          </div>

          <div className="actions">
            <button className="primary" disabled={running} onClick={() => submit()}>
              {running ? "Saga en ejecución…" : "Ejecutar transferencia"}
            </button>
            <button
              className="secondary"
              disabled={!lastSagaId || running}
              onClick={() => submit(lastSagaId!)}
              title="Reenvía el mismo saga_id para comprobar la idempotencia"
            >
              CP-05 · Reintentar mismo UUID
            </button>
          </div>

          {notice && <p className="notice">{notice}</p>}
        </section>

        <section className="panel">
          <h2>
            Avance de la saga
            {saga && <span className={`badge ${isTerminal ? "terminal" : "live"}`}>{finalStatus}</span>}
          </h2>

          {lastSagaId && (
            <p className="saga-id">
              saga_id (clave de idempotencia): <code>{lastSagaId}</code>
              {saga && <> · modo <strong>{saga.mode === "ORCHESTRATION" ? "orquestación" : "coreografía"}</strong></>}
            </p>
          )}

          {steps.length === 0 && <p className="empty">Sin pasos aún. Lanza una transferencia.</p>}

          <ol className="timeline">
            {steps.map((step) => (
              <li key={step.id} className={`${STATUS_STYLE[step.status]} ${step.is_compensation ? "comp" : ""}`}>
                <div className="step-head">
                  <span className="step-name">
                    {step.is_compensation && <span className="rev">↩</span>}
                    {step.step}
                  </span>
                  <span className="step-service">{step.service}</span>
                  <span className="step-status">{step.status}</span>
                </div>
                {step.detail && <p className="step-detail">{step.detail}</p>}
                {Object.keys(step.payload ?? {}).length > 0 && (
                  <pre>{JSON.stringify(step.payload, null, 2)}</pre>
                )}
              </li>
            ))}
          </ol>
        </section>

        <section className="panel">
          <h2>
            Saldos <button className="link" onClick={loadAccounts}>refrescar</button>
          </h2>
          <table>
            <thead>
              <tr>
                <th>Cuenta</th>
                <th>Titular</th>
                <th>Saldo</th>
              </tr>
            </thead>
            <tbody>
              {accounts.map((account) => (
                <tr key={account.id}>
                  <td><code>{account.id}</code></td>
                  <td>{account.holder}</td>
                  <td className="money">
                    {Number(account.balance).toLocaleString("es-CO", {
                      style: "currency",
                      currency: account.currency,
                    })}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="hint">
            Consistencia final: tras una compensación completa los saldos deben volver
            exactamente a su valor previo.
          </p>
        </section>
      </main>

      <footer>
        <a href="http://localhost:4200" target="_blank" rel="noreferrer">Prefect UI</a>
        <a href="http://localhost:8000/docs" target="_blank" rel="noreferrer">Gateway · OpenAPI</a>
      </footer>
    </div>
  );
}
