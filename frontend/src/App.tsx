import { useEffect, useMemo, useRef, useState } from "react";
import {
  Bank,
  BrandMark,
  CheckCircle,
  ChevronRight,
  Clock,
  MinusCircle,
  NetworkDown,
  Repeat,
  ShieldAlert,
  Undo,
  Wallet,
  XCircle,
} from "./Icons";

const GATEWAY = import.meta.env.VITE_GATEWAY_URL ?? "http://localhost:8000";

type Mode = "ORCHESTRATION" | "CHOREOGRAPHY";
type StepStatus =
  | "RUNNING"
  | "SUCCESS"
  | "FAILED"
  | "COMPENSATING"
  | "COMPENSATED"
  | "SKIPPED"
  | "REPLAY";

type Step = {
  id: number;
  seq: number;
  step: string;
  service: string;
  status: StepStatus;
  is_compensation: boolean;
  detail: string | null;
  payload: Record<string, unknown>;
  created_at: string;
};

type Saga = {
  saga_id: string;
  mode: Mode;
  status: string;
  amount: string;
  from_account: string;
  to_account: string;
  created_at?: string;
};

type Account = { id: string; holder: string; balance: string; currency: string };

type Scenario = {
  id: string;
  label: string;
  hint: string;
  amount: string;
  fraud: boolean;
  timeout: boolean;
  repeat?: boolean;
  Icon: (props: { className?: string }) => JSX.Element;
};

const SCENARIOS: Scenario[] = [
  {
    id: "CP-01",
    label: "Camino feliz",
    hint: "La transferencia se confirma sin incidencias.",
    amount: "1200",
    fraud: false,
    timeout: false,
    Icon: CheckCircle,
  },
  {
    id: "CP-02",
    label: "Fondos insuficientes",
    hint: "Se rechaza de entrada: no hay nada que compensar.",
    amount: "99000",
    fraud: false,
    timeout: false,
    Icon: Wallet,
  },
  {
    id: "CP-03",
    label: "Alerta de fraude",
    hint: "Riesgo bloquea y se reembolsa el débito.",
    amount: "1500",
    fraud: true,
    timeout: false,
    Icon: ShieldAlert,
  },
  {
    id: "CP-04",
    label: "Caída de la red",
    hint: "Se revoca el riesgo y luego se reembolsa el débito.",
    amount: "1500",
    fraud: false,
    timeout: true,
    Icon: NetworkDown,
  },
  {
    id: "CP-05",
    label: "Reintento duplicado",
    hint: "Mismo identificador: se reconoce sin doble cobro.",
    amount: "800",
    fraud: false,
    timeout: false,
    repeat: true,
    Icon: Repeat,
  },
];

const STEP_LABEL: Record<string, string> = {
  REQUEST: "Solicitud recibida",
  DEBIT: "Débito contable",
  RISK: "Evaluación de riesgo",
  CLEARING: "Liquidación interbancaria",
  CREDIT: "Crédito al destino",
  COMPENSATION: "Compensación",
  IDEMPOTENCY: "Control de idempotencia",
};

const SERVICE_LABEL: Record<string, string> = {
  gateway: "API Gateway",
  accounts: "Cuentas y Saldos",
  risk: "Riesgo y Fraude",
  clearing: "Pasarela Interbancaria",
  orchestrator: "Orquestador",
};

const STATUS_LABEL: Record<StepStatus, string> = {
  RUNNING: "En ejecución",
  SUCCESS: "Completado",
  FAILED: "Fallido",
  COMPENSATING: "Compensando",
  COMPENSATED: "Compensado",
  SKIPPED: "Omitido",
  REPLAY: "Duplicado",
};

function StatusIcon({ status }: { status: StepStatus }) {
  if (status === "SUCCESS") return <CheckCircle className="step-icon" />;
  if (status === "FAILED") return <XCircle className="step-icon" />;
  if (status === "COMPENSATED" || status === "COMPENSATING") return <Undo className="step-icon" />;
  if (status === "SKIPPED") return <MinusCircle className="step-icon" />;
  if (status === "REPLAY") return <Repeat className="step-icon" />;
  return <Clock className="step-icon" />;
}

function money(value: string | number, currency = "USD") {
  return Number(value).toLocaleString("es-CO", {
    style: "currency",
    currency,
    minimumFractionDigits: 2,
  });
}

/** Traduce el estado crudo de la saga a algo legible para el evaluador. */
function describeStatus(status: string) {
  const compensated = status.endsWith("_COMPENSADO");
  const base = compensated ? status.replace("_COMPENSADO", "") : status;
  const map: Record<string, string> = {
    RECIBIDA: "Recibida",
    RECEIVED: "Recibida",
    EN_EJECUCION: "En ejecución",
    CONFIRMADO: "Confirmada",
    RECHAZADO_FONDOS: "Rechazada · fondos insuficientes",
    RECHAZADO_RIESGO: "Rechazada · riesgo",
    RECHAZADO_RED: "Rechazada · red externa",
    RECHAZADO_CREDITO: "Rechazada · crédito",
  };
  const label = map[base] ?? base;
  return compensated ? `${label} · compensada` : label;
}

function statusTone(status: string): "live" | "ok" | "bad" {
  if (status.startsWith("CONFIRMADO")) return "ok";
  if (status.startsWith("RECHAZADO")) return "bad";
  return "live";
}

export default function App() {
  const [tab, setTab] = useState<"simulador" | "historial">("simulador");

  const [from, setFrom] = useState("ACC-1001");
  const [to, setTo] = useState("ACC-1002");
  const [amount, setAmount] = useState("1200");
  const [mode, setMode] = useState<Mode>("ORCHESTRATION");
  const [forceFraud, setForceFraud] = useState(false);
  const [forceTimeout, setForceTimeout] = useState(false);

  const [saga, setSaga] = useState<Saga | null>(null);
  const [steps, setSteps] = useState<Step[]>([]);
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [history, setHistory] = useState<Saga[]>([]);
  const [lastSagaId, setLastSagaId] = useState<string | null>(null);
  const [activeScenario, setActiveScenario] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ tone: "info" | "warn"; text: string } | null>(null);
  const [running, setRunning] = useState(false);

  const sourceRef = useRef<EventSource | null>(null);
  const resultRef = useRef<HTMLDivElement | null>(null);

  const loadAccounts = async () => {
    try {
      const response = await fetch(`${GATEWAY}/accounts`);
      setAccounts(await response.json());
    } catch {
      setNotice({ tone: "warn", text: "No se pudo contactar con el gateway en " + GATEWAY });
    }
  };

  const loadHistory = async () => {
    const response = await fetch(`${GATEWAY}/sagas`);
    setHistory(await response.json());
  };

  useEffect(() => {
    loadAccounts();
    loadHistory();
    // Enlace profundo: ?saga=<uuid> abre directamente esa traza.
    const deepLink = new URLSearchParams(window.location.search).get("saga");
    if (deepLink) openSaga(deepLink);
    return () => sourceRef.current?.close();
  }, []);

  /** Deja el identificador en la URL para poder compartir la traza. */
  const rememberInUrl = (sagaId: string) => {
    const url = new URL(window.location.href);
    url.searchParams.set("saga", sagaId);
    window.history.replaceState({}, "", url);
  };

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
      loadHistory();
    });
    source.onerror = () => {
      source.close();
      setRunning(false);
    };
  };

  const submit = async (reuseSagaId?: string) => {
    setNotice(null);
    if (!reuseSagaId) setSteps([]);
    setRunning(true);
    setTab("simulador");

    try {
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
          saga_id: reuseSagaId ?? null,
        }),
      });
      const data = await response.json();

      if (data.duplicate) {
        setNotice({
          tone: "warn",
          text: `CP-05 · Reintento reconocido. La operación ${data.saga_id.slice(0, 8)} ya estaba en estado «${describeStatus(data.status)}» y no se volvió a ejecutar: los saldos no cambian.`,
        });
      }
      setLastSagaId(data.saga_id);
      rememberInUrl(data.saga_id);
      subscribe(data.saga_id);
      loadAccounts();
      resultRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
    } catch {
      setRunning(false);
      setNotice({ tone: "warn", text: "El gateway no respondió. ¿Está levantado docker compose?" });
    }
  };

  const openSaga = async (sagaId: string) => {
    const response = await fetch(`${GATEWAY}/sagas/${sagaId}`);
    const data = await response.json();
    setSaga(data.saga);
    setSteps(data.log);
    setLastSagaId(sagaId);
    rememberInUrl(sagaId);
    setTab("simulador");
    resultRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  const applyScenario = (scenario: Scenario) => {
    setActiveScenario(scenario.id);
    setAmount(scenario.amount);
    setForceFraud(scenario.fraud);
    setForceTimeout(scenario.timeout);
    setNotice({
      tone: "info",
      text: scenario.repeat
        ? `${scenario.id} · Ejecuta la transferencia y después pulsa «Reintentar con el mismo identificador».`
        : `${scenario.id} · ${scenario.hint}`,
    });
  };

  /* Un paso genera varias filas en la bitácora (RUNNING y luego su estado
     final). En pantalla debe ser UNA fila que evoluciona, no dos apiladas:
     nos quedamos con el último estado de cada paso, separando la ejecución
     de su compensación, y ordenamos por su posición en la bitácora. */
  const timeline = useMemo(() => {
    const latest = new Map<string, Step>();
    for (const step of steps) {
      latest.set(`${step.step}|${step.is_compensation}`, step);
    }
    return [...latest.values()].sort((a, b) => a.seq - b.seq);
  }, [steps]);

  const compensations = useMemo(
    () => timeline.filter((s) => s.is_compensation && s.status === "COMPENSATED").length,
    [timeline],
  );

  const currentStatus = saga?.status ?? null;
  const tone = currentStatus ? statusTone(currentStatus) : "live";
  const sameAccount = from === to;

  return (
    <div className="app">
      {/* ---------- Cabecera fija ---------- */}
      <header className="site-header">
        {/* ---------- Barra utilitaria: solo enlaces que llevan a algo ---------- */}
        <div className="utility">
          <div className="wrap utility-inner">
            <div className="utility-right">
              <a href="http://localhost:4200" target="_blank" rel="noreferrer">
                Panel de Prefect
              </a>
              <a href={`${GATEWAY}/docs`} target="_blank" rel="noreferrer">
                API del gateway
              </a>
            </div>
          </div>
        </div>

        {/* ---------- Marca ---------- */}
        <div className="brand">
          <div className="wrap brand-inner">
            <BrandMark />
            <span className="brand-name">
              NOVABANK<span className="brand-suffix">INTERNATIONAL</span>
            </span>
          </div>
        </div>

        {/* ---------- Navegación principal ---------- */}
        <nav className="mainnav" aria-label="Secciones">
          <div className="wrap mainnav-inner">
            <button className={tab === "simulador" ? "is-active" : ""} onClick={() => setTab("simulador")}>
              Transferencias
            </button>
            <button className={tab === "historial" ? "is-active" : ""} onClick={() => setTab("historial")}>
              Historial de operaciones
            </button>
            <a href="#saldos">Cuentas</a>
            <a href="#escenarios">Escenarios de prueba</a>
          </div>
        </nav>
      </header>

      {/* ---------- Hero ---------- */}
      <section className="hero">
        <div className="wrap hero-inner">
          <div className="hero-copy">
            <p className="eyebrow">Transferencias interbancarias de alto valor</p>
            <h1>
              Consistencia eventual,
              <br />
              sin bloqueos globales
            </h1>
            <p className="hero-lead">
              Patrón Saga con transacciones de compensación en orden inverso. Elige la
              modalidad de coordinación y observa cada micro-paso en tiempo real.
            </p>
            <div className="hero-facts">
              <div>
                <strong>BASE</strong>
                <span>Sin 2PC</span>
              </div>
              <div>
                <strong>2–4 s</strong>
                <span>Por micro-paso</span>
              </div>
              <div>
                <strong>4</strong>
                <span>Bases aisladas</span>
              </div>
            </div>
            <a className="btn btn-green" href="#escenarios">
              Ver escenarios de prueba
            </a>
          </div>

          {/* ---------- Tarjeta de transferencia ---------- */}
          <div className="card hero-card">
            <h2 className="card-title">Nueva transferencia</h2>

            <label className="field">
              <span>Cuenta origen</span>
              <select value={from} onChange={(e) => setFrom(e.target.value)}>
                {accounts.map((account) => (
                  <option key={account.id} value={account.id}>
                    {account.id} · {account.holder}
                  </option>
                ))}
              </select>
            </label>

            <label className="field">
              <span>Cuenta destino</span>
              <select value={to} onChange={(e) => setTo(e.target.value)}>
                {accounts.map((account) => (
                  <option key={account.id} value={account.id}>
                    {account.id} · {account.holder}
                  </option>
                ))}
              </select>
            </label>

            <label className="field">
              <span>Importe (USD)</span>
              <input
                type="number"
                min="1"
                step="0.01"
                value={amount}
                onChange={(e) => setAmount(e.target.value)}
              />
            </label>

            <span className="field-label">Modalidad de la saga</span>
            <div className="segmented">
              <button
                className={mode === "ORCHESTRATION" ? "is-active" : ""}
                onClick={() => setMode("ORCHESTRATION")}
              >
                Orquestación
                <small>Coordinador central</small>
              </button>
              <button
                className={mode === "CHOREOGRAPHY" ? "is-active" : ""}
                onClick={() => setMode("CHOREOGRAPHY")}
              >
                Coreografía
                <small>Por eventos</small>
              </button>
            </div>

            <span className="field-label">Simulador de caos</span>
            <label className="toggle">
              <input
                type="checkbox"
                checked={forceFraud}
                onChange={(e) => setForceFraud(e.target.checked)}
              />
              <span className="track" aria-hidden />
              <span className="toggle-text">
                Forzar alerta de fraude <em>CP-03</em>
              </span>
            </label>
            <label className="toggle">
              <input
                type="checkbox"
                checked={forceTimeout}
                onChange={(e) => setForceTimeout(e.target.checked)}
              />
              <span className="track" aria-hidden />
              <span className="toggle-text">
                Forzar timeout interbancario <em>CP-04</em>
              </span>
            </label>

            {sameAccount && (
              <p className="field-error">
                El origen y el destino deben ser cuentas distintas.
              </p>
            )}

            <button
              className="btn btn-primary"
              disabled={running || sameAccount}
              onClick={() => submit()}
            >
              {running ? "Procesando la saga…" : "Ejecutar transferencia"}
            </button>
            <button
              className="btn btn-ghost"
              disabled={!lastSagaId || running}
              onClick={() => submit(lastSagaId ?? undefined)}
            >
              Reintentar con el mismo identificador
            </button>
            <p className="card-foot">
              Cada operación recibe un UUID de idempotencia al entrar al gateway.
            </p>
          </div>
        </div>
      </section>

      {/* ---------- Escenarios ---------- */}
      <section className="scenarios" id="escenarios">
        <div className="wrap">
          <h2 className="section-title">Elige el escenario que quieres demostrar</h2>
          <div className="scenario-grid">
            {SCENARIOS.map((scenario) => (
              <button
                key={scenario.id}
                className={`scenario ${activeScenario === scenario.id ? "is-active" : ""}`}
                onClick={() => applyScenario(scenario)}
              >
                <scenario.Icon className="scenario-icon" />
                <span className="scenario-id">{scenario.id}</span>
                <span className="scenario-label">{scenario.label}</span>
              </button>
            ))}
          </div>
        </div>
      </section>

      {/* ---------- Contenido ---------- */}
      <main className="wrap content" ref={resultRef}>
        {notice && <div className={`notice notice-${notice.tone}`}>{notice.text}</div>}

        {tab === "historial" ? (
          <section className="card">
            <div className="card-head">
              <h2 className="card-title">Historial de operaciones</h2>
              <button className="link" onClick={loadHistory}>
                Actualizar
              </button>
            </div>
            <table className="table">
              <thead>
                <tr>
                  <th>Identificador</th>
                  <th>Modalidad</th>
                  <th>Origen → destino</th>
                  <th className="right">Importe</th>
                  <th>Estado</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {history.map((item) => (
                  <tr key={item.saga_id}>
                    <td><code>{item.saga_id.slice(0, 8)}</code></td>
                    <td>{item.mode === "ORCHESTRATION" ? "Orquestación" : "Coreografía"}</td>
                    <td className="muted">
                      {item.from_account} → {item.to_account}
                    </td>
                    <td className="right money">{money(item.amount)}</td>
                    <td>
                      <span className={`pill pill-${statusTone(item.status)}`}>
                        {describeStatus(item.status)}
                      </span>
                    </td>
                    <td className="right">
                      <button className="link" onClick={() => openSaga(item.saga_id)}>
                        Ver traza <ChevronRight className="mini" />
                      </button>
                    </td>
                  </tr>
                ))}
                {history.length === 0 && (
                  <tr>
                    <td colSpan={6} className="empty">
                      Todavía no hay operaciones registradas.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </section>
        ) : (
          <div className="columns">
            {/* ---------- Traza ---------- */}
            <section className="card">
              <div className="card-head">
                <h2 className="card-title">Avance de la operación</h2>
                {currentStatus && (
                  <span className={`pill pill-${tone}`}>{describeStatus(currentStatus)}</span>
                )}
              </div>

              {saga && (
                <dl className="meta">
                  <div>
                    <dt>Identificador</dt>
                    <dd><code>{saga.saga_id}</code></dd>
                  </div>
                  <div>
                    <dt>Modalidad</dt>
                    <dd>{saga.mode === "ORCHESTRATION" ? "Orquestación" : "Coreografía"}</dd>
                  </div>
                  <div>
                    <dt>Compensaciones</dt>
                    <dd>{compensations}</dd>
                  </div>
                </dl>
              )}

              {timeline.length === 0 ? (
                <div className="empty-state">
                  <Bank className="empty-icon" />
                  <p>
                    Sin pasos todavía. Configura la transferencia y pulsa
                    <strong> Ejecutar transferencia</strong>.
                  </p>
                </div>
              ) : (
                <ol className="timeline">
                  {timeline.map((step) => (
                    <li
                      key={step.id}
                      className={`tl-${step.status.toLowerCase()} ${step.is_compensation ? "is-comp" : ""}`}
                    >
                      <span className="tl-marker">
                        <StatusIcon status={step.status} />
                      </span>
                      <div className="tl-body">
                        <div className="tl-head">
                          <span className="tl-name">
                            {STEP_LABEL[step.step] ?? step.step}
                            {step.is_compensation && <em className="tl-tag">compensación</em>}
                          </span>
                          <span className="tl-status">{STATUS_LABEL[step.status]}</span>
                        </div>
                        <p className="tl-service">{SERVICE_LABEL[step.service] ?? step.service}</p>
                        {step.detail && <p className="tl-detail">{step.detail}</p>}
                      </div>
                    </li>
                  ))}
                </ol>
              )}

              {compensations > 0 && (
                <p className="reverse-note">
                  <Undo className="mini" />
                  Las compensaciones se ejecutaron en orden inverso al de la transacción.
                </p>
              )}
            </section>

            {/* ---------- Saldos ---------- */}
            <aside className="card" id="saldos">
              <div className="card-head">
                <h2 className="card-title">Saldos</h2>
                <button className="link" onClick={loadAccounts}>
                  Actualizar
                </button>
              </div>
              <table className="table">
                <tbody>
                  {accounts.map((account) => (
                    <tr key={account.id}>
                      <td>
                        <span className="acc-id">{account.id}</span>
                        <span className="acc-holder">{account.holder}</span>
                      </td>
                      <td className="right money">{money(account.balance, account.currency)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <p className="hint">
                Tras una compensación completa los saldos deben volver exactamente a su
                valor anterior.
              </p>
            </aside>
          </div>
        )}
      </main>

      <footer className="footer">
        <div className="wrap footer-inner">
          <span>NovaBank International · Taller de Arquitectura de Software Distribuida</span>
          <span className="muted">
            Entidad ficticia con fines académicos. No es una institución financiera real.
          </span>
        </div>
      </footer>
    </div>
  );
}
