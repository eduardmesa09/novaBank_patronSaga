-- Servicio de Riesgo y Prevención de Fraude
--
-- Se aplica automáticamente al arrancar el contenedor db-risk, que es
-- el ÚNICO Postgres que este servicio conoce. Ningún otro servicio tiene
-- credenciales ni ruta de red hacia aquí.

CREATE TABLE daily_limits (
    account_id     TEXT PRIMARY KEY,
    max_per_tx     NUMERIC(18,2) NOT NULL,
    max_per_day    NUMERIC(18,2) NOT NULL,
    used_today     NUMERIC(18,2) NOT NULL DEFAULT 0,
    limit_date     DATE NOT NULL DEFAULT CURRENT_DATE
);

CREATE TABLE risk_decisions (
    id          BIGSERIAL PRIMARY KEY,
    saga_id     UUID NOT NULL,
    account_id  TEXT NOT NULL,
    amount      NUMERIC(18,2) NOT NULL,
    decision    TEXT NOT NULL CHECK (decision IN ('APPROVED','REJECTED','REVOKED')),
    reason      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX risk_decisions_saga_idx ON risk_decisions (saga_id);

CREATE TABLE processed_operations (
    saga_id    UUID NOT NULL,
    operation  TEXT NOT NULL,
    response   JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (saga_id, operation)
);

INSERT INTO daily_limits (account_id, max_per_tx, max_per_day) VALUES
    ('ACC-1001', 4000.00,  8000.00),
    ('ACC-1002', 2500.00,  5000.00),
    ('ACC-1003', 50000.00, 120000.00),
    ('ACC-2001', 50000.00, 120000.00);
