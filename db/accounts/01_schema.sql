-- Servicio de Cuentas y Saldos
--
-- Se aplica automáticamente al arrancar el contenedor db-accounts, que es
-- el ÚNICO Postgres que este servicio conoce. Ningún otro servicio tiene
-- credenciales ni ruta de red hacia aquí.

CREATE TABLE accounts (
    id         TEXT PRIMARY KEY,
    holder     TEXT NOT NULL,
    balance    NUMERIC(18,2) NOT NULL CHECK (balance >= 0),
    currency   TEXT NOT NULL DEFAULT 'USD',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Libro contable: append-only. Una compensacion NO borra el debito,
-- escribe un asiento REFUND que lo neutraliza (trazabilidad de auditoria).
CREATE TABLE ledger_entries (
    id         BIGSERIAL PRIMARY KEY,
    saga_id    UUID NOT NULL,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    kind       TEXT NOT NULL CHECK (kind IN ('DEBIT','CREDIT','REFUND')),
    amount     NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ledger_entries_saga_idx ON ledger_entries (saga_id);

-- CP-05: idempotencia local. La PK (saga_id, operation) hace imposible
-- ejecutar dos veces la misma operacion de la misma saga.
CREATE TABLE processed_operations (
    saga_id    UUID NOT NULL,
    operation  TEXT NOT NULL,
    response   JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (saga_id, operation)
);

INSERT INTO accounts (id, holder, balance) VALUES
    ('ACC-1001', 'Eduard Meza',        5000.00),
    ('ACC-1002', 'Valentina Rojas',    3200.00),
    ('ACC-1003', 'NovaBank Corporate', 150000.00),
    ('ACC-2001', 'Banco Externo S.A.', 80000.00);
