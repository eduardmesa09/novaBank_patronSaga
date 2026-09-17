-- Pasarela Interbancaria
--
-- Se aplica automáticamente al arrancar el contenedor db-clearing, que es
-- el ÚNICO Postgres que este servicio conoce. Ningún otro servicio tiene
-- credenciales ni ruta de red hacia aquí.

CREATE TABLE settlements (
    saga_id       UUID PRIMARY KEY,
    from_account  TEXT NOT NULL,
    to_account    TEXT NOT NULL,
    amount        NUMERIC(18,2) NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('SETTLED','FAILED','CANCELLED')),
    external_ref  TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE clearing_attempts (
    id          BIGSERIAL PRIMARY KEY,
    saga_id     UUID NOT NULL,
    outcome     TEXT NOT NULL,
    detail      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX clearing_attempts_saga_idx ON clearing_attempts (saga_id);

CREATE TABLE processed_operations (
    saga_id    UUID NOT NULL,
    operation  TEXT NOT NULL,
    response   JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (saga_id, operation)
);
