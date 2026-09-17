-- Bitácora de sagas (capa coordinadora)
--
-- Se aplica automáticamente al arrancar el contenedor db-saga, que es
-- el ÚNICO Postgres que este servicio conoce. Ningún otro servicio tiene
-- credenciales ni ruta de red hacia aquí.

-- Estado de la saga. En coreografia NO es un coordinador:
-- lo escribe un consumidor de auditoria que solo observa el bus.
CREATE TABLE sagas (
    saga_id      UUID PRIMARY KEY,
    mode         TEXT NOT NULL CHECK (mode IN ('ORCHESTRATION','CHOREOGRAPHY')),
    from_account TEXT NOT NULL,
    to_account   TEXT NOT NULL,
    amount       NUMERIC(18,2) NOT NULL,
    status       TEXT NOT NULL,
    chaos        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Bitacora paso a paso. Es la fuente del dashboard en tiempo real.
CREATE TABLE saga_log (
    id          BIGSERIAL PRIMARY KEY,
    saga_id     UUID NOT NULL REFERENCES sagas(saga_id),
    seq         INT  NOT NULL,
    step        TEXT NOT NULL,
    service     TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN
                  ('RUNNING','SUCCESS','FAILED','COMPENSATING','COMPENSATED','SKIPPED','REPLAY')),
    is_compensation BOOLEAN NOT NULL DEFAULT FALSE,
    detail      TEXT,
    payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX saga_log_saga_idx ON saga_log (saga_id, id);
