-- Una base de datos y un rol por servicio: frontera fisica de datos.
-- Equivale a los 4 proyectos Supabase que usaremos mas adelante.

CREATE ROLE accounts_user  WITH LOGIN PASSWORD 'accounts_pwd';
CREATE ROLE risk_user      WITH LOGIN PASSWORD 'risk_pwd';
CREATE ROLE clearing_user  WITH LOGIN PASSWORD 'clearing_pwd';
CREATE ROLE saga_user      WITH LOGIN PASSWORD 'saga_pwd';

CREATE DATABASE accounts_db OWNER accounts_user;
CREATE DATABASE risk_db     OWNER risk_user;
CREATE DATABASE clearing_db OWNER clearing_user;
CREATE DATABASE saga_db     OWNER saga_user;

-- Nadie puede conectarse a la base de datos de otro dominio.
REVOKE CONNECT ON DATABASE accounts_db FROM PUBLIC;
REVOKE CONNECT ON DATABASE risk_db     FROM PUBLIC;
REVOKE CONNECT ON DATABASE clearing_db FROM PUBLIC;
REVOKE CONNECT ON DATABASE saga_db     FROM PUBLIC;

GRANT CONNECT ON DATABASE accounts_db TO accounts_user;
GRANT CONNECT ON DATABASE risk_db     TO risk_user;
GRANT CONNECT ON DATABASE clearing_db TO clearing_user;
GRANT CONNECT ON DATABASE saga_db     TO saga_user;
