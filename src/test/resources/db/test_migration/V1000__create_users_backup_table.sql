CREATE TABLE users_backup
(
    id          BIGSERIAL PRIMARY KEY,
    external_id TEXT      NOT NULL UNIQUE,
    email       TEXT      NOT NULL UNIQUE,
    created_at  TIMESTAMP NOT NULL DEFAULT now()
)