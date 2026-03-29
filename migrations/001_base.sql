CREATE TABLE IF NOT EXISTS programs (
    id SERIAL PRIMARY KEY,
    program_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    idl JSONB NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS checkpoints (
    program_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (program_id, key)
);
