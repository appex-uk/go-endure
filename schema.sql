CREATE TABLE IF NOT EXISTS endure_workflow (
    id               text PRIMARY KEY,
    name             text        NOT NULL,
    input            jsonb       NOT NULL,
    status           text        NOT NULL CHECK (status IN ('queued', 'running', 'waiting', 'completed', 'failed')),
    output           jsonb,
    error            text,
    run_at           timestamptz NOT NULL DEFAULT now(),
    waiting_on       text,
    worker_id        text,
    lease_token      uuid,
    lease_expires_at timestamptz,
    attempt          integer     NOT NULL DEFAULT 0,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS endure_workflow_queued_idx
    ON endure_workflow (run_at) WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS endure_workflow_lease_idx
    ON endure_workflow (lease_expires_at) WHERE status = 'running';

CREATE TABLE IF NOT EXISTS endure_step (
    workflow_id  text        NOT NULL REFERENCES endure_workflow (id) ON DELETE CASCADE,
    name         text        NOT NULL,
    status       text        NOT NULL CHECK (status IN ('running', 'retrying', 'completed', 'failed')),
    output       jsonb,
    error        text,
    attempts     integer     NOT NULL DEFAULT 0,
    not_before   timestamptz,
    started_at   timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    PRIMARY KEY (workflow_id, name)
);

CREATE TABLE IF NOT EXISTS endure_signal (
    workflow_id text        NOT NULL REFERENCES endure_workflow (id) ON DELETE CASCADE,
    name        text        NOT NULL,
    payload     jsonb       NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, name)
);
