CREATE TABLE IF NOT EXISTS runs (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_name    TEXT NOT NULL,
  input            JSONB NOT NULL DEFAULT '{}',
  status           TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','running','completed','failed')),
  lease_owner      TEXT,
  lease_expires_at TIMESTAMPTZ,
  lease_version    INTEGER NOT NULL DEFAULT 0,
  output           JSONB,
  error            TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS runs_claim_idx ON runs (status, lease_expires_at);

CREATE TABLE IF NOT EXISTS steps (
  id              BIGSERIAL PRIMARY KEY,
  run_id          UUID NOT NULL REFERENCES runs(id),
  seq             INTEGER NOT NULL,
  kind            TEXT NOT NULL DEFAULT 'tool',
  name            TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  status          TEXT NOT NULL CHECK (status IN ('started','completed')),
  input           JSONB,
  output          JSONB,
  started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at    TIMESTAMPTZ,
  UNIQUE (run_id, seq)
);