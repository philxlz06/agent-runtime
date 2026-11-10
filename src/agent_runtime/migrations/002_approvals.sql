-- allow runs to park while waiting for a human
ALTER TABLE runs DROP CONSTRAINT IF EXISTS runs_status_check;
ALTER TABLE runs ADD CONSTRAINT runs_status_check
  CHECK (status IN ('pending', 'running', 'waiting_approval', 'completed', 'failed'));

-- one row per approval request
CREATE TABLE IF NOT EXISTS approvals (
  run_id      UUID NOT NULL REFERENCES runs(id),
  seq         INTEGER NOT NULL,
  prompt      TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'pending'
              CHECK (status IN ('pending', 'approved', 'rejected')),
  decided_by  TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  decided_at  TIMESTAMPTZ,
  PRIMARY KEY (run_id, seq)
);
