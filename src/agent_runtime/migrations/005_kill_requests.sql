CREATE TABLE IF NOT EXISTS kill_requests (
  id            BIGSERIAL PRIMARY KEY,
  requested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_by    TEXT,
  claimed_at    TIMESTAMPTZ
);
