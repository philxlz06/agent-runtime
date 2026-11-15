CREATE TABLE IF NOT EXISTS demo_refunds (
  id               BIGSERIAL PRIMARY KEY,
  idempotency_key  TEXT NOT NULL UNIQUE,
  order_id         TEXT NOT NULL,
  amount           INTEGER NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
