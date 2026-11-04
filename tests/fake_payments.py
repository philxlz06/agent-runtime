"""A pretend payment API (like Stripe) that remembers idempotency keys."""
import psycopg
from psycopg.types.json import Jsonb

from agent_runtime.db import DSN

SCHEMA = """
CREATE TABLE IF NOT EXISTS effects (
    idempotency_key TEXT PRIMARY KEY,
    payload         JSONB NOT NULL,
    result          JSONB NOT NULL,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


async def refund(key, amount):
    result = {"refund_id": f"re_{key}", "amount": amount}
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        cur = await conn.execute(
            "INSERT INTO effects (idempotency_key, payload, result) VALUES (%s, %s, %s) "
            "ON CONFLICT (idempotency_key) DO NOTHING RETURNING result",
            (key, Jsonb({"amount": amount}), Jsonb(result)),
        )
        row = await cur.fetchone()
        if row is None:   # seen this key before → don't refund again
            cur = await conn.execute(
                "SELECT result FROM effects WHERE idempotency_key = %s", (key,)
            )
            row = await cur.fetchone()
        return row[0]