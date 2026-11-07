from pathlib import Path

import psycopg
import pytest_asyncio

from agent_runtime.db import DSN
from fake_payments import SCHEMA as EFFECTS_SCHEMA

RUNTIME_SCHEMA = (Path(__file__).parent.parent / "schema.sql").read_text()


@pytest_asyncio.fixture
async def conn():
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        await conn.execute(RUNTIME_SCHEMA)       # runs + steps tables (no-op if they exist)
        await conn.execute(EFFECTS_SCHEMA)       # fake Stripe's table
        await conn.execute("TRUNCATE steps, runs, effects")
        yield conn