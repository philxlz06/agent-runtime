import psycopg
import pytest_asyncio

from agent_runtime.db import DSN
from agent_runtime.migrate import migrate
from fake_payments import SCHEMA as EFFECTS_SCHEMA


@pytest_asyncio.fixture
async def conn():
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        await migrate(conn)                                          # bring the DB up to date
        await conn.execute(EFFECTS_SCHEMA)
        await conn.execute("TRUNCATE steps, approvals, runs, effects")
        yield conn
