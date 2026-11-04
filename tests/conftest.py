import psycopg
import pytest_asyncio

from agent_runtime.db import DSN
from fake_payments import SCHEMA as EFFECTS_SCHEMA


@pytest_asyncio.fixture
async def conn():
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        await conn.execute(EFFECTS_SCHEMA)
        await conn.execute("TRUNCATE steps, runs, effects")
        yield conn