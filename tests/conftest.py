import psycopg
import pytest_asyncio

from agent_runtime.db import DSN


@pytest_asyncio.fixture
async def conn():
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        await conn.execute("TRUNCATE steps, runs")
        yield conn