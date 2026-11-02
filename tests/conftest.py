import psycopg
import pytest_asyncio

DSN = "postgresql://runtime:runtime@localhost:5432/runtime"


@pytest_asyncio.fixture
async def conn():
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        await conn.execute("TRUNCATE steps, runs")
        yield conn