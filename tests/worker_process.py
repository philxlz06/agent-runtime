import asyncio
import logging
import os
import random

from psycopg_pool import AsyncConnectionPool

from agent_runtime.db import DSN
from agent_runtime.step import idempotency_key, step
from agent_runtime.worker import worker_loop
from agent_runtime.workflow import workflow
from fake_payments import refund


@step
async def slow_add(a, b):
    await asyncio.sleep(random.uniform(0.05, 0.2))   # takes a moment, so kills can land mid-run
    return a + b


@step
async def charge(amount):
    return await refund(idempotency_key(), amount)


@workflow
async def chaos_flow(x):
    a = await slow_add(x, x)        # step 1: 2x
    b = await slow_add(a, 1)        # step 2: 2x + 1
    await charge(b)                 # step 3: a side effect (must happen exactly once)
    return b


async def main():
    lease = float(os.environ.get("LEASE_SECONDS", "2"))
    async with AsyncConnectionPool(
        DSN, kwargs={"autocommit": True}, min_size=1, max_size=4, open=False
    ) as pool:
        await worker_loop(pool, f"proc-{os.getpid()}", lease_seconds=lease, poll_interval=0.1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
