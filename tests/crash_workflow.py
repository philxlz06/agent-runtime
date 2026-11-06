import asyncio
import os
import signal
import sys
import uuid

import psycopg

from agent_runtime.db import DSN
from agent_runtime.workflow import run_workflow, workflow
from agent_runtime.step import idempotency_key, step
from fake_payments import refund

@step
async def record(i):
    if os.environ.get("CRASH_AT") == str(i):
        os.kill(os.getpid(), signal.SIGKILL)   # die instantly
    with open(os.environ["CALLS_LOG"], "a") as f:
        f.write(f"{i}\n")                      # proof this step really ran
    return i


@workflow
async def five_steps():
    for i in range(1, 6):
        await record(i)
    return "done"

##########
@step
async def pay(amount):
    result = await refund(idempotency_key(), amount)     # 💸 money leaves
    if os.environ.get("CRASH_AFTER_REFUND") == "1":
        os.kill(os.getpid(), signal.SIGKILL)             # die before @step can save
    return result


@workflow
async def refund_flow(amount):
    return await pay(amount)
#########

async def main(run_id):
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        await run_workflow(conn, run_id)


if __name__ == "__main__":
    asyncio.run(main(uuid.UUID(sys.argv[1])))