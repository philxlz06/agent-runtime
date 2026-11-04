import asyncio
import os
import signal
import sys
import uuid

import psycopg

from agent_runtime.db import DSN
from agent_runtime.step import step
from agent_runtime.workflow import run_workflow, workflow


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


async def main(run_id):
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        await run_workflow(conn, run_id)


if __name__ == "__main__":
    asyncio.run(main(uuid.UUID(sys.argv[1])))