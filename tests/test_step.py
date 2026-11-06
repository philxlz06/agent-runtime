import asyncio
import uuid

import pytest

from agent_runtime.context import RunContext, current_run
from agent_runtime.step import ParallelStepError, step, idempotency_key
from fake_payments import refund

calls = []


@step
async def add(a, b):
    calls.append("add")
    return a + b


@step
async def slow(x):
    await asyncio.sleep(0)
    return x


async def new_run(conn):
    run_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO runs (id, workflow_name) VALUES (%s, %s)", (run_id, "test")
    )
    return run_id


@pytest.mark.asyncio
async def test_step_replays_saved_output(conn):
    calls.clear()
    run_id = await new_run(conn)

    current_run.set(RunContext(run_id=run_id, conn=conn))
    assert await add(2, 3) == 5   # first time: really runs, saves seq 1

    current_run.set(RunContext(run_id=run_id, conn=conn))   # "restart": counter back to 0
    assert await add(2, 3) == 5   # second time: replayed from the journal

    assert calls == ["add"]       # the real function ran only once


@pytest.mark.asyncio
async def test_parallel_steps_are_rejected(conn):
    run_id = await new_run(conn)
    current_run.set(RunContext(run_id=run_id, conn=conn))
    with pytest.raises(ParallelStepError):
        await asyncio.gather(slow(1), slow(2))

#####
@step
async def pay(amount):
    return await refund(idempotency_key(), amount)


@pytest.mark.asyncio
async def test_step_passes_its_key_to_outside_api(conn):
    run_id = await new_run(conn)
    current_run.set(RunContext(run_id=run_id, conn=conn))

    result = await pay(40)
    assert result["refund_id"] == f"re_{run_id}:1"     # the service got key "run_id:1"

    cur = await conn.execute("SELECT status FROM steps WHERE run_id = %s", (run_id,))
    assert await cur.fetchall() == [("completed",)]   # one row, started → completed

#####