import pytest

from agent_runtime.step import step
from agent_runtime.workflow import run_workflow, start_run, workflow


@step
async def double(x):
    return x * 2


@workflow
async def double_twice(x):
    a = await double(x)
    b = await double(a)
    return b


@pytest.mark.asyncio
async def test_run_workflow_completes(conn):
    run_id = await start_run(conn, "double_twice", {"x": 3})
    assert await run_workflow(conn, run_id) == 12

    cur = await conn.execute("SELECT status, output FROM runs WHERE id = %s", (run_id,))
    assert await cur.fetchone() == ("completed", 12)

    cur = await conn.execute("SELECT count(*) FROM steps WHERE run_id = %s", (run_id,))
    assert (await cur.fetchone())[0] == 2