import pytest

from agent_runtime.approval import decide, require_approval
from agent_runtime.step import step
from agent_runtime.worker import claim_run
from agent_runtime.workflow import run_workflow, start_run, workflow

calls = []


@step
async def do_refund(amount):
    calls.append(amount)
    return amount


@workflow
async def approval_flow(amount):
    ok = await require_approval(f"Refund ${amount}?")
    if ok:
        await do_refund(amount)
    return ok


async def status_of(conn, run_id):
    cur = await conn.execute("SELECT status FROM runs WHERE id = %s", (run_id,))
    return (await cur.fetchone())[0]


@pytest.mark.asyncio
async def test_run_parks_then_resumes_after_approval(conn):
    calls.clear()
    run_id = await start_run(conn, "approval_flow", {"amount": 500})

    _, v1 = await claim_run(conn, "worker-0")
    assert await run_workflow(conn, run_id, lease_version=v1) is None   # parked
    assert await status_of(conn, run_id) == "waiting_approval"
    assert calls == []                                                  # risky step NOT done
    assert await claim_run(conn, "worker-1") is None                    # nobody claims a parked run

    assert await decide(conn, run_id, seq=1, approved=True) is True
    assert await decide(conn, run_id, seq=1, approved=False) is False   # can't decide twice
    assert await status_of(conn, run_id) == "pending"                   # back in line

    _, v2 = await claim_run(conn, "worker-1")
    assert await run_workflow(conn, run_id, lease_version=v2) is True
    assert calls == [500]                                               # refund ran exactly once
    assert await status_of(conn, run_id) == "completed"


@pytest.mark.asyncio
async def test_rejected_approval_skips_the_risky_step(conn):
    calls.clear()
    run_id = await start_run(conn, "approval_flow", {"amount": 500})

    _, v1 = await claim_run(conn, "worker-0")
    await run_workflow(conn, run_id, lease_version=v1)                  # parks
    await decide(conn, run_id, seq=1, approved=False)

    _, v2 = await claim_run(conn, "worker-1")
    assert await run_workflow(conn, run_id, lease_version=v2) is False
    assert calls == []                                                  # refund never happened
    assert await status_of(conn, run_id) == "completed"
