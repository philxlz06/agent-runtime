import pytest

from agent_runtime.demo import refund_demo  # noqa: F401  (registers the workflow)
from agent_runtime.workflow import run_workflow, start_run


@pytest.mark.asyncio
async def test_refund_demo_refunds_once(conn, monkeypatch):
    monkeypatch.setenv("DEMO_STEP_DELAY", "0")
    run_id = await start_run(conn, "refund_demo", {"order_id": "A-1"})
    output = await run_workflow(conn, run_id)

    cur = await conn.execute("SELECT count(*) FROM demo_refunds")
    assert (await cur.fetchone())[0] == 1                  # exactly one refund
    assert "refund_id" in output
