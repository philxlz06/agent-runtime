import asyncio

import httpx
import pytest
import pytest_asyncio
from psycopg_pool import AsyncConnectionPool

from agent_runtime.api import app
from agent_runtime.approval import require_approval
from agent_runtime.db import DSN
from agent_runtime.step import step
from agent_runtime.worker import worker_loop
from agent_runtime.workflow import start_run, workflow
from agent_runtime import api

@pytest_asyncio.fixture
async def client(conn):                                  # conn first → tables migrated + wiped
    async with app.router.lifespan_context(app):         # start the app (opens the pool)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


@pytest.mark.asyncio
async def test_start_run_then_read_it(client):
    r = await client.post("/runs", json={"workflow": "anything", "input": {"x": 1}})
    assert r.status_code == 201
    run_id = r.json()["id"]

    r = await client.get(f"/runs/{run_id}")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "pending"
    assert body["input"] == {"x": 1}
    assert body["steps"] == []

    r = await client.get("/runs")
    assert [run["id"] for run in r.json()] == [run_id]


@pytest.mark.asyncio
async def test_same_idempotency_key_returns_same_run(client, conn):
    headers = {"Idempotency-Key": "refund-order-42"}
    first = await client.post("/runs", json={"workflow": "anything"}, headers=headers)
    second = await client.post("/runs", json={"workflow": "anything"}, headers=headers)
    assert first.json()["id"] == second.json()["id"]           # retry → same run

    cur = await conn.execute("SELECT count(*) FROM runs")
    assert (await cur.fetchone())[0] == 1                      # only ONE run was created


@pytest.mark.asyncio
async def test_unknown_run_is_404(client):
    r = await client.get("/runs/00000000-0000-0000-0000-000000000000")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_approve_through_the_api(client, conn):
    run_id = await start_run(conn, "anything")
    await conn.execute("UPDATE runs SET status = 'waiting_approval' WHERE id = %s", (run_id,))
    await conn.execute(
        "INSERT INTO approvals (run_id, seq, prompt) VALUES (%s, 1, 'Refund $500?')", (run_id,)
    )

    r = await client.post(f"/runs/{run_id}/approvals/1", json={"approved": True})
    assert r.status_code == 200

    r = await client.post(f"/runs/{run_id}/approvals/1", json={"approved": False})
    assert r.status_code == 409                                # already decided

    r = await client.get(f"/runs/{run_id}")
    assert r.json()["status"] == "pending"                     # back in line for workers
    assert r.json()["approvals"][0]["status"] == "approved"


@step
async def api_double(x):
    return x * 2


@workflow
async def api_flow(x):
    ok = await require_approval(f"double {x}?")
    if not ok:
        return None
    return await api_double(x)


async def wait_for_status(client, run_id, status, timeout=10):
    """Poll the API until the run reaches `status`, or fail after `timeout` seconds."""
    for _ in range(int(timeout / 0.1)):
        body = (await client.get(f"/runs/{run_id}")).json()
        if body["status"] == status:
            return body
        await asyncio.sleep(0.1)
    raise AssertionError(f"run never reached {status!r}; last status was {body['status']!r}")


@pytest.mark.asyncio
async def test_end_to_end_with_workers(client):
    stop = asyncio.Event()
    async with AsyncConnectionPool(
        DSN, kwargs={"autocommit": True}, max_size=6, open=False
    ) as pool:
        workers = [
            asyncio.create_task(worker_loop(pool, f"w{i}", poll_interval=0.1, stop=stop))
            for i in range(2)
        ]
        try:
            r = await client.post("/runs", json={"workflow": "api_flow", "input": {"x": 21}})
            run_id = r.json()["id"]

            body = await wait_for_status(client, run_id, "waiting_approval")
            assert body["approvals"][0]["prompt"] == "double 21?"

            r = await client.post(f"/runs/{run_id}/approvals/1", json={"approved": True})
            assert r.status_code == 200

            body = await wait_for_status(client, run_id, "completed")
            assert body["output"] == 42
            assert [s["name"] for s in body["steps"]] == ["require_approval", "api_double"]
        finally:
            stop.set()
            await asyncio.gather(*workers)


@pytest.mark.asyncio
async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


@pytest_asyncio.fixture
async def client(conn, monkeypatch):
    monkeypatch.setenv("ADMIN_API_KEY", "test-key")              # a fake key, just for tests
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", headers={"X-Admin-Key": "test-key"}
        ) as c:
            yield c


@pytest.mark.asyncio
async def test_writes_need_the_admin_key(client):
    r = await client.post("/runs", json={"workflow": "anything"}, headers={"X-Admin-Key": "wrong"})
    assert r.status_code == 401


@pytest.fixture(autouse=True)
def reset_rate_limits():
    api.demo_start_limit.hits.clear() 
    api.demo_kill_limit.hits.clear()         # each test starts with a clean limiter
    api.agent_limit.hits.clear()
    api.agent_daily_limit.hits.clear()


@pytest.mark.asyncio
async def test_demo_start_needs_no_key_and_caps_count(client):
    r = await client.post("/demo/runs?count=2", headers={"X-Admin-Key": ""})
    assert r.status_code == 201
    assert len(r.json()["ids"]) == 2
    r = await client.post("/demo/runs?count=50")
    assert r.status_code == 422                 # over the cap of 3


@pytest.mark.asyncio
async def test_demo_start_is_rate_limited(client):
    for _ in range(5):
        assert (await client.post("/demo/runs")).status_code == 201
    assert (await client.post("/demo/runs")).status_code == 429   # the 6th within a minute


@pytest.mark.asyncio
async def test_demo_stats(client):
    await client.post("/demo/runs?count=3")
    r = await client.get("/demo/stats")
    assert r.json()["pending"] == 3
    assert r.json()["refunds"] == 0


@pytest.mark.asyncio
async def test_demo_kill_queues_a_request_with_cooldown(client, conn):
    assert (await client.post("/demo/kill")).status_code == 202
    assert (await client.post("/demo/kill")).status_code == 429      # cooldown
    cur = await conn.execute("SELECT count(*) FROM kill_requests")
    assert (await cur.fetchone())[0] == 1


@pytest.mark.asyncio
async def test_home_serves_the_page(client):
    r = await client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]


@pytest.mark.asyncio
async def test_runs_filter_by_workflow(client, conn):
    await start_run(conn, "refund_demo", {"order_id": "A"})
    await start_run(conn, "other", {})
    r = await client.get("/runs?workflow=refund_demo")
    assert [run["workflow_name"] for run in r.json()] == ["refund_demo"]
    assert r.json()[0]["steps_done"] == 0


@pytest.mark.asyncio
async def test_runs_list_marks_resumed_runs(client, conn):
    run_id = await start_run(conn, "anything")
    await conn.execute(
        "INSERT INTO steps (run_id, seq, name, idempotency_key, status, lease_version) VALUES "
        "(%s, 1, 'a', 'k1', 'completed', 1), (%s, 2, 'b', 'k2', 'completed', 2)",
        (run_id, run_id),
    )
    r = await client.get("/runs")
    assert r.json()[0]["resumed"] is True                 # steps finished by 2 attempts = resumed


@pytest.mark.asyncio
async def test_demo_support_starts_a_demo_agent_run(client, conn):
    r = await client.post("/demo/support", json={"message": "I was charged twice"})
    assert r.status_code == 201
    cur = await conn.execute("SELECT input->>'source' FROM runs WHERE id = %s", (r.json()["id"],))
    assert (await cur.fetchone())[0] == "demo"
    assert (await client.post("/demo/support", json={"message": "x" * 301})).status_code == 422


@pytest.mark.asyncio
async def test_visitors_can_only_decide_demo_runs(client, conn):
    real = await start_run(conn, "support_agent", {"message": "hi", "source": "api"})
    r = await client.post(f"/runs/{real}/approvals/2", json={"approved": True}, headers={"X-Admin-Key": ""})
    assert r.status_code == 401
    r = await client.post(f"/demo/support/{real}/decision", json={"approved": True})
    assert r.status_code == 404

    demo = await start_run(conn, "support_agent", {"message": "refund pls", "source": "demo"})
    await conn.execute("UPDATE runs SET status = 'waiting_approval' WHERE id = %s", (demo,))
    await conn.execute("INSERT INTO approvals (run_id, seq, prompt) VALUES (%s, 2, 'Refund $25?')", (demo,))
    r = await client.post(f"/demo/support/{demo}/decision", json={"approved": True})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_demo_stats_only_counts_part1_refunds(client, conn):
    demo = await start_run(conn, "refund_demo", {"order_id": "A"})
    agent = await start_run(conn, "support_agent", {"message": "m"})
    await conn.execute(
        "INSERT INTO demo_refunds (idempotency_key, order_id, amount) VALUES (%s, 'A', 25), (%s, 'B', 25)",
        (f"{demo}:2", f"{agent}:4"),
    )
    r = await client.get("/demo/stats")
    assert r.json()["refunds"] == 1          # the agent's refund doesn't count toward Part 1
