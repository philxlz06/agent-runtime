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
