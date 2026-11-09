import asyncio

import psycopg
import pytest

from agent_runtime.context import RunContext, current_run
from agent_runtime.db import DSN
from agent_runtime.step import LeaseLostError, step
from psycopg_pool import AsyncConnectionPool
from agent_runtime.worker import claim_run, heartbeat, renew_lease, worker_loop
from agent_runtime.workflow import start_run, workflow

async def claimer(worker_id):
    """One fake worker: keep claiming runs until there are none left."""
    claimed = []
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as c:
        while True:
            claim = await claim_run(c, worker_id)
            if claim is None:
                break
            claimed.append(claim[0])
    return claimed


@pytest.mark.asyncio
async def test_each_run_is_claimed_exactly_once(conn):
    created = set()
    for _ in range(50):
        created.add(await start_run(conn, "double_twice", {"x": 1}))

    results = await asyncio.gather(*(claimer(f"worker-{i}") for i in range(5)))

    all_claimed = [run_id for worker in results for run_id in worker]
    assert len(all_claimed) == len(set(all_claimed))   # no run claimed twice
    assert set(all_claimed) == created                  # every run got claimed


async def lease_is_valid(conn, run_id):
    cur = await conn.execute("SELECT lease_expires_at > now() FROM runs WHERE id = %s", (run_id,))
    return (await cur.fetchone())[0]


@pytest.mark.asyncio
async def test_claim_sets_a_lease(conn):
    run_id = await start_run(conn, "double_twice", {"x": 1})
    await claim_run(conn, "worker-0", lease_seconds=30)

    cur = await conn.execute(
        "SELECT lease_owner, lease_expires_at - now() FROM runs WHERE id = %s", (run_id,)
    )
    owner, remaining = await cur.fetchone()
    assert owner == "worker-0"
    assert 25 < remaining.total_seconds() <= 30      # about 30 seconds left





@step
async def add(a, b):
    return a + b


@pytest.mark.asyncio
async def test_zombie_step_write_is_rejected(conn):
    run_id = await start_run(conn, "double_twice", {"x": 1})

    _, v0 = await claim_run(conn, "worker-0", lease_seconds=1)             # worker-0 holds token v0
    await asyncio.sleep(1.5)                                               # its lease expires
    _, v1 = await claim_run(conn, "worker-1")                              # worker-1 takes over
    assert v1 == v0 + 1

    current_run.set(RunContext(run_id=run_id, conn=conn, lease_version=v0))   # zombie worker-0
    with pytest.raises(LeaseLostError):
        await add(2, 3)
    cur = await conn.execute("SELECT count(*) FROM steps WHERE run_id = %s", (run_id,))
    assert (await cur.fetchone())[0] == 0                                  # nothing was written

    current_run.set(RunContext(run_id=run_id, conn=conn, lease_version=v1))   # the real owner
    assert await add(2, 3) == 5


@pytest.mark.asyncio
async def test_expired_run_gets_reclaimed(conn):
    run_id = await start_run(conn, "double_twice", {"x": 1})

    first = await claim_run(conn, "worker-0", lease_seconds=1)
    assert first[0] == run_id                                              # worker-0 takes it
    assert await claim_run(conn, "worker-1") is None                       # still leased → nothing for worker-1

    await asyncio.sleep(1.5)                                               # worker-0 "dies": no heartbeat
    second = await claim_run(conn, "worker-1")
    assert second[0] == run_id                                             # lease expired → worker-1 takes over
    assert second[1] == first[1] + 1                                       # new claim → new fencing token

    cur = await conn.execute("SELECT lease_owner FROM runs WHERE id = %s", (run_id,))
    assert (await cur.fetchone())[0] == "worker-1"                         # the row now says worker-1
    assert await renew_lease(conn, run_id, first[1]) is False            # worker-0 lost it


@pytest.mark.asyncio
async def test_heartbeat_keeps_lease_alive_then_it_expires(conn):
    run_id = await start_run(conn, "double_twice", {"x": 1})
    _, v = await claim_run(conn, "worker-0", lease_seconds=1)             # a 1-second lease

    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as hb_conn:
        hb = asyncio.create_task(
            heartbeat(hb_conn, run_id, v, lease_seconds=1, every=0.3)
        )
        await asyncio.sleep(2.5)                     # way longer than the 1s lease…
        assert await lease_is_valid(conn, run_id)    # …but the heartbeat kept it alive

        hb.cancel()                                  # the worker "dies"
        try:
            await hb
        except asyncio.CancelledError:
            pass

    await asyncio.sleep(1.5)                         # nobody renews now
    assert not await lease_is_valid(conn, run_id)    # → the lease expired



@pytest.mark.asyncio
async def test_renew_fails_with_a_stale_version(conn):
    run_id = await start_run(conn, "double_twice", {"x": 1})
    _, v = await claim_run(conn, "worker-0")

    assert await renew_lease(conn, run_id, v) is True        # current ticket → renewed
    assert await renew_lease(conn, run_id, v - 1) is False   # old ticket → refused


@workflow
async def add_twice(x):
    a = await add(x, x)          # step 1: 2x
    return await add(a, 1)       # step 2: 2x + 1


@pytest.mark.asyncio
async def test_workers_complete_all_runs(conn):
    expected = {}
    for x in range(20):
        run_id = await start_run(conn, "add_twice", {"x": x})
        expected[run_id] = 2 * x + 1

    stop = asyncio.Event()
    async with AsyncConnectionPool(
        DSN, kwargs={"autocommit": True}, min_size=2, max_size=12, open=False
    ) as pool:
        workers = [
            asyncio.create_task(worker_loop(pool, f"worker-{i}", poll_interval=0.1, stop=stop))
            for i in range(3)
        ]
        for _ in range(150):                              # wait up to ~15 seconds
            cur = await conn.execute("SELECT count(*) FROM runs WHERE status = 'completed'")
            if (await cur.fetchone())[0] == 20:
                break
            await asyncio.sleep(0.1)
        stop.set()                                        # tell the workers to finish
        await asyncio.gather(*workers)                    # wait for them to exit

    cur = await conn.execute("SELECT id, status, output FROM runs")
    for run_id, status, output in await cur.fetchall():
        assert status == "completed"
        assert output == expected[run_id]                 # right answer for every run

    cur = await conn.execute("SELECT run_id, count(*) FROM steps GROUP BY run_id")
    rows = await cur.fetchall()
    assert len(rows) == 20
    assert all(n == 2 for _, n in rows)                   # exactly 2 steps each, none doubled
