import asyncio

import psycopg
import pytest

from agent_runtime.db import DSN
from agent_runtime.worker import claim_run, heartbeat, renew_lease
from agent_runtime.workflow import start_run


async def claimer(worker_id):
    """One fake worker: keep claiming runs until there are none left."""
    claimed = []
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as c:
        while True:
            run_id = await claim_run(c, worker_id)
            if run_id is None:
                break
            claimed.append(run_id)
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

#m3
@pytest.mark.asyncio
async def test_renew_fails_if_not_the_owner(conn):
    run_id = await start_run(conn, "double_twice", {"x": 1})
    await claim_run(conn, "worker-0")

    assert await renew_lease(conn, run_id, "worker-0") is True    # owner → renewed
    assert await renew_lease(conn, run_id, "worker-1") is False   # not owner → refused


@pytest.mark.asyncio
async def test_heartbeat_keeps_lease_alive_then_it_expires(conn):
    run_id = await start_run(conn, "double_twice", {"x": 1})
    await claim_run(conn, "worker-0", lease_seconds=1)              # a 1-second lease

    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as hb_conn:
        hb = asyncio.create_task(
            heartbeat(hb_conn, run_id, "worker-0", lease_seconds=1, every=0.3)
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

#m3