import asyncio

import psycopg
import pytest

from agent_runtime.db import DSN
from agent_runtime.worker import claim_run
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