import asyncio
import uuid

import pytest

from agent_runtime.context import RunContext, current_run


async def fake_run(n_steps):
    current_run.set(RunContext(run_id=uuid.uuid4()))
    seqs = []
    for _ in range(n_steps):
        await asyncio.sleep(0)  # let the other run go in between
        seqs.append(current_run.get().next_seq())
    return seqs


@pytest.mark.asyncio
async def test_each_run_has_its_own_counter():
    a, b = await asyncio.gather(fake_run(3), fake_run(5))
    assert a == [1, 2, 3]
    assert b == [1, 2, 3, 4, 5]