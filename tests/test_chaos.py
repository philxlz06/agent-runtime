import asyncio
import os
import random
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from agent_runtime.workflow import start_run

SCRIPT = Path(__file__).parent / "worker_process.py"
N_RUNS = 30
N_WORKERS = 3


def start_worker():
    return subprocess.Popen([sys.executable, str(SCRIPT)], env={**os.environ, "LEASE_SECONDS": "2"})


@pytest.mark.asyncio
async def test_chaos_every_run_finishes_exactly_once(conn):
    expected = {}
    for x in range(N_RUNS):
        run_id = await start_run(conn, "chaos_flow", {"x": x})
        expected[run_id] = 2 * x + 1

    workers = [start_worker() for _ in range(N_WORKERS)]
    kills = 0
    try:
        for i in range(600):                                   # wait up to ~60 seconds
            cur = await conn.execute("SELECT count(*) FROM runs WHERE status = 'completed'")
            if (await cur.fetchone())[0] == N_RUNS:
                break
            if i % 10 == 5 and kills < 8:                      # about once a second, up to 8 times…
                victim = random.choice(workers)
                victim.send_signal(signal.SIGKILL)             # …kill a random worker mid-work 💥
                victim.wait()
                workers.remove(victim)
                workers.append(start_worker())                 # …and start a replacement
                kills += 1
            await asyncio.sleep(0.1)
    finally:
        for w in workers:                                      # always clean up the processes
            w.kill()
            w.wait()

    assert kills > 0                                           # we really did kill workers

    cur = await conn.execute("SELECT id, status, output FROM runs")
    for run_id, status, output in await cur.fetchall():
        assert status == "completed"                           # every run finished…
        assert output == expected[run_id]                      # …with the right answer

    cur = await conn.execute("SELECT run_id, count(*) FROM steps GROUP BY run_id")
    assert all(n == 3 for _, n in await cur.fetchall())        # no step saved twice

    cur = await conn.execute("SELECT count(*) FROM effects")
    assert (await cur.fetchone())[0] == N_RUNS                 # exactly one refund per run


@pytest.mark.asyncio
async def test_worker_exits_cleanly_on_sigterm(conn):
    p = start_worker()
    await asyncio.sleep(1.5)                  # let it start up and begin polling
    p.send_signal(signal.SIGTERM)             # what `docker stop` sends
    assert p.wait(timeout=5) == 0             # exited on its own, cleanly
