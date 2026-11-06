import os
import subprocess
import sys
from pathlib import Path

import pytest

from agent_runtime.workflow import start_run

SCRIPT = Path(__file__).parent / "crash_workflow.py"

####
def run_worker(run_id, log, crash_at=None, crash_after_refund=False):
    env = {**os.environ, "CALLS_LOG": str(log)}
    if crash_at:
        env["CRASH_AT"] = str(crash_at)
    if crash_after_refund:
        env["CRASH_AFTER_REFUND"] = "1"
    # sys.executable = the venv's python; SCRIPT = the worker file to run
    return subprocess.run([sys.executable, str(SCRIPT), str(run_id)], env=env)
#####

@pytest.mark.asyncio
async def test_resumes_after_kill(conn, tmp_path):
    log = tmp_path / "calls.log"
    run_id = await start_run(conn, "five_steps")

    # 1. Worker gets killed at the start of step 4
    first = run_worker(run_id, log, crash_at=4)
    assert first.returncode == -9
    assert log.read_text().split() == ["1", "2", "3"]

    cur = await conn.execute("SELECT status FROM runs WHERE id = %s", (run_id,))
    assert (await cur.fetchone())[0] == "running"

    # 2. A fresh worker picks up the same run
    second = run_worker(run_id, log)
    assert second.returncode == 0
    assert log.read_text().split() == ["1", "2", "3", "4", "5"]

    cur = await conn.execute("SELECT status FROM runs WHERE id = %s", (run_id,))
    assert (await cur.fetchone())[0] == "completed"

####
@pytest.mark.asyncio
async def test_crash_after_refund_does_not_refund_twice(conn, tmp_path):
    log = tmp_path / "calls.log"
    run_id = await start_run(conn, "refund_flow", {"amount": 40})

    # 1. Worker refunds, then dies before the step can save
    first = run_worker(run_id, log, crash_after_refund=True)
    assert first.returncode == -9

    cur = await conn.execute("SELECT count(*) FROM effects")
    assert (await cur.fetchone())[0] == 1                  # the refund DID happen

    cur = await conn.execute("SELECT status FROM steps WHERE run_id = %s", (run_id,))
    assert await cur.fetchall() == [("started",)]          # but the step never saved

    # 2. A fresh worker reruns the step with the SAME key
    second = run_worker(run_id, log)
    assert second.returncode == 0

    cur = await conn.execute("SELECT count(*) FROM effects")
    assert (await cur.fetchone())[0] == 1                  # STILL one refund ✅

    cur = await conn.execute("SELECT status, output FROM runs WHERE id = %s", (run_id,))
    status, output = await cur.fetchone()
    assert status == "completed"
    assert output["refund_id"] == f"re_{run_id}:1"         # same answer as the first try

####