import os
import subprocess
import sys
from pathlib import Path

import pytest

from agent_runtime.workflow import start_run

SCRIPT = Path(__file__).parent / "crash_workflow.py"


def run_worker(run_id, log, crash_at=None):
    env = {**os.environ, "CALLS_LOG": str(log)}
    if crash_at:
        env["CRASH_AT"] = str(crash_at)
    return subprocess.run([sys.executable, str(SCRIPT), str(run_id)], env=env)
#sys executable is the venv folder, str script is the crash workflow file to be ran


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