"""Chaos benchmark: many runs, random SIGKILLs and SIGSTOP freezes, then check every invariant."""
import asyncio
import os
import random
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent.parent
# so we can import fake_payments
sys.path.insert(0, str(ROOT / "tests"))

from agent_runtime.db import DSN                          # noqa: E402
from agent_runtime.migrate import migrate                 # noqa: E402
from agent_runtime.workflow import start_run              # noqa: E402
from fake_payments import SCHEMA as EFFECTS_SCHEMA        # noqa: E402

WORKER = ROOT / "tests" / "worker_process.py"
N_WORKERS = int(os.environ.get("N_WORKERS", "4"))
N_ATTACKS = int(os.environ.get("N_KILLS", "1000"))
LEASE = os.environ.get("LEASE_SECONDS", "2")


def start_worker():
    return subprocess.Popen(
        [sys.executable, str(WORKER)],
        env={**os.environ, "LEASE_SECONDS": LEASE},
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


async def count(conn, sql, params=()):
    cur = await conn.execute(sql, params)
    return (await cur.fetchone())[0]


async def main():
    if "localhost" not in DSN and "127.0.0.1" not in DSN:
        sys.exit(
            "refusing to run chaos against a non-local database (it wipes tables)")

    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        await migrate(conn)
        await conn.execute(EFFECTS_SCHEMA)
        await conn.execute("TRUNCATE steps, approvals, runs, effects")

        workers = [start_worker() for _ in range(N_WORKERS)]
        frozen = {}                 # process → time to wake it
        watching = []               # (run_id, victim_id, attack_time)
        recoveries = []             # seconds until another worker took over
        kills = freezes = created = 0
        t0 = time.monotonic()
        try:
            while kills + freezes < N_ATTACKS:
                # 1. keep ~50 runs waiting, so workers are always busy
                backlog = await count(conn, "SELECT count(*) FROM runs WHERE status <> 'completed'")
                for _ in range(max(0, 50 - backlog)):
                    await start_run(conn, "chaos_flow", {"x": created})
                    created += 1

                # 2. wake frozen workers whose lease has surely expired → they're zombies now
                now = time.monotonic()
                for proc in [p for p, wake in frozen.items() if wake <= now]:
                    proc.send_signal(signal.SIGCONT)
                    del frozen[proc]

                # 3. attack a random awake worker
                awake = [w for w in workers if w not in frozen]
                if awake:
                    victim = random.choice(awake)
                    victim_id = f"proc-{victim.pid}"
                    cur = await conn.execute(
                        "SELECT id FROM runs WHERE status = 'running' AND lease_owner = %s",
                        (victim_id,),
                    )
                    held = [row[0] for row in await cur.fetchall()]
                    if random.random() < 0.2:                       # 20%: freeze → future zombie
                        victim.send_signal(signal.SIGSTOP)
                        frozen[victim] = time.monotonic() + float(LEASE) + 1
                        freezes += 1
                    else:                                            # 80%: kill + replace
                        victim.send_signal(signal.SIGKILL)
                        victim.wait()
                        workers.remove(victim)
                        workers.append(start_worker())
                        kills += 1
                    watching += [(rid, victim_id, time.monotonic())
                                 for rid in held]

                # 4. which watched runs has another worker taken over?
                still = []
                for rid, victim_id, t in watching:
                    cur = await conn.execute(
                        "SELECT status, lease_owner FROM runs WHERE id = %s", (
                            rid,)
                    )
                    status, owner = await cur.fetchone()
                    if status == "completed" or owner != victim_id:
                        recoveries.append(time.monotonic() - t)
                    else:
                        still.append((rid, victim_id, t))
                watching = still

                await asyncio.sleep(0.3)

            # wake everyone, then wait for the remaining runs to finish
            for proc in frozen:
                proc.send_signal(signal.SIGCONT)
            for _ in range(240):                                     # up to ~2 minutes
                if await count(conn, "SELECT count(*) FROM runs WHERE status NOT IN ('completed', 'failed')") == 0:
                    break
                await asyncio.sleep(0.5)
        finally:
            for w in workers:
                w.kill()
                w.wait()
        elapsed = time.monotonic() - t0

        total = await count(conn, "SELECT count(*) FROM runs")
        completed = await count(conn, "SELECT count(*) FROM runs WHERE status = 'completed'")
        wrong = await count(conn,
                            "SELECT count(*) FROM runs WHERE status = 'completed' "
                            "AND output::int <> 2 * (input->>'x')::int + 1")
        bad_journals = await count(conn,
                                   "SELECT count(*) FROM (SELECT run_id FROM steps GROUP BY run_id HAVING count(*) <> 3) x")
        effects = await count(conn, "SELECT count(*) FROM effects")

    q = statistics.quantiles(recoveries, n=100) if len(
        recoveries) >= 2 else None
    print(
        f"attacks:      {kills} SIGKILL + {freezes} SIGSTOP freezes ({N_WORKERS} workers)")
    print(
        f"runs:         {completed}/{total} completed, {wrong} wrong outputs")
    print(f"journal:      {bad_journals} runs with a wrong number of steps")
    print(
        f"refunds:      {effects} for {completed} runs → {effects - completed} duplicates")
    if q:
        print(
            f"recovery:     p50 {q[49]:.2f}s · p95 {q[94]:.2f}s  (lease {LEASE}s, {len(recoveries)} takeovers)")
    print(
        f"throughput:   {completed / elapsed:.1f} runs/s under attack ({elapsed:.0f}s total)")


if __name__ == "__main__":
    asyncio.run(main())
