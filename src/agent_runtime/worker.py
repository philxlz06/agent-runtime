import asyncio
import logging

from agent_runtime.step import LeaseLostError
from agent_runtime.workflow import run_workflow


log = logging.getLogger(__name__)

LEASE_SECONDS = 30


async def claim_run(conn, worker_id, lease_seconds=LEASE_SECONDS):
    """Atomically take the oldest pending run and lease it. Returns its id, or None."""
    cur = await conn.execute(
        """
        UPDATE runs
        SET status = 'running',
            lease_owner = %s,
            lease_expires_at = now() + %s * interval '1 second',
            lease_version = lease_version + 1,              
            updated_at = now()
        WHERE id = (
            SELECT id FROM runs
            WHERE status = 'pending'
                OR (status = 'running' AND lease_expires_at < now() )
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id, lease_version
        """,
        (worker_id, lease_seconds),
    )
    row = await cur.fetchone()
    return row if row else None


async def process_run(pool, run_id, lease_version, lease_seconds=LEASE_SECONDS):
    """Run one claimed run with a heartbeat beside it. Never raises."""
    async with pool.connection() as work_conn, pool.connection() as hb_conn:
        hb = asyncio.create_task(heartbeat(hb_conn, run_id, lease_version, lease_seconds))
        try:
            await run_workflow(work_conn, run_id, lease_version=lease_version)
        except LeaseLostError:
            log.warning("lost the lease on run %s; another worker has it", run_id)
        except Exception:
            log.exception("run %s failed", run_id)
        finally:
            hb.cancel()
            try:
                await hb
            except asyncio.CancelledError:
                pass


async def worker_loop(pool, worker_id, *, lease_seconds=LEASE_SECONDS,
                      poll_interval=1.0, stop=None):
    """Claim and run work forever, until `stop` is set."""
    while not (stop and stop.is_set()):
        async with pool.connection() as conn:
            claim = await claim_run(conn, worker_id, lease_seconds)
        if claim is None:
            await asyncio.sleep(poll_interval)
            continue
        run_id, lease_version = claim
        await process_run(pool, run_id, lease_version, lease_seconds)


async def renew_lease(conn, run_id, lease_version, lease_seconds=LEASE_SECONDS):
    """Push the lease forward. Returns False if our ticket is no longer current."""
    cur = await conn.execute(
        """
        UPDATE runs
        SET lease_expires_at = now() + %s * interval '1 second', updated_at = now()
        WHERE id = %s AND lease_version = %s AND status = 'running'
        """,
        (lease_seconds, run_id, lease_version),
    )
    return cur.rowcount == 1


async def heartbeat(conn, run_id, lease_version, lease_seconds=LEASE_SECONDS, every=None):
    """Renew the lease every few seconds, until cancelled or the lease is lost."""
    every = every or lease_seconds / 3
    while True:
        await asyncio.sleep(every)
        if not await renew_lease(conn, run_id, lease_version, lease_seconds):
            return
