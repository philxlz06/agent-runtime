import asyncio

LEASE_SECONDS = 30


async def claim_run(conn, worker_id, lease_seconds=LEASE_SECONDS):
    """Atomically take the oldest pending run and lease it. Returns its id, or None."""
    cur = await conn.execute(
        """
        UPDATE runs
        SET status = 'running',
            lease_owner = %s,
            lease_expires_at = now() + %s * interval '1 second',
            updated_at = now()
        WHERE id = (
            SELECT id FROM runs
            WHERE status = 'pending'
                OR (status = 'running' AND lease_expires_at < now() )
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id
        """,
        (worker_id, lease_seconds),
    )
    row = await cur.fetchone()
    return row[0] if row else None


async def renew_lease(conn, run_id, worker_id, lease_seconds=LEASE_SECONDS):
    """Push the lease forward. Returns False if this worker no longer owns the run."""
    cur = await conn.execute(
        """
        UPDATE runs
        SET lease_expires_at = now() + %s * interval '1 second', updated_at = now()
        WHERE id = %s AND lease_owner = %s AND status = 'running'
        """,
        (lease_seconds, run_id, worker_id),
    )
    return cur.rowcount == 1


async def heartbeat(conn, run_id, worker_id, lease_seconds=LEASE_SECONDS, every=None):
    """Renew the lease every few seconds, until cancelled or the lease is lost."""
    every = every or lease_seconds / 3
    while True:
        await asyncio.sleep(every)
        if not await renew_lease(conn, run_id, worker_id, lease_seconds):
            return    # someone else owns it now, so stop renewing