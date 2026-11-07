async def claim_run(conn, worker_id):
    """Atomically take the oldest pending run. Returns its id, or None if there's no work."""
    cur = await conn.execute(
        """
        UPDATE runs
        SET status = 'running', lease_owner = %s, updated_at = now()
        WHERE id = (
            SELECT id FROM runs
            WHERE status = 'pending'
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id
        """,
        (worker_id,),
    )
    row = await cur.fetchone()
    return row[0] if row else None