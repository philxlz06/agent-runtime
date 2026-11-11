from psycopg.types.json import Jsonb

from agent_runtime.context import current_run
from agent_runtime.step import NonDeterminismError, _check_lease


class ApprovalNeeded(Exception):
    """Not an error: tells run_workflow to park this run until a human decides."""

    def __init__(self, seq):
        super().__init__(f"waiting for approval at step {seq}")
        self.seq = seq


async def require_approval(prompt):
    """Pause the workflow until a human approves or rejects. Returns True / False."""
    ctx = current_run.get()
    seq = ctx.next_seq()
    name = "require_approval"

    # 1. Already decided earlier in this run? (replay, like any step)
    cur = await ctx.conn.execute(
        "SELECT name, status, output FROM steps WHERE run_id = %s AND seq = %s",
        (ctx.run_id, seq),
    )
    row = await cur.fetchone()
    if row is not None:
        saved_name, status, saved_output = row
        if saved_name != name:
            raise NonDeterminismError(f"step {seq}: journal has {saved_name}, code called {name}")
        if status == "completed":
            return saved_output

    # 2. Has a human decided since we parked? → save the decision in the journal
    cur = await ctx.conn.execute(
        "SELECT status FROM approvals WHERE run_id = %s AND seq = %s", (ctx.run_id, seq)
    )
    row = await cur.fetchone()
    if row is not None and row[0] != "pending":
        approved = row[0] == "approved"
        async with ctx.conn.transaction():
            await _check_lease(ctx)
            await ctx.conn.execute(
                "INSERT INTO steps (run_id, seq, name, idempotency_key, status, output, completed_at) "
                "VALUES (%s, %s, %s, %s, 'completed', %s, now()) "
                "ON CONFLICT (run_id, seq) DO NOTHING",
                (ctx.run_id, seq, name, f"{ctx.run_id}:{seq}", Jsonb(approved)),
            )
        return approved

    # 3. Not decided yet → make sure the request exists, then park
    async with ctx.conn.transaction():
        await _check_lease(ctx)
        await ctx.conn.execute(
            "INSERT INTO approvals (run_id, seq, prompt) VALUES (%s, %s, %s) "
            "ON CONFLICT (run_id, seq) DO NOTHING",
            (ctx.run_id, seq, prompt),
        )
    raise ApprovalNeeded(seq)


async def decide(conn, run_id, seq, approved, decided_by="human"):
    """Record a human's decision and put the run back in line. False if nothing was pending."""
    async with conn.transaction():
        cur = await conn.execute(
            "UPDATE approvals SET status = %s, decided_by = %s, decided_at = now() "
            "WHERE run_id = %s AND seq = %s AND status = 'pending'",
            ("approved" if approved else "rejected", decided_by, run_id, seq),
        )
        if cur.rowcount == 0:
            return False
        await conn.execute(
            "UPDATE runs SET status = 'pending', updated_at = now() "
            "WHERE id = %s AND status = 'waiting_approval'",
            (run_id,),
        )
    return True
