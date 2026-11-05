import functools

from psycopg.types.json import Jsonb

from agent_runtime.context import current_run


class NonDeterminismError(Exception):
    """Workflow took a different path on replay than the first time."""


class ParallelStepError(Exception):
    """Two steps of the same run ran at the same time."""


def idempotency_key() -> str:
    """The key for the step running right now. Pass it to outside APIs."""
    ctx = current_run.get()
    if ctx.step_key is None:
        raise RuntimeError("idempotency_key() only works inside a @step")
    return ctx.step_key


def step(fn):
    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        ctx = current_run.get()
        if ctx.in_step:
            raise ParallelStepError("steps must run one at a time (D20)")
        ctx.in_step = True
        try:
            seq = ctx.next_seq()
            name = fn.__name__
            key = f"{ctx.run_id}:{seq}"

            # 1. Check the journal.
            cur = await ctx.conn.execute(
                "SELECT name, status, output FROM steps WHERE run_id = %s AND seq = %s",
                (ctx.run_id, seq),
            )
            row = await cur.fetchone()
            if row is not None:
                saved_name, status, saved_output = row
                if saved_name != name:
                    raise NonDeterminismError(
                        f"step {seq}: journal has {saved_name}, code called {name}"
                    )
                if status == "completed":
                    return saved_output
                # status == "started": crashed mid-step last time → run it again

            # 2. Mark it started (skipped if the started row already exists).
            await ctx.conn.execute(
                "INSERT INTO steps (run_id, seq, name, idempotency_key, status) "
                "VALUES (%s, %s, %s, %s, 'started') "
                "ON CONFLICT (run_id, seq) DO NOTHING",
                (ctx.run_id, seq, name, key),
            )

            # 3. Run it for real, with its key available.
            ctx.step_key = key
            try:
                result = await fn(*args, **kwargs)
            finally:
                ctx.step_key = None

            # 4. Mark it completed.
            async with ctx.conn.transaction():
                await ctx.conn.execute(
                    "UPDATE steps SET status = 'completed', output = %s, "
                    "completed_at = now() WHERE run_id = %s AND seq = %s",
                    (Jsonb(result), ctx.run_id, seq),
                )
            return result
        finally:
            ctx.in_step = False

    return wrapper