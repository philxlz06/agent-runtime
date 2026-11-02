import functools

from psycopg.types.json import Jsonb

from agent_runtime.context import current_run


class NonDeterminismError(Exception):
    """Workflow took a different path on replay than the first time."""


def step(fn):
    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        ctx = current_run.get()
        seq = ctx.next_seq()
        name = fn.__name__

        # 1. Already done? Replay the saved output.
        cur = await ctx.conn.execute(
            "SELECT name, output FROM steps "
            "WHERE run_id = %s AND seq = %s AND status = 'completed'",
            (ctx.run_id, seq),
        )
        row = await cur.fetchone()
        if row is not None:
            saved_name, saved_output = row
            if saved_name != name:
                raise NonDeterminismError(
                    f"step {seq}: journal has {saved_name}, code called {name}"
                )
            return saved_output

        # 2. Not done yet: run it for real.
        result = await fn(*args, **kwargs)

        # 3. Save the result.
        async with ctx.conn.transaction():
            await ctx.conn.execute(
                "INSERT INTO steps "
                "(run_id, seq, name, idempotency_key, status, output, completed_at) "
                "VALUES (%s, %s, %s, %s, 'completed', %s, now())",
                (ctx.run_id, seq, name, f"{ctx.run_id}:{seq}", Jsonb(result)),
            )
        return result

    return wrapper