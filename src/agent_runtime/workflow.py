import uuid

from psycopg.types.json import Jsonb

from agent_runtime.context import RunContext, current_run
from agent_runtime.step import LeaseLostError

REGISTRY: dict = {}


def workflow(fn):
    REGISTRY[fn.__name__] = fn
    return fn


async def start_run(conn, workflow_name, input=None) -> uuid.UUID:
    cur = await conn.execute(
        "INSERT INTO runs (workflow_name, input) VALUES (%s, %s) RETURNING id",
        (workflow_name, Jsonb(input or {})),
    )
    (run_id,) = await cur.fetchone()
    return run_id


async def run_workflow(conn, run_id, lease_version=None):
    cur = await conn.execute(
        "SELECT workflow_name, input FROM runs WHERE id = %s", (run_id,)
    )
    workflow_name, input = await cur.fetchone()
    fn = REGISTRY[workflow_name]

    await conn.execute(
        "UPDATE runs SET status = 'running', updated_at = now() WHERE id = %s",
        (run_id,),
    )

    token = current_run.set(
        RunContext(run_id=run_id, conn=conn, lease_version=lease_version)
    )
    try:
        output = await fn(**input)
    except LeaseLostError:
        raise                          # not our run anymore: don't touch its row at all
    except Exception as e:
        await conn.execute(
            "UPDATE runs SET status = 'failed', error = %s, updated_at = now() "
            "WHERE id = %s AND lease_version = COALESCE(%s, lease_version)",
            (repr(e), run_id, lease_version),
        )
        raise
    finally:
        current_run.reset(token)

    await conn.execute(
        "UPDATE runs SET status = 'completed', output = %s, updated_at = now() "
        "WHERE id = %s AND lease_version = COALESCE(%s, lease_version)",
        (Jsonb(output), run_id, lease_version),
    )
    return output