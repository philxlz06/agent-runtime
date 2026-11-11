import uuid

from psycopg.types.json import Jsonb

from agent_runtime.context import RunContext, current_run
from agent_runtime.step import LeaseLostError
from agent_runtime.approval import ApprovalNeeded


REGISTRY: dict = {}


def workflow(fn):
    REGISTRY[fn.__name__] = fn
    return fn


async def start_run(conn, workflow_name, input=None, request_key=None) -> uuid.UUID:
    cur = await conn.execute(
        "INSERT INTO runs (workflow_name, input, request_key) VALUES (%s, %s, %s) "
        "ON CONFLICT (request_key) DO NOTHING RETURNING id",
        (workflow_name, Jsonb(input or {}), request_key),
    )
    row = await cur.fetchone()
    if row is None:                         # this key was used before → return that run
        cur = await conn.execute("SELECT id FROM runs WHERE request_key = %s", (request_key,))
        row = await cur.fetchone()
    return row[0]

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
    except ApprovalNeeded as park:
        await conn.execute(
            "UPDATE runs SET status = 'waiting_approval', lease_owner = NULL, "
            "lease_expires_at = NULL, updated_at = now() "
            "WHERE id = %s AND lease_version = COALESCE(%s, lease_version)",
            (run_id, lease_version),
        )
        # a human may have decided in the instant before we parked → don't leave it stranded
        cur = await conn.execute(
            "SELECT status FROM approvals WHERE run_id = %s AND seq = %s", (run_id, park.seq)
        )
        row = await cur.fetchone()
        if row is not None and row[0] != "pending":
            await conn.execute(
                "UPDATE runs SET status = 'pending' WHERE id = %s AND status = 'waiting_approval'",
                (run_id,),
            )
        return None                    # parked, not finished
    except LeaseLostError:
        raise                        # unchanged                          # not our run anymore: don't touch its row at all
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
