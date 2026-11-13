from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel

from agent_runtime.approval import decide
from agent_runtime.db import DSN
from agent_runtime.workflow import start_run
from agent_runtime.migrate import migrate


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with AsyncConnectionPool(DSN, kwargs={"autocommit": True}, open=False) as pool:
        async with pool.connection() as conn:
            await migrate(conn)              # bring the DB up to date on every start
        app.state.pool = pool
        yield

app = FastAPI(title="agent-runtime", lifespan=lifespan)


async def get_conn(request: Request):
    async with request.app.state.pool.connection() as conn:
        yield conn                       # borrow one connection per request


class StartRun(BaseModel):
    workflow: str
    input: dict = {}


class Decision(BaseModel):
    approved: bool
    decided_by: str = "human"


@app.post("/runs", status_code=201)
async def create_run(
    body: StartRun,
    idempotency_key: str | None = Header(default=None),
    conn=Depends(get_conn),
):
    run_id = await start_run(conn, body.workflow, body.input, request_key=idempotency_key)
    return {"id": run_id}


@app.get("/runs")
async def list_runs(status: str | None = None, limit: int = 50, conn=Depends(get_conn)):
    cur = conn.cursor(row_factory=dict_row)
    await cur.execute(
        "SELECT id, workflow_name, status, created_at, updated_at FROM runs "
        "WHERE %s::text IS NULL OR status = %s "
        "ORDER BY created_at DESC LIMIT %s",
        (status, status, limit),
    )
    return await cur.fetchall()


@app.get("/runs/{run_id}")
async def get_run(run_id: UUID, conn=Depends(get_conn)):
    cur = conn.cursor(row_factory=dict_row)
    await cur.execute(
        "SELECT id, workflow_name, input, status, output, error, lease_owner, "
        "created_at, updated_at FROM runs WHERE id = %s",
        (run_id,),
    )
    run = await cur.fetchone()
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")

    await cur.execute(
        "SELECT seq, name, status, output, started_at, completed_at "
        "FROM steps WHERE run_id = %s ORDER BY seq",
        (run_id,),
    )
    run["steps"] = await cur.fetchall()

    await cur.execute(
        "SELECT seq, prompt, status, decided_by, decided_at "
        "FROM approvals WHERE run_id = %s ORDER BY seq",
        (run_id,),
    )
    run["approvals"] = await cur.fetchall()
    return run


@app.post("/runs/{run_id}/approvals/{seq}")
async def decide_approval(run_id: UUID, seq: int, body: Decision, conn=Depends(get_conn)):
    if not await decide(conn, run_id, seq, body.approved, body.decided_by):
        raise HTTPException(status_code=409, detail="no pending approval at that step")
    return {"ok": True}

@app.get("/health")
async def health_checkup(conn=Depends(get_conn)):
    cur = await conn.execute(
        "SELECT 1"
    )
    return {"ok": True}

