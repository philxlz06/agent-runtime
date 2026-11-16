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
from fastapi import Query
from agent_runtime.ratelimit import RateLimiter
import os
import secrets
from pathlib import Path
from fastapi.responses import FileResponse
from pydantic import Field


def require_admin(x_admin_key: str | None = Header(default=None)):
    """Only requests carrying the right X-Admin-Key header get through."""
    expected = os.environ.get("ADMIN_API_KEY")
    if not expected or not x_admin_key or not secrets.compare_digest(x_admin_key, expected):
        raise HTTPException(status_code=401, detail="admin key required")


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


@app.post("/runs", status_code=201, dependencies=[Depends(require_admin)])
async def create_run(
    body: StartRun,
    idempotency_key: str | None = Header(default=None),
    conn=Depends(get_conn),
):
    run_id = await start_run(conn, body.workflow, body.input, request_key=idempotency_key)
    return {"id": run_id}


@app.get("/runs")
async def list_runs(
    status: str | None = None,
    workflow: str | None = None,
    limit: int = 50,
    conn=Depends(get_conn),
):
    cur = conn.cursor(row_factory=dict_row)
    await cur.execute(
        "SELECT r.id, r.workflow_name, r.status, r.lease_owner, r.created_at, r.updated_at, "
        "  (SELECT count(*) FROM steps s WHERE s.run_id = r.id AND s.status = 'completed') AS steps_done, "
        "  (SELECT count(DISTINCT s.lease_version) FROM steps s "
        "     WHERE s.run_id = r.id AND s.lease_version IS NOT NULL) > 1 AS resumed "
        "FROM runs r "
        "WHERE (%s::text IS NULL OR r.status = %s) "
        "  AND (%s::text IS NULL OR r.workflow_name = %s) "
        "ORDER BY r.created_at DESC LIMIT %s",
        (status, status, workflow, workflow, limit),
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
        "SELECT seq, name, status, lease_version, output, started_at, completed_at "
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


@app.post("/runs/{run_id}/approvals/{seq}", dependencies=[Depends(require_admin)])
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


demo_start_limit = RateLimiter(limit=5, per_seconds=60)   # 5 clicks per minute per visitor
MAX_DEMO_BACKLOG = 50                                     # global brake


@app.post("/demo/runs", status_code=201)
async def demo_start(
    request: Request,
    count: int = Query(1, ge=1, le=3),
    conn=Depends(get_conn),
):
    if not demo_start_limit.allow(request.client.host):
        raise HTTPException(status_code=429, detail="slow down: 5 demo requests per minute")
    cur = await conn.execute("SELECT count(*) FROM runs WHERE status IN ('pending', 'running')")
    if (await cur.fetchone())[0] >= MAX_DEMO_BACKLOG:
        raise HTTPException(status_code=429, detail="the demo is busy, try again in a moment")
    ids = []
    for _ in range(count):
        order_id = f"D-{secrets.token_hex(3)}"            # e.g. D-4f9a1c
        ids.append(await start_run(conn, "refund_demo", {"order_id": order_id}))
    return {"ids": ids}


@app.get("/demo/stats")
async def demo_stats(conn=Depends(get_conn)):
    cur = conn.cursor(row_factory=dict_row)
    await cur.execute(
        "SELECT "
        " count(*) FILTER (WHERE status = 'pending')   AS pending,"
        " count(*) FILTER (WHERE status = 'running')   AS running,"
        " count(*) FILTER (WHERE status = 'completed') AS completed,"
        " count(*) FILTER (WHERE status = 'failed')    AS failed "
        "FROM runs WHERE workflow_name = 'refund_demo'"
    )
    stats = await cur.fetchone()
    await cur.execute("SELECT count(*) AS refunds FROM demo_refunds")
    stats.update(await cur.fetchone())
    return stats


demo_kill_limit = RateLimiter(limit=1, per_seconds=20)    # one kill per 20s, for EVERYONE


@app.post("/demo/kill", status_code=202)
async def demo_kill(conn=Depends(get_conn)):
    if not demo_kill_limit.allow("global"):
        raise HTTPException(status_code=429, detail="a worker was just killed, try again soon")
    await conn.execute("INSERT INTO kill_requests DEFAULT VALUES")
    return {"queued": True}


STATIC = Path(__file__).parent / "static"


@app.get("/", include_in_schema=False)
async def home():
    return FileResponse(STATIC / "index.html")


agent_limit = RateLimiter(limit=3, per_seconds=60)             # per visitor
agent_daily_limit = RateLimiter(limit=200, per_seconds=86400)   # everyone combined, per day


class SupportMessage(BaseModel):
    message: str = Field(min_length=3, max_length=300)


@app.post("/demo/support", status_code=201)
async def demo_support(request: Request, body: SupportMessage, conn=Depends(get_conn)):
    if not agent_limit.allow(request.client.host):
        raise HTTPException(status_code=429, detail="slow down: 3 agent messages per minute")
    if not agent_daily_limit.allow("global"):
        raise HTTPException(status_code=429, detail="the agent demo hit today's limit, try tomorrow")
    order_id = f"D-{secrets.token_hex(3)}"
    run_id = await start_run(
        conn, "support_agent", {"message": body.message, "order_id": order_id, "source": "demo"}
    )
    return {"id": run_id}


@app.post("/demo/support/{run_id}/decision")
async def demo_support_decision(run_id: UUID, body: Decision, conn=Depends(get_conn)):
    cur = await conn.execute(
        "SELECT input->>'source' FROM runs WHERE id = %s AND workflow_name = 'support_agent'",
        (run_id,),
    )
    row = await cur.fetchone()
    if row is None or row[0] != "demo":
        raise HTTPException(status_code=404, detail="not a demo agent run")
    cur = await conn.execute(
        "SELECT seq FROM approvals WHERE run_id = %s AND status = 'pending'", (run_id,)
    )
    pending = await cur.fetchone()
    if pending is None or not await decide(
        conn, run_id, pending[0], body.approved, "demo visitor (playing staff)"
    ):
        raise HTTPException(status_code=409, detail="no pending approval for this run")
    return {"ok": True}
