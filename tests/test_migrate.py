import pytest

from agent_runtime.migrate import migrate
from agent_runtime.workflow import start_run


@pytest.mark.asyncio
async def test_migrations_are_recorded(conn):
    cur = await conn.execute("SELECT name FROM schema_migrations ORDER BY name")
    names = [row[0] for row in await cur.fetchall()]
    assert names[:2] == ["001_initial.sql", "002_approvals.sql"]


@pytest.mark.asyncio
async def test_migrate_twice_changes_nothing(conn):
    await migrate(conn)          # already up to date → should skip everything, no errors
    await migrate(conn)


@pytest.mark.asyncio
async def test_runs_can_wait_for_approval(conn):
    run_id = await start_run(conn, "anything")
    await conn.execute(          # this would FAIL before 002 (the old CHECK rule)
        "UPDATE runs SET status = 'waiting_approval' WHERE id = %s", (run_id,)
    )
