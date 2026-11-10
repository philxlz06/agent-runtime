from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


async def migrate(conn):
    """Apply any migrations this database hasn't run yet, oldest first."""
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        " name TEXT PRIMARY KEY,"
        " applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
    )
    cur = await conn.execute("SELECT name FROM schema_migrations")
    done = {row[0] for row in await cur.fetchall()}

    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if path.name in done:
            continue                                         # already applied → skip
        async with conn.transaction():                       # all-or-nothing per file
            await conn.execute(path.read_text())
            await conn.execute(
                "INSERT INTO schema_migrations (name) VALUES (%s)", (path.name,)
            )


if __name__ == "__main__":
    import asyncio

    import psycopg

    from agent_runtime.db import DSN

    async def main():
        async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
            await migrate(conn)

    asyncio.run(main())
