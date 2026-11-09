# agent-runtime
- Python 3.13, uv, src layout (code in src/agent_runtime/), pytest + pytest-asyncio
- Postgres via docker compose; tests create their own tables (tests/conftest.py)
- One branch per milestone; conventional commits (feat/fix/test/docs/refactor/chore)
- Don't commit; leave changes for review. Always run `uv run pytest -v` after changes.
- Keep changes minimal and match existing style; explain anything non-obvious.