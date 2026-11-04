import pytest

from fake_payments import refund


@pytest.mark.asyncio
async def test_same_key_refunds_once(conn):
    first = await refund("run1:2", 40)
    second = await refund("run1:2", 40)    # retry with the SAME key

    assert first == second                 # same answer both times
    cur = await conn.execute("SELECT count(*) FROM effects")
    assert (await cur.fetchone())[0] == 1  # but only ONE refund happened