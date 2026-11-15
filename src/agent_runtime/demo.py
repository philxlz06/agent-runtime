import asyncio
import os
from agent_runtime.context import current_run
from agent_runtime.step import idempotency_key, step
from agent_runtime.workflow import workflow


@step
async def greet(name):
    return f"hello, {name}"


@workflow
async def hello(name="world"):
    return await greet(name)


async def pause():
    """Slow each demo step down so viewers can watch (and kill a worker mid-run)."""
    await asyncio.sleep(float(os.environ.get("DEMO_STEP_DELAY", "2")))


async def record_refund(key, order_id, amount):
    """Fake payment provider: one refund per idempotency key, ever."""
    conn = current_run.get().conn
    cur = await conn.execute(
        "INSERT INTO demo_refunds (idempotency_key, order_id, amount) VALUES (%s, %s, %s) "
        "ON CONFLICT (idempotency_key) DO NOTHING RETURNING id",
        (key, order_id, amount),
    )
    row = await cur.fetchone()
    if row is None:                                    # retried → return the original refund
        cur = await conn.execute(
            "SELECT id FROM demo_refunds WHERE idempotency_key = %s", (key,)
        )
        row = await cur.fetchone()
    return row[0]


@step
async def check_order(order_id):
    await pause()
    return {"order_id": order_id, "amount": 25}


@step
async def issue_refund(order):
    await pause()
    return await record_refund(idempotency_key(), order["order_id"], order["amount"])


@step
async def notify_customer(order_id):
    await pause()
    return f"emailed customer about order {order_id}"


@workflow
async def refund_demo(order_id):
    order = await check_order(order_id)        # step 1: ~2s
    refund_id = await issue_refund(order)      # step 2: ~2s, the side effect
    await notify_customer(order_id)            # step 3: ~2s
    return {"refund_id": refund_id}
