import pytest

from agent_runtime.demo import refund_demo  # noqa: F401  (registers the workflow)
from agent_runtime.workflow import run_workflow, start_run
from agent_runtime import llm
from agent_runtime.approval import decide

@pytest.mark.asyncio
async def test_refund_demo_refunds_once(conn, monkeypatch):
    monkeypatch.setenv("DEMO_STEP_DELAY", "0")
    run_id = await start_run(conn, "refund_demo", {"order_id": "A-1"})
    output = await run_workflow(conn, run_id)

    cur = await conn.execute("SELECT count(*) FROM demo_refunds")
    assert (await cur.fetchone())[0] == 1                  # exactly one refund
    assert "refund_id" in output


def fake_claude(monkeypatch, answers):
    """Replace llm.ask with a fake that returns the given answers in order, and records each call."""
    calls = []
    remaining = iter(answers)

    async def fake_ask(prompt, max_tokens=200):
        calls.append(prompt)
        return next(remaining)

    monkeypatch.setattr(llm, "ask", fake_ask)
    return calls


@pytest.mark.asyncio
async def test_refund_request_waits_for_a_human_and_never_reasks_claude(conn, monkeypatch):
    monkeypatch.setenv("DEMO_STEP_DELAY", "0")
    calls = fake_claude(monkeypatch, ["refund", "Sorry about that, we've refunded you."])
    run_id = await start_run(conn, "support_agent", {"message": "I was charged twice", "order_id": "A-9"})

    assert await run_workflow(conn, run_id) is None                 # parked at the approval
    assert await decide(conn, run_id, seq=2, approved=True) is True

    output = await run_workflow(conn, run_id)                       # resumes after the human said yes
    assert output["category"] == "refund" and output["refunded"] is True
    assert len(calls) == 2                                          # classify + reply: classify was NOT re-asked on resume


@pytest.mark.asyncio
async def test_question_gets_a_reply_without_any_refund(conn, monkeypatch):
    monkeypatch.setenv("DEMO_STEP_DELAY", "0")
    fake_claude(monkeypatch, ["question", "Happy to help!"])
    run_id = await start_run(conn, "support_agent", {"message": "What are your hours?"})

    output = await run_workflow(conn, run_id)
    assert output == {"category": "question", "refunded": False, "reply": "Happy to help!"}
    cur = await conn.execute("SELECT count(*) FROM demo_refunds")
    assert (await cur.fetchone())[0] == 0
