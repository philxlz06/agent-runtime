from agent_runtime.step import step
from agent_runtime.workflow import workflow


@step
async def greet(name):
    return f"hello, {name}"


@workflow
async def hello(name="world"):
    return await greet(name)
