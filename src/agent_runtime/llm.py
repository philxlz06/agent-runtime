import os

from anthropic import AsyncAnthropic

MODEL = os.environ.get("AGENT_MODEL", "claude-haiku-5-5")
_client = None


def client():
    """Create the Claude client once, the first time it's needed."""
    global _client
    if _client is None:
        _client = AsyncAnthropic()        # reads ANTHROPIC_API_KEY from the environment
    return _client


async def ask(prompt, max_tokens=200):
    """Send one prompt to Claude and return its text answer."""
    response = await client().messages.create(
        model=MODEL,
        max_tokens=max_tokens,
        thinking={"type": "disabled"},    # simple tasks: no extended reasoning, cheaper and faster
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()
