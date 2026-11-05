import uuid
from contextvars import ContextVar
from dataclasses import dataclass

from psycopg import AsyncConnection


@dataclass
class RunContext:
    run_id: uuid.UUID
    conn: AsyncConnection | None = None
    seq: int = 0
    in_step: bool = False
    step_key: str | None = None   # key of the step running right now

    def next_seq(self) -> int:
        self.seq += 1
        return self.seq


current_run: ContextVar[RunContext] = ContextVar("current_run")