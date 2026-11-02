import uuid
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass
class RunContext:
    run_id: uuid.UUID
    seq: int = 0  # last step number handed out

    def next_seq(self) -> int:
        self.seq += 1
        return self.seq


current_run: ContextVar[RunContext] = ContextVar("current_run")

