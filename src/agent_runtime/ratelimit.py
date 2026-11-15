import time
from collections import defaultdict, deque


class RateLimiter:
    """Allow at most `limit` hits per `per_seconds`, separately for each key (like an IP)."""

    def __init__(self, limit, per_seconds):
        self.limit = limit
        self.per = per_seconds
        self.hits = defaultdict(deque)          # key → timestamps of recent hits

    def allow(self, key):
        now = time.monotonic()
        recent = self.hits[key]
        while recent and recent[0] <= now - self.per:
            recent.popleft()                    # forget hits older than the window
        if len(recent) >= self.limit:
            return False                        # too many recently → refuse
        recent.append(now)
        return True
