# Chaos benchmark results

Run: `uv run python scripts/chaos.py` (local Postgres, MacBook Air M2, 4 workers, 2s lease)
Date: 2026-11-13

| Metric | Result |
|---|---|
| Attacks | 1,000 (790 SIGKILL, 210 SIGSTOP freezes → zombies) |
| Runs | 1,913 / 1,913 completed |
| Wrong outputs | 0 |
| Runs with doubled steps | 0 |
| Duplicate refunds | 0 |
| Recovery after a kill | p50 2.20s · p95 3.13s (≈ lease length) |
| Throughput under attack | 5.7 runs/s |

Recovery time scales with the lease: production uses a 30s lease, so expect about 30s there.
