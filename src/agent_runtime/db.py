import os

DSN = os.environ.get(
    "DATABASE_URL", "postgresql://runtime:runtime@localhost:5432/runtime"
)