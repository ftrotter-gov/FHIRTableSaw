"""Minimal .env loader.

We intentionally avoid adding a new dependency (e.g., python-dotenv).
This loader supports simple KEY=VALUE lines and ignores comments/blank lines.
"""

from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: str | Path = ".env", *, override: bool = False) -> dict[str, str]:
    """Load a .env file into process environment.

    Returns dict of loaded key/value pairs.
    """

    p = Path(path)
    if not p.exists():
        return {}

    loaded: dict[str, str] = {}
    for raw_line in p.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if not k:
            continue
        loaded[k] = v
        if override or k not in os.environ:
            os.environ[k] = v

    return loaded


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_db_url() -> str:
    """Get the DATABASE_URL from environment.

    Returns:
        PostgreSQL connection URL

    Raises:
        ValueError: If DATABASE_URL is not set
    """
    return require_env("DATABASE_URL")
