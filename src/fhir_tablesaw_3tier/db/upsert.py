"""Postgres upsert helpers.

This project targets Postgres for real loads. For performance at scale we avoid
ORM patterns that do SELECT-per-row existence checks.

Instead we use Postgres-native:
- INSERT .. ON CONFLICT .. RETURNING id
- INSERT .. ON CONFLICT DO NOTHING (for join tables)

These helpers keep the call sites compact and consistent.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import ColumnElement
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


def is_postgres(session: Session) -> bool:
    bind: Engine | None = session.get_bind()  # type: ignore[assignment]
    if bind is None:
        return False
    return bind.dialect.name == "postgresql"


def execute_returning_scalar(session: Session, stmt) -> Any:
    """Execute statement and return scalar_one().

    Intended for INSERT/UPDATE statements that include RETURNING.
    """

    return session.execute(stmt).scalar_one()


def coalesce_str(value: str | None) -> str:
    return value if value is not None else ""
