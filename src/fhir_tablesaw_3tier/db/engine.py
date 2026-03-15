"""SQLAlchemy engine helpers.

We support a configurable Postgres schema via env var `DB_SCHEMA`.

Approach:
- Do NOT hardcode table.schema on each model (keeps sqlite tests simple).
- Instead, for Postgres connections we:
  1) ensure the schema exists (`CREATE SCHEMA IF NOT EXISTS ...`)
  2) set `search_path` on each new connection to that schema

This causes `Base.metadata.create_all()` and normal ORM queries to operate
within that schema.
"""

from __future__ import annotations

import re

from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine


_SCHEMA_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_schema(schema: str) -> str:
    schema = schema.strip()
    if not schema:
        raise ValueError("DB_SCHEMA cannot be empty")
    if not _SCHEMA_RE.match(schema):
        raise ValueError(
            f"Invalid DB schema name: {schema!r}. Must match {_SCHEMA_RE.pattern}"
        )
    return schema


def create_engine_with_schema(*, db_url: str, schema: str | None) -> Engine:
    """Create an engine and (for Postgres) force search_path to the given schema."""

    engine = create_engine(db_url)

    if engine.dialect.name != "postgresql":
        return engine

    schema_name = _validate_schema(schema or "fhir_tablesaw")

    # Ensure schema exists.
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))

    # Ensure every new connection uses this schema.
    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_connection, _connection_record) -> None:  # type: ignore[no-redef]
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f'SET search_path TO "{schema_name}"')
        finally:
            cursor.close()

    return engine
