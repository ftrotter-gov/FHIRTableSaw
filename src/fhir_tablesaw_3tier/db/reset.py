"""Database reset helpers.

Because this project intentionally does not use migrations yet, the recommended
workflow for schema changes is to wipe and rebuild.

This module provides a programmatic reset:
- drop all tables defined in SQLAlchemy metadata
- recreate them
"""

from __future__ import annotations

import os

from sqlalchemy import create_engine, text

from fhir_tablesaw_3tier.db.base import Base
from fhir_tablesaw_3tier.db.engine import _validate_schema
from fhir_tablesaw_3tier.env import load_dotenv, require_env
from fhir_tablesaw_3tier.db.engine import create_engine_with_schema

# Ensure all SQLAlchemy models are registered on Base.metadata
# before we run create_all().
from fhir_tablesaw_3tier.db import models as _models  # noqa: F401


def reset_db(*, db_url: str | None = None) -> None:
    load_dotenv()
    if db_url is None:
        db_url = require_env("DATABASE_URL")

    schema = _validate_schema(os.environ.get("DB_SCHEMA") or "fhir_tablesaw")

    # For Postgres we want a *true wipe* that also removes objects that are not
    # tracked by SQLAlchemy metadata (or were created by other tooling).
    # The simplest safe reset is to drop + recreate the configured schema.
    raw_engine = create_engine(db_url)
    if raw_engine.dialect.name == "postgresql":
        with raw_engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        raw_engine.dispose()

        engine = create_engine_with_schema(db_url=db_url, schema=schema)
        Base.metadata.create_all(engine)
        return

    # Non-postgres (sqlite tests): drop/create tracked tables.
    engine = create_engine_with_schema(db_url=db_url, schema=schema)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
