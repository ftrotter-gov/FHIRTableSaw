"""Database reset helpers.

Because this project intentionally does not use migrations yet, the recommended
workflow for schema changes is to wipe and rebuild.

This module provides a programmatic reset:
- drop all tables defined in SQLAlchemy metadata
- recreate them
"""

from __future__ import annotations

import os

from fhir_tablesaw_3tier.db.base import Base
from fhir_tablesaw_3tier.env import load_dotenv, require_env
from fhir_tablesaw_3tier.db.engine import create_engine_with_schema


def reset_db(*, db_url: str | None = None) -> None:
    load_dotenv()
    if db_url is None:
        db_url = require_env("DATABASE_URL")
    schema = os.environ.get("DB_SCHEMA") or "fhir_tablesaw"
    engine = create_engine_with_schema(db_url=db_url, schema=schema)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
