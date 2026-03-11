from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any


UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T")


def is_uuid(s: str) -> bool:
    return bool(UUID_RE.match(s))


def infer_pg_type(value: Any) -> str:
    """Infer a Postgres type from a JSON scalar value.

    This is heuristic. If conflicting types occur, callers should decide how to
    reconcile (usually `jsonb`).
    """

    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "numeric"
    if isinstance(value, str):
        # FHIR `id` may be a UUID, but we only mark it specially when the column
        # is explicitly requested (e.g., fhir_id/uuid columns).
        if DATE_RE.match(value):
            return "date"
        if DATETIME_RE.match(value):
            return "timestamp"
        return "text"
    # Any object/array should be `jsonb` if stored.
    return "jsonb"


def reconcile_pg_types(types: set[str]) -> str:
    types = {t for t in types if t != "null"}
    if not types:
        return "text"
    if len(types) == 1:
        return next(iter(types))
    # numeric + integer => numeric
    if types.issubset({"integer", "numeric"}):
        return "numeric"
    # date + timestamp => timestamp
    if types.issubset({"date", "timestamp"}):
        return "timestamp"
    return "jsonb"
