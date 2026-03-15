"""Common persistence helpers.

We avoid foreign keys per instruction, but we still implement basic relationship
tables using integer ids.
"""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from fhir_tablesaw_3tier.db.models import TelecomRow


def ensure_uuid(value: str | uuid.UUID) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))


def get_or_create_telecom_id(session: Session, *, type: str, value: str) -> int:
    """Get or create TelecomRow and return its id.

    Note: Because we don't enforce a uniqueness constraint on (type,value), we
    explicitly select first and reuse.
    """

    existing = session.execute(
        select(TelecomRow).where(TelecomRow.type == type, TelecomRow.value == value)
    ).scalar_one_or_none()
    if existing is not None:
        return int(existing.id)

    row = TelecomRow(type=type, value=value)
    session.add(row)
    session.flush()
    return int(row.id)
