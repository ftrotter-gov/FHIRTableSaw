"""Common persistence helpers.

We avoid foreign keys per instruction, but we still implement basic relationship
tables using integer ids.
"""

from __future__ import annotations

import hashlib
import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from fhir_tablesaw_3tier.db.models import AddressRow, TelecomRow
from fhir_tablesaw_3tier.db.upsert import execute_returning_scalar, is_postgres


def ensure_uuid(value: str | uuid.UUID) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    s = str(value)
    try:
        return uuid.UUID(s)
    except ValueError:
        # Some real-world/test FHIR servers use non-UUID logical ids (e.g., "HansSolo").
        # We still need a stable UUID for relational storage, so we derive a
        # deterministic UUID5.
        return uuid.uuid5(uuid.NAMESPACE_URL, s)


def address_hash(
    *,
    line1: str | None,
    line2: str | None,
    city: str | None,
    state: str | None,
    postal_code: str | None,
    country: str | None,
) -> str:
    """Return deterministic hash for address components.

    We use a stable, low-collision hash to allow fast dedupe/upsert using a
    single indexed column.
    """

    # Use a delimiter unlikely to appear; still safe as we hash the final bytes.
    parts = [line1, line2, city, state, postal_code, country]
    norm = "\x1f".join(["" if p is None else str(p).strip() for p in parts])
    return hashlib.blake2b(norm.encode("utf-8"), digest_size=16).hexdigest()


def get_or_create_telecom_id(session: Session, *, type: str, value: str) -> int:
    """Get or create TelecomRow and return its id.

    Note: Because we don't enforce a uniqueness constraint on (type,value), we
    explicitly select first and reuse.
    """

    if is_postgres(session):
        stmt = (
            pg_insert(TelecomRow)
            .values(type=type, value=value)
            .on_conflict_do_update(
                index_elements=[TelecomRow.type, TelecomRow.value],
                set_={"type": type, "value": value},
            )
            .returning(TelecomRow.id)
        )
        return int(execute_returning_scalar(session, stmt))

    # sqlite/unit tests fallback
    existing = session.execute(
        select(TelecomRow).where(TelecomRow.type == type, TelecomRow.value == value)
    ).scalar_one_or_none()
    if existing is not None:
        return int(existing.id)

    row = TelecomRow(type=type, value=value)
    session.add(row)
    session.flush()
    return int(row.id)


def get_or_create_address_id(
    session: Session,
    *,
    line1: str | None,
    line2: str | None,
    city: str | None,
    state: str | None,
    postal_code: str | None,
    country: str | None,
) -> int:
    """Get or create AddressRow via address_hash and return its id."""

    h = address_hash(
        line1=line1,
        line2=line2,
        city=city,
        state=state,
        postal_code=postal_code,
        country=country,
    )

    if is_postgres(session):
        stmt = (
            pg_insert(AddressRow)
            .values(
                address_hash=h,
                line1=line1,
                line2=line2,
                city=city,
                state=state,
                postal_code=postal_code,
                country=country,
            )
            .on_conflict_do_update(
                index_elements=[AddressRow.address_hash],
                set_={
                    "line1": line1,
                    "line2": line2,
                    "city": city,
                    "state": state,
                    "postal_code": postal_code,
                    "country": country,
                },
            )
            .returning(AddressRow.id)
        )
        return int(execute_returning_scalar(session, stmt))

    # sqlite fallback
    existing = session.execute(select(AddressRow).where(AddressRow.address_hash == h)).scalar_one_or_none()
    if existing is not None:
        return int(existing.id)

    row = AddressRow(
        address_hash=h,
        line1=line1,
        line2=line2,
        city=city,
        state=state,
        postal_code=postal_code,
        country=country,
    )
    session.add(row)
    session.flush()
    return int(row.id)
