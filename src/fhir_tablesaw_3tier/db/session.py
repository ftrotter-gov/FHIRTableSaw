"""DB session helpers for 3-tier bridge.

We keep this minimal and sqlite-friendly for unit tests, while still targeting
Postgres in production.
"""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def create_engine_and_sessionmaker(
    db_url: str,
    *,
    echo: bool = False,
    future: bool = True,
):
    engine = create_engine(db_url, echo=echo, future=future)
    SessionLocal = sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        future=future,
    )
    return engine, SessionLocal


def create_session(engine: Engine) -> Session:
    return Session(engine, future=True)
