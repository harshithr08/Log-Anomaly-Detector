"""
db.py — Postgres tables and the async engine.

Tables are created by Alembic migrations (migrations/), never by the app.
"""

from datetime import datetime

from sqlalchemy import ARRAY, DateTime, Index, Integer, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Template(Base):
    """One event type, e.g. 'payment INFO Fraud check passed order=<*> score=<*>'.
    The ID is assigned by the shared pattern registry, so every parser agrees."""

    __tablename__ = "templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    template: Mapped[str] = mapped_column(Text, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Session(Base):
    """All the log events of one request, across every service, in time order."""

    __tablename__ = "sessions"

    session_id: Mapped[str] = mapped_column(Text, primary_key=True)  # the request ID
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    closed_by: Mapped[str] = mapped_column(Text)  # "end" (saw the last line) or "timeout"
    event_count: Mapped[int] = mapped_column(Integer)
    services: Mapped[list[str]] = mapped_column(ARRAY(Text))
    # [{"ts", "service", "level", "template_id", "message", "params"}, ...]
    events: Mapped[list[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (Index("ix_sessions_started_at", "started_at"),)


def make_engine(url: str) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True)
