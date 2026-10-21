"""
db.py — Postgres tables and the async engine.

Tables are created by Alembic migrations (migrations/), never by the app.
"""

from datetime import datetime

from sqlalchemy import ARRAY, Boolean, DateTime, Float, Index, Integer, Text, func
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


class Score(Base):
    """What the models thought of one session. One row per (session, model
    version), so scoring the same session again is harmless."""

    __tablename__ = "scores"

    session_id: Mapped[str] = mapped_column(Text, primary_key=True)
    model_version: Mapped[str] = mapped_column(Text, primary_key=True)
    lstm_score: Mapped[float] = mapped_column(Float)
    timing_score: Mapped[float] = mapped_column(Float)
    anomalous: Mapped[bool] = mapped_column(Boolean)
    detectors: Mapped[list[str]] = mapped_column(ARRAY(Text))  # which ones fired
    explanation: Mapped[dict] = mapped_column(JSONB)  # why, per detector that fired
    scored_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (Index("ix_scores_scored_at", "scored_at"),)


def make_engine(url: str) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True)
