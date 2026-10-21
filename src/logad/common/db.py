"""
db.py — Postgres tables and the async engine.

Tables are created by Alembic migrations (migrations/), never by the app.
"""

from datetime import datetime

from sqlalchemy import ARRAY, Boolean, DateTime, Float, Index, Integer, Text, func, text
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


class RateAnomalyRow(Base):
    """An event type that happened far more (spike) or far less (drop) often than
    usual in one time window. One row per (window, event type)."""

    __tablename__ = "rate_anomalies"

    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    template_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    count: Mapped[int] = mapped_column(Integer)
    expected: Mapped[float] = mapped_column(Float)
    z: Mapped[float] = mapped_column(Float)
    direction: Mapped[str] = mapped_column(Text)  # "spike" or "drop"
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Alert(Base):
    """One incident on one service: many flagged requests and rate anomalies
    grouped together. At most one open alert per service (a partial unique
    index), so duplicates are impossible even if the manager restarts."""

    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    service: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)  # "open" or "resolved"
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # first evidence
    raised_at: Mapped[datetime] = mapped_column(  # when the alert was actually raised
        DateTime(timezone=True), server_default=func.now()
    )
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    sessions: Mapped[int] = mapped_column(Integer)  # flagged requests in the incident
    detectors: Mapped[list[str]] = mapped_column(ARRAY(Text))
    summary: Mapped[str] = mapped_column(Text)
    examples: Mapped[list[dict]] = mapped_column(JSONB)  # a few flagged requests and why

    __table_args__ = (
        Index(
            "uq_alerts_one_open_per_service",
            "service",
            unique=True,
            postgresql_where=text("status = 'open'"),
        ),
        Index("ix_alerts_opened_at", "opened_at"),
    )


def make_engine(url: str) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True)
