"""
app.py — the query API, the live WebSocket and (from static/) the dashboard.

    GET    /api/health                Redis and Postgres reachable (liveness)
    GET    /api/ready                 ... and a model is scoring right now (readiness)
    GET    /api/stats                 live numbers (sampled every second)
    GET    /api/alerts                ?status=open|resolved|all&limit=&before_id=
    GET    /api/alerts/{id}           one alert with its example requests in full
    GET    /api/sessions/{id}         one request: its events and its scores
    GET    /api/rate-anomalies        the latest rate anomalies
    GET    /api/faults                the shop's active faults
    POST   /api/faults/{service}      break a shop service  {"kind", "probability", "delay_ms"}
    DELETE /api/faults/{service}      fix it
                                      (both need the X-API-Key header if LOGAD_API_KEY is set)
    WS     /ws                        every second: stats + open alerts
    GET    /                          the dashboard (static/index.html)

Run: uvicorn logad.api.app:create_app --factory --port 8200
"""

import asyncio
import contextlib
import logging
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from sqlalchemy import select, text

from logad.api.stats import StatsSampler
from logad.common.config import Settings, get_settings
from logad.common.db import Alert, make_engine
from logad.common.streams import Keys
from shop.settings import SERVICES, ShopSettings

log = logging.getLogger("logad.api")
PUSH_EVERY_S = 1.0
STATIC_DIR = Path(__file__).parent / "static"


class FaultRequest(BaseModel):
    kind: Literal["slow", "error", "skip", "crash"]
    probability: float = Field(1.0, ge=0.0, le=1.0)
    delay_ms: int = Field(1500, ge=0, le=30_000)


def _alert_json(row) -> dict:
    return {
        "id": row.id,
        "service": row.service,
        "status": row.status,
        "opened_at": row.opened_at.isoformat(),
        "raised_at": row.raised_at.isoformat(),
        "last_seen_at": row.last_seen_at.isoformat(),
        "resolved_at": row.resolved_at.isoformat() if row.resolved_at else None,
        "sessions": row.sessions,
        "detectors": row.detectors,
        "summary": row.summary,
    }


def create_app(
    settings: Settings | None = None,
    shop_settings: ShopSettings | None = None,
    shop_transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    shop_settings = shop_settings or ShopSettings()
    keys = Keys(settings.stream_prefix)

    async def push_forever(app: FastAPI):
        """Sample stats once a second and push them to every open dashboard,
        until app.state.stop is set. It is stopped by that flag, never cancelled:
        cancelling it in the middle of a database query could hang the
        database driver's cleanup at shutdown."""
        while not app.state.stop.is_set():
            try:
                stats = await app.state.sampler.sample()
                message = {"stats": stats, "open_alerts": await open_alerts(app)}
                for socket in list(app.state.sockets):
                    try:
                        await socket.send_json(message)
                    except Exception:  # closed meanwhile
                        app.state.sockets.discard(socket)
            except Exception as error:  # e.g. Postgres briefly down: try again
                log.warning("Stats sample failed: %r", error)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(app.state.stop.wait(), PUSH_EVERY_S)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
        app.state.engine = make_engine(settings.database_url)
        app.state.shop = httpx.AsyncClient(transport=shop_transport, timeout=5)
        app.state.sampler = StatsSampler(
            app.state.redis, app.state.engine, keys, settings.shard_count
        )
        app.state.sockets = set()
        app.state.stop = asyncio.Event()
        pusher = asyncio.create_task(push_forever(app))
        yield
        app.state.stop.set()  # finishes its current round, then returns
        await pusher
        await app.state.shop.aclose()
        await app.state.redis.aclose()
        await app.state.engine.dispose()

    app = FastAPI(title="logad", lifespan=lifespan)

    async def open_alerts(app: FastAPI) -> list[dict]:
        async with app.state.engine.connect() as conn:
            rows = await conn.execute(
                select(Alert.__table__).where(Alert.status == "open").order_by(Alert.id.desc())
            )
            return [_alert_json(row) for row in rows]

    @app.get("/api/health")
    async def health():
        await app.state.redis.ping()
        async with app.state.engine.connect() as conn:
            await conn.execute(text("select 1"))
        return {"status": "ok"}

    @app.get("/api/ready")
    async def ready():
        """Ready = Redis and Postgres reachable AND requests are being scored."""
        checks = {}
        try:
            await app.state.redis.ping()
            checks["redis"] = "ok"
        except Exception as error:
            checks["redis"] = repr(error)
        try:
            async with app.state.engine.connect() as conn:
                version = (
                    await conn.execute(
                        text(
                            "select model_version from scores"
                            " where scored_at > now() - interval '2 minutes'"
                            " order by scored_at desc limit 1"
                        )
                    )
                ).scalar()
            checks["postgres"] = "ok"
            checks["scoring"] = f"model {version}" if version else "no requests scored in 2 min"
        except Exception as error:
            checks["postgres"] = repr(error)
        ok = checks.get("redis") == "ok" and checks.get("postgres") == "ok"
        ok = ok and checks.get("scoring", "").startswith("model")
        if not ok:
            raise HTTPException(503, checks)
        return {"status": "ready", **checks}

    async def require_key(x_api_key: str | None = Header(None)):
        """Admin actions need the key when one is configured (always, once public)."""
        if settings.api_key and not secrets.compare_digest(x_api_key or "", settings.api_key):
            raise HTTPException(401, "missing or wrong X-API-Key")

    @app.get("/api/stats")
    async def stats():
        return app.state.sampler.latest or await app.state.sampler.sample()

    @app.get("/api/alerts")
    async def alerts(
        status: Literal["open", "resolved", "all"] = "all",
        limit: int = 20,
        before_id: int | None = None,
    ):
        query = select(Alert.__table__).order_by(Alert.id.desc()).limit(min(limit, 100))
        if status != "all":
            query = query.where(Alert.status == status)
        if before_id is not None:
            query = query.where(Alert.id < before_id)
        async with app.state.engine.connect() as conn:
            items = [_alert_json(row) for row in await conn.execute(query)]
        next_before = items[-1]["id"] if len(items) == min(limit, 100) else None
        return {"items": items, "next_before_id": next_before}

    async def _session(conn, session_id: str) -> dict | None:
        row = (
            await conn.execute(
                text("select * from sessions where session_id = :id"), {"id": session_id}
            )
        ).first()
        if row is None:
            return None
        templates = dict(
            (
                await conn.execute(
                    text("select id, template from templates where id = any(:ids)"),
                    {"ids": list({e["template_id"] for e in row.events})},
                )
            ).all()
        )
        scores = await conn.execute(
            text(
                "select model_version, lstm_score, timing_score, anomalous, detectors, explanation"
                " from scores where session_id = :id"
            ),
            {"id": session_id},
        )
        return {
            "session_id": row.session_id,
            "started_at": row.started_at.isoformat(),
            "ended_at": row.ended_at.isoformat(),
            "closed_by": row.closed_by,
            "services": row.services,
            "events": [
                {
                    **{k: e[k] for k in ("ts", "service", "level", "message")},
                    "template": templates.get(e["template_id"]),
                }
                for e in row.events
            ],
            "scores": [dict(score._mapping) for score in scores],
        }

    @app.get("/api/sessions/{session_id}")
    async def session(session_id: str):
        async with app.state.engine.connect() as conn:
            found = await _session(conn, session_id)
        if found is None:
            raise HTTPException(404, "no such session")
        return found

    @app.get("/api/alerts/{alert_id}")
    async def alert(alert_id: int):
        async with app.state.engine.connect() as conn:
            row = (await conn.execute(select(Alert.__table__).where(Alert.id == alert_id))).first()
            if row is None:
                raise HTTPException(404, "no such alert")
            examples = []
            for example in row.examples:
                examples.append({**example, "session": await _session(conn, example["session_id"])})
        return {**_alert_json(row), "examples": examples}

    @app.get("/api/rate-anomalies")
    async def rate_anomalies(limit: int = 20):
        async with app.state.engine.connect() as conn:
            rows = await conn.execute(
                text(
                    "select r.window_start, r.count, r.expected, r.z, r.direction, t.template "
                    "from rate_anomalies r join templates t on t.id = r.template_id "
                    "order by r.window_start desc, r.z desc limit :limit"
                ),
                {"limit": min(limit, 100)},
            )
            return [{**dict(r._mapping), "window_start": r.window_start.isoformat()} for r in rows]

    def _fault_url(service: str) -> str:
        if service not in SERVICES:
            raise HTTPException(404, f"unknown service; one of {', '.join(SERVICES)}")
        return shop_settings.url_for(service) + "/admin/fault"

    @app.get("/api/faults")
    async def faults():
        result = {}
        for service in SERVICES:
            try:
                response = await app.state.shop.get(_fault_url(service))
                result[service] = response.json()["fault"]
            except httpx.HTTPError:
                result[service] = "unreachable"
        return result

    @app.post("/api/faults/{service}", dependencies=[Depends(require_key)])
    async def break_service(service: str, fault: FaultRequest):
        try:
            response = await app.state.shop.post(_fault_url(service), json=fault.model_dump())
        except httpx.HTTPError as error:
            raise HTTPException(502, f"{service} unreachable: {type(error).__name__}") from error
        return response.json()

    @app.delete("/api/faults/{service}", dependencies=[Depends(require_key)])
    async def fix_service(service: str):
        try:
            response = await app.state.shop.delete(_fault_url(service))
        except httpx.HTTPError as error:
            raise HTTPException(502, f"{service} unreachable: {type(error).__name__}") from error
        return response.json()

    @app.websocket("/ws")
    async def websocket(socket: WebSocket):
        await socket.accept()
        app.state.sockets.add(socket)
        try:
            while True:
                await socket.receive_text()  # we only push; this notices disconnects
        except WebSocketDisconnect:
            app.state.sockets.discard(socket)

    # last, so the /api and /ws routes above take precedence
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="dashboard")
    return app
