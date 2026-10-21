"""
service.py — the plumbing every shop service shares.

A Service wraps a FastAPI app and adds:
    - a request ID per request (taken from the X-Request-ID header, or new)
    - "Handling ..." / "Completed ... status= duration_ms=" log lines
    - the generic faults: 'slow' (delay) and 'error' (500 response)
    - /health and /admin/fault endpoints
    - call(): talk to another service, passing the request ID along

Each service module only adds its own routes and its own 'skip' fault.
"""

import asyncio
import logging
import random
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from shop.faults import FaultInjector, GroundTruth, admin_router
from shop.logs import close_logging, request_id_var, setup_logging
from shop.settings import ShopSettings

REQUEST_ID_HEADER = "x-request-id"
UNLOGGED_PATHS = ("/health", "/admin")


class DownstreamUnavailable(Exception):
    """Another service could not be reached (down, crashed, or timed out)."""

    def __init__(self, service: str):
        super().__init__(service)
        self.service = service


class Service:
    def __init__(
        self,
        name: str,
        settings: ShopSettings | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        error_messages: tuple[str, ...] = ("Internal error",),
        rng: random.Random | None = None,
    ):
        self.name = name
        self.settings = settings or ShopSettings()
        self.rng = rng or random.Random()
        self.log = setup_logging(name, self.settings.log_dir)
        self.faults = FaultInjector(
            name, GroundTruth(self.settings.ground_truth_dir / f"{name}.jsonl"), self.rng
        )
        self.error_messages = error_messages
        self._transport = transport
        self._http: httpx.AsyncClient | None = None
        self._background_jobs = []

        self.app = FastAPI(title=f"shop-{name}", lifespan=self._lifespan)
        self.app.middleware("http")(self._handle_request)
        self.app.add_exception_handler(DownstreamUnavailable, self._downstream_unavailable)
        self.app.add_api_route("/health", self._health, methods=["GET"])
        self.app.include_router(admin_router(self.faults))

    # ── lifecycle ────────────────────────────────────────────────────────────

    @asynccontextmanager
    async def _lifespan(self, app):
        self.faults.close_open_faults()
        self.log.info("Service started")
        tasks = [asyncio.create_task(job()) for job in self._background_jobs]
        yield
        self.log.info("Service stopping")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._http:
            await self._http.aclose()
        close_logging(self.log)

    def _health(self):
        return {"status": "ok", "service": self.name}

    # ── every request ────────────────────────────────────────────────────────

    async def _handle_request(self, request: Request, call_next):
        if request.url.path.startswith(UNLOGGED_PATHS):
            return await call_next(request)

        request_id = request.headers.get(REQUEST_ID_HEADER) or uuid.uuid4().hex[:16]
        token = request_id_var.set(request_id)
        started = time.perf_counter()
        try:
            self.log.info("Handling %s %s", request.method, request.url.path)
            await self.faults.maybe_delay()
            if self.faults.hits("error"):
                self.log.error(self.rng.choice(self.error_messages))
                response = JSONResponse({"detail": "internal error"}, status_code=500)
            else:
                try:
                    response = await call_next(request)
                except Exception as error:
                    self.log.error("Unhandled error: %s", type(error).__name__)
                    response = JSONResponse({"detail": "internal error"}, status_code=500)

            status = response.status_code
            level = (
                logging.ERROR
                if status >= 500
                else logging.WARNING
                if status >= 400
                else logging.INFO
            )
            duration_ms = (time.perf_counter() - started) * 1000
            self.log.log(
                level,
                "Completed %s %s status=%d duration_ms=%d",
                request.method,
                request.url.path,
                status,
                duration_ms,
            )
            response.headers[REQUEST_ID_HEADER] = request_id
            return response
        finally:
            request_id_var.reset(token)

    async def _downstream_unavailable(self, request: Request, error: DownstreamUnavailable):
        return JSONResponse({"detail": f"{error.service} unavailable"}, status_code=503)

    # ── helpers for service code ─────────────────────────────────────────────

    def background(self, job):
        """Register an async function to run for the whole life of the service."""
        self._background_jobs.append(job)
        return job

    @property
    def http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                transport=self._transport, timeout=self.settings.downstream_timeout_s
            )
        return self._http

    async def call(self, service: str, method: str, path: str, **kwargs) -> httpx.Response:
        """Call another shop service, passing the current request ID along."""
        url = self.settings.url_for(service) + path
        headers = {REQUEST_ID_HEADER: request_id_var.get()}
        try:
            return await self.http.request(method, url, headers=headers, **kwargs)
        except httpx.TransportError as error:
            self.log.error("Call to %s failed: %s", service, type(error).__name__)
            raise DownstreamUnavailable(service) from error

    async def work(self, low_ms: float, high_ms: float) -> None:
        """Simulate the normal time a step takes (a DB query, an API call, ...)."""
        await asyncio.sleep(self.rng.uniform(low_ms, high_ms) / 1000)
