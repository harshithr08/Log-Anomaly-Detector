# Log Anomaly Detector

A live log anomaly detection pipeline. A demo shop made of microservices writes
logs; a streaming pipeline parses them, groups them into requests, scores each
request with three detectors, and shows explained alerts on a dashboard.

> **Status:** being built phase by phase. See [ROADMAP.md](ROADMAP.md).
> Done: **Phase 1 (foundation)**, **Phase 2 (demo shop + traffic + faults)**.

## Quick start

Needs Python 3.11+ and Docker.

```bash
python -m venv .venv
.venv\Scripts\activate             # Windows  (Linux/macOS: source .venv/bin/activate)
pip install -e ".[dev]"

docker compose up -d --wait        # Redis + Postgres
python -m logad.common.health      # should print "ok" twice
pytest                             # all tests
```

## The demo shop

`docker compose up -d --build` also starts a small shop made of four services
and a traffic generator (~10 requests/s, change with `TRAFFIC_RATE`):

```
customer ──► shop (gateway, :8000) ──► inventory (:8001)
                                   ──► payment   (:8002)
                                   ──► shipping  (:8003)
```

Every request gets an ID that is passed between services and printed on every
log line. Logs are written to `runtime/logs/<service>.log`:

```
2026-10-21T02:50:38.384Z INFO payment [req=90d78a6ef11a42ed] Completed POST /charges status=200 duration_ms=62
```

Break and fix services on purpose:

```bash
python -m shop.inject break payment slow --delay-ms 1500   # slow | error | skip | crash
python -m shop.inject status
python -m shop.inject fix all
```

| Fault | Effect |
|---|---|
| `slow` | adds delay to the service's requests |
| `error` | the service fails requests with a 500 |
| `skip` | the service silently skips a step (still returns 200) |
| `crash` | the process dies; Docker restarts it |

Every broken request ID and every fault's start/end is recorded in
`runtime/ground_truth/<service>.jsonl`. This is the answer key for evaluating
the detectors later; the pipeline never reads it.

## Configuration

Settings come from environment variables (`LOGAD_*`); defaults are in
[src/logad/common/config.py](src/logad/common/config.py) and an example is in
[.env.example](.env.example).

## Project layout

```
src/logad/common/   config + health checks shared by every process
src/shop/           demo shop: services, traffic generator, fault injector
tests/              pytest tests (integration tests need docker compose up)
docker-compose.yml  Redis + Postgres + the shop
.github/workflows/  CI: lint, tests, compose check on every push
```
