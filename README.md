# Log Anomaly Detector

A live log anomaly detection pipeline. A demo shop made of microservices writes
logs; a streaming pipeline parses them, groups them into requests, scores each
request with three detectors, and shows explained alerts on a dashboard.

> **Status:** being built phase by phase. See [ROADMAP.md](ROADMAP.md).
> Done: **Phase 1 (foundation)**.

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

Settings come from environment variables (`LOGAD_*`); defaults are in
[src/logad/common/config.py](src/logad/common/config.py) and an example is in
[.env.example](.env.example).

## Project layout

```
src/logad/common/   config + health checks shared by every process
tests/              pytest tests (integration tests need docker compose up)
docker-compose.yml  Redis (queue) + Postgres (database)
.github/workflows/  CI: lint, tests, compose check on every push
```
