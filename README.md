# Log Anomaly Detector

A live log anomaly detection pipeline. A demo shop made of microservices writes
logs; a streaming pipeline parses them, groups them into requests, scores each
request with three detectors, and shows explained alerts on a dashboard.

> **Status:** being built phase by phase. See [ROADMAP.md](ROADMAP.md).
> Done: **Phase 1 (foundation)**, **Phase 2 (demo shop + traffic + faults)**,
> **Phase 3 (live pipeline)**, **Phase 4 (correct under failure)**.

## Quick start

Needs Python 3.11+ and Docker.

```bash
python -m venv .venv
.venv\Scripts\activate             # Windows  (Linux/macOS: source .venv/bin/activate)
pip install -e ".[dev]"

docker compose up -d --wait        # Redis + Postgres (+ migrations) + the shop
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

## The pipeline

```
log files ─► shipper ─► ingest API ─► [raw-logs] ─► parser ×2 ─► [events ×4 shards] ─► session builder ─► Postgres
```

| Step | What it does |
|---|---|
| **shipper** | Tails `runtime/logs/*.log`, sends new complete lines in batches. Saves its position only after ingest accepts a batch, so a crash re-sends instead of losing lines. Handles log rotation. |
| **ingest** | `POST /ingest` appends each line to the `raw-logs` Redis stream. |
| **parser** | Splits each line (time, level, service, request ID, message, `key=value` params) and finds its template, e.g. `payment INFO Fraud check passed order=<*> score=<*>`. A shared registry in Redis gives each template one global ID on every worker. Events go to shard `crc32(request_id) % 4`; adding events and acking raw lines happen in one transaction. |
| **session builder** | Groups the events of each request into one session. A session closes 2 s after the shop's `Completed ...` line (time for late lines from other services), or after 30 s of silence. Events are acked only after the session is saved. |

Look at the result:

```bash
docker compose exec postgres psql -U logad -d logad -c \
  "select session_id, closed_by, event_count, services from sessions order by started_at desc limit 5"
```

In a live check, 3,757 requests written by the shop became exactly 3,757
sessions, each with exactly the right number of events.

## Correct under failure

Any worker can crash at any moment; every line still ends up in exactly one
session, exactly once.

| Problem | What handles it |
|---|---|
| A worker crashes mid-batch | Entries are acked only after their work is done. A restarted worker first finishes its own unfinished entries; entries left by a dead or replaced worker are claimed by another one after 60 s (`XAUTOCLAIM`). |
| A line is delivered twice (re-sent, re-processed) | Every line has a permanent ID (file + byte offset) from the shipper; the session builder keeps each line once. Saving a session is insert-or-ignore. |
| Parser output half-written | Adding events and acking the raw line happen in one `MULTI/EXEC` transaction. |
| Bad or "poison" lines | Unparsable lines, and lines handed out more than 5 times, go to the `dead-letter` stream with the reason. |
| Logs arrive faster than they can be processed | Ingest answers `429 Too Many Requests` above a backlog limit; the shipper backs off, lines wait in the files. Processed entries are trimmed from Redis. |
| Pipeline stalls, or a slow worker is overtaken | Sessions follow the logs' own timestamps and only close up to the oldest line not yet fully processed (a watermark), so nothing closes early or times out by mistake. |
| `docker stop` | Workers finish the current batch and exit; unfinished work stays pending for the next start. |

**Chaos test** ([eval/chaos.py](eval/chaos.py), also run in CI): kills random
pipeline workers with SIGKILL every few seconds under traffic, sometimes
replacing a container entirely, then compares every request in the shop's logs
with the sessions in Postgres. Latest result: [eval/results/chaos.md](eval/results/chaos.md):
13 kills in 2 minutes, 2,555 requests → 2,555 sessions, 0 missing, 0 extra,
0 wrong event counts, 0 duplicated lines.

```bash
python eval/chaos.py --fresh      # ~4 minutes
```

## Configuration

Settings come from environment variables (`LOGAD_*`); defaults are in
[src/logad/common/config.py](src/logad/common/config.py) and an example is in
[.env.example](.env.example).

## Project layout

```
src/logad/common/   config, database tables, Redis key names, health checks
src/logad/shipper/  tails the shop's log files, sends new lines to ingest (at least once)
src/logad/ingest/   ingest API: POST /ingest -> Redis stream "raw-logs"
src/logad/parser/   parser workers: raw lines -> events with shared template IDs, 4 shards
src/logad/sessions/ session builder: events of one request -> one row in Postgres
eval/               chaos test (and later: evaluations, benchmarks) + their results
migrations/         database migrations (Alembic); `alembic upgrade head` applies them
src/shop/           demo shop: services, traffic generator, fault injector
tests/              pytest tests (integration tests need docker compose up)
docker-compose.yml  Redis + Postgres + the shop + the pipeline
.github/workflows/  CI: lint, tests, compose check on every push
```
