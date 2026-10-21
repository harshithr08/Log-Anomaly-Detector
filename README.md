# Log Anomaly Detector

A live log anomaly detection pipeline. A demo shop made of microservices writes
logs; a streaming pipeline parses them, groups them into requests, scores each
request with three detectors, and shows explained alerts on a dashboard.

> **Status:** being built phase by phase. See [ROADMAP.md](ROADMAP.md).
> Done: **Phase 1 (foundation)**, **Phase 2 (demo shop + traffic + faults)**,
> **Phase 3 (live pipeline)**, **Phase 4 (correct under failure)**, **Phase 5 (models)**,
> **Phase 6 (alerts + dashboard)**.

**Dashboard:** `docker compose up -d --build`, then open <http://localhost:8200>.
Break a service with the buttons on the right and watch an explained alert appear.

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

## Detecting anomalies

Three detectors, because the shop's faults look completely different in the logs:

| Detector | Looks at | Catches | How it scores |
|---|---|---|---|
| **LSTM** (next-event, DeepLog-style) | the order of events in a request | skipped / wrong / extra steps | surprise at the most surprising step ("after X expected Y, got Z") |
| **Timing** | each part's time vs requests that took exactly the same path | slow requests | distance from normal, in robust spreads ("payment is 14 spreads slower") |
| **Rate** | count of each event type per 10 s vs its usual level | error floods, traffic drops | spike / drop z-score; a brand-new error type stands out from 0 |

All three learn from **normal traffic only**. Every session is scored live by
the scoring workers (LSTM through ONNX Runtime, no PyTorch in the image) and
saved in `scores` with which detector fired and why; rate anomalies go to
`rate_anomalies`.

### Results

[eval/results/models.md](eval/results/models.md), from a fresh collection
(12 min clean traffic + 10 scheduled faults on every service), evaluated once.
Thresholds come from clean validation data only: at most 1 in 1,000 normal
requests flagged.

| Detector | Precision | Recall | False alarms | Skip | Slow | Error |
|---|---|---|---|---|---|---|
| Best simple rule (unseen sequence) | 100% | 19.4% | 0.0% | 100% | 0% | 100% |
| **LSTM + timing** | **99.6%** | **96.1%** | **0.1%** | 100% | 95.2% | 100% |

- Skipped steps and errors are easy: even a Markov chain gets them all here
  (the shop has only ~22 distinct normal paths). The LSTM ties with it on this
  shop; it is kept because it looks 10 steps back instead of 1, and it won
  clearly on the HDFS benchmark.
- Slow requests are the hard part (82% of the broken ones) and no simple rule
  catches them. Misses: a +400 ms shop slowdown on checkouts, the slowest and
  most variable request.

### Design decisions (made on a separate development collection)

| Decision | Why |
|---|---|
| Compare times with the **same exact path**, not the same request type | Request types mix cache hits with misses and declined with completed checkouts; slow recall 9% → 97% |
| Count only **slower** than normal | Faster or shorter isn't this detector's job (missing steps are the LSTM's) |
| Times as log(1 + ms/10) | A few ms of jitter on a 3 ms request is not an incident |
| **Distance** instead of **Isolation Forest** for timing | Caught more (98.1% vs 96.9%), and the forest's score saturates: 50× slower scored barely higher |
| Event order by file position within the same millisecond | Parallel parsers could deliver same-millisecond lines swapped |
| Threshold a hair above the cut-off | Many normal sessions tie there (declined cards); ONNX vs PyTorch rounding flipped some |
| Rate baseline frozen during an incident, accepted after 3 min | Stays flagged while it lasts, but a lasting change doesn't alert forever |

### Train your own

```bash
pip install -e ".[train]" --extra-index-url https://download.pytorch.org/whl/cpu
python eval/collect.py --fresh            # ~22 min: clean traffic, then faults
python -m logad.models.dataset export     # sessions + labels -> data/shop/
python -m logad.models.train              # trains, grades, saves models/<version>/
```

The scoring workers pick up a new `models/latest` within 30 seconds.

## Alerts and dashboard

Thousands of scored requests become a few alerts a person can act on:

- **One alert per service per incident.** Each detector names a service: the
  LSTM's unexpected step, the timing detector's slowest service, the rate
  detector's event type.
- **Opens** at 5 flagged requests of a service within 60 s, or at any rate
  anomaly. A single flag is never an alert: 1 in 1,000 normal requests is flagged.
- **Resolves** once evidence has stayed below that level for 60 s (not "no
  evidence at all": normal traffic always has a trickle of false alarms).
- **Explains itself**: the most common reason per detector plus example
  requests, e.g. *"inventory: 24 requests with an unexpected step: after
  'Handling POST /reservations' expected 'Checking stock levels', got 'Reserved
  stock'"*.
- **Restart-safe**: the alert manager rebuilds its picture from Postgres every
  2 s; a partial unique index allows only one open alert per service.

The **query API** (`:8200/api/...`) serves stats, paginated alerts, alert
details with full example requests, sessions, rate anomalies and the Break/Fix
buttons; **`/ws`** pushes stats and open alerts every second to the
**dashboard** (one HTML page, Chart.js): live throughput, flagged requests,
backlogs, open and resolved alerts, and an alert detail view with the
surprising line highlighted. Light and dark themes; works on a phone.

Measured live: a payment skip fault was raised as an alert 3-14 s after the
fault started (it needs 5 flagged requests), and resolved by itself a minute
after the fault was fixed.

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
src/logad/models/   dataset, baselines, LSTM, timing, training, ONNX serving
src/logad/scoring/  scoring workers: every saved session -> scores
src/logad/rates/    rate detector: event counts per 10 s -> rate_anomalies
src/logad/alerts/   alert manager: evidence -> one explained alert per service per incident
src/logad/api/      query API, WebSocket, dashboard (static/index.html)
eval/               chaos test, dataset collection, results
migrations/         database migrations (Alembic); `alembic upgrade head` applies them
src/shop/           demo shop: services, traffic generator, fault injector
tests/              pytest tests (integration tests need docker compose up)
docker-compose.yml  Redis + Postgres + the shop + the pipeline
.github/workflows/  CI: lint, tests, compose check on every push
```
