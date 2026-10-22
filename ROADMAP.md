# Roadmap: Live Log Anomaly Detector

## Goal

A small online shop made of microservices writes logs **live**. A streaming
pipeline parses them, groups them into requests, and scores every request with
three detectors. Problems show up as **explained alerts** on a dashboard within
seconds.

Three things the project proves, each with a number from a re-runnable script:

1. **It catches real problems.** We break the shop on purpose and record exactly
   which requests were affected. That ground truth gives: faults caught, time to
   detect, false alarms per hour.
2. **It stays correct when parts crash.** Kill any worker mid-stream: 0 lost, 0 duplicated.
3. **It is fast.** Throughput, p99 latency, and how it scales with more workers.

### The final demo (2 minutes)

1. `docker compose up` → open the dashboard. Traffic is flowing through the shop.
2. Click **"Break payment: slow"** → an alert appears: *payment service, latency
   6× normal, caught by the timing detector, 37 affected requests*.
3. Click **"Break inventory: skip step"** → an alert: *unexpected event order,
   caught by the LSTM: expected `stock reserved`, got `payment started`*.
4. Kill a worker → the pipeline keeps going; counters show **0 lost, 0 duplicated**.
5. Click **"Fix"** → the alerts resolve on their own.

---

## Progress

- [x] Phase 1: Foundation
- [x] Phase 2: Demo shop + traffic + fault injector
- [x] Phase 3: Live pipeline
- [x] Phase 4: Correct under failure
- [x] Phase 5: Models
- [x] Phase 6: Alerts + dashboard
- [x] Phase 7: Measure & optimize
- [x] Phase 8: Fault evaluation + polish
- [ ] Phase 9: Public deployment
- [ ] Bonus: HDFS benchmark

---

## Architecture

```
 TRAFFIC GENERATOR ──► DEMO SHOP (FastAPI services)       FAULT INJECTOR
                       shop → inventory → payment → shipping   (break / fix; records
                       plain-text logs with a request_id        affected request IDs)
                                   │
                              log files
                                   ▼
                       Shipper (tails files, sends batches)
                                   ▼
                       Ingest API ── 429 when the queue is full
                                   ▼
                       [Redis Stream: raw-logs]
                                   ▼
                       Parser workers ×N (Drain3 + shared pattern registry)
                                   │ bad lines → [dead-letter stream]
                                   ▼
                       [Redis Streams: events, sharded by request_id]
                                   ▼
                       Session builder (one session = one request)
                                   ▼
                       [Redis Stream: sessions]
                                   ▼
                       Scoring workers ×N: LSTM + timing
                       Rate detector: event counts per 10s
                                   ▼
                       Alert manager (group, explain, auto-resolve)
                                   ▼
                       PostgreSQL (sessions, scores, alerts, faults)
                                   ▼
                       Query API + WebSocket ──► Dashboard
```

Every box is its own process in Docker Compose, so each part scales and fails on its own.

---

## Key decisions

| Decision | Choice | Why |
|---|---|---|
| Log source | **Demo shop we build** | Live logs, clear sessions, faults on demand, exact ground truth. |
| Log format | **Plain-text lines** | Like real apps. Template parsing is a real step. |
| Getting logs out | **Services write files; a shipper tails them** | Like Fluent Bit / Vector: apps don't know the pipeline exists. |
| Session | **One request** (`request_id` passed between services) | Short, with a clear start and end. |
| Queue | **Redis Streams** | Kafka's ideas (consumer groups, acks, replay) with much less setup. |
| Database | **PostgreSQL** | Upserts make "no duplicates" easy. |
| Models | **Versioned folders** (weights, vocab, thresholds, metrics) | Every score traces back to the model that made it. |
| Model serving | **ONNX Runtime** vs PyTorch | Faster inference; measured. |
| Frontend | **One HTML page** (Chart.js + WebSocket) | Enough to show everything. |
| Skipped on purpose | Kafka, Kubernetes, React, Flink | Scope control. |

---

## The 3 detectors

| Detector | Looks at | Catches | Trained on |
|---|---|---|---|
| **LSTM next-event** (DeepLog-style) | Order of events in a request | Skipped, wrong or extra steps | Normal requests only |
| **Timing** | Each part's time vs requests that took exactly the same path | Slow requests | Normal requests only |
| **Rate detector** | Count of each event type per 10s vs rolling average | Error floods, traffic drops | Nothing (statistics) |

*Changed in Phase 5:* the timing detector replaced the Isolation Forest. On the
development data it caught more slow requests, and the forest's score saturates
(a request 50× slower scored barely higher). See README, "Design decisions".

Every alert says **which detector fired and why**.

### How we measure them

- **Per request:** precision, recall, F1, PR-AUC on held-out requests. Labels come
  from the fault injector.
- **Per incident:** % of faults caught, median time to detect, false alarms per hour.
- **Ablation:** each detector alone vs all three, to show each one earns its place.
- **Thresholds** are set on validation data to hit a target false-alarm rate, never on test data.

---

## Phases

**One commit per working step**: each commit adds one piece, with its tests,
and leaves the project working (CI green). Every phase ends with a full check
and a push. Tests are written inside each phase, not at the end.

### Commit plan

| # | Phase | Commit |
|---|---|---|
| 1 | — | Project roadmap ✅ |
| 2 | 1 | Foundation: package, config, Redis + Postgres, CI ✅ |
| 3 | 2 | Demo shop, traffic generator, fault injector ✅ |
| 4 | 3 | Database schema + migrations (Alembic) ✅ |
| 5 | 3 | Shipper: tail log files, send batches ✅ |
| 6 | 3 | Ingest API → `raw-logs` stream ✅ |
| 7 | 3 | Parser workers: Drain3 + shared pattern registry ✅ |
| 8 | 3 | Session builder → sessions in Postgres ✅ |
| 9 | 4 | Ack after processing, reclaim from dead workers, idempotent writes ✅ |
| 10 | 4 | Dead-letter stream, backpressure (429), graceful shutdown ✅ |
| 11 | 4 | Chaos test: kill workers, prove 0 lost / 0 duplicated ✅ |
| 12 | 5 | Dataset builder + baselines ✅ |
| 13 | 5 | LSTM + timing detector training, threshold calibration, versioned models ✅ (+ fixes from the live check) |
| 14 | 5 | ONNX export + scoring workers ✅ |
| 15 | 5 | Rate detector ✅ |
| 16 | 6 | Alert manager: group, explain, auto-resolve ✅ |
| 17 | 6 | Query API + WebSocket ✅ |
| 18 | 6 | Dashboard with Break/Fix buttons ✅ |
| 19 | 7 | Load test + benchmark script ✅ |
| 20 | 7 | Optimize the bottleneck ✅ (livelock fix, hiredis, fewer checks) |
| 21 | 8 | Fault evaluation + ablation ✅ |
| 22 | 8 | Drift re-calibration, health checks, auth, structured logs ✅ |
| 23 | 8 | Final README: diagram, results, demo GIF ✅ |
| 24 | 9 | Public demo mode: limited Break, self-fixing faults, retention ✅ |
| 25 | 9 | Production stack (Caddy HTTPS), server setup, auto-deploy ✅ |
| 26 | 9 | Go live: public link in the README |

### Phase 1: Foundation
- Package layout, config from environment variables (`pydantic-settings`), `.env.example`.
- `pyproject.toml` with pinned dependencies, ruff (lint), pytest.
- GitHub Actions: lint + tests on every push.
- Docker Compose with Redis + Postgres and health checks.

**Done when:** CI is green; a fresh clone runs `pytest` and `docker compose up`.

### Phase 2: Demo shop + traffic + fault injector
- Services `shop`, `inventory`, `payment`, `shipping`; each calls the next and passes `request_id`.
- Realistic plain-text logs (timestamps, levels, request IDs, amounts, timings).
- Normal variety: browse / search / cart / checkout, cache hits and misses,
  out-of-stock, occasional normal retries.
- Traffic generator with a configurable rate.
- Fault injector per service: slow, errors, skipped step, crash. Records start/end
  times and every affected request ID.

**Done when:** steady log traffic flows, and turning a fault on visibly changes the
logs and is recorded.

### Phase 3: Live pipeline
- Shipper → Ingest API → `raw-logs` stream.
- Parser workers: Drain3 + shared pattern registry (same template → same ID on every worker).
- Session builder: closes a request on its last line plus a short grace period, or on timeout.
- Sessions saved to Postgres (SQLAlchemy + Alembic migrations).

**Done when:** shop traffic becomes complete, correctly grouped sessions in
Postgres, live. A test checks grouping against what the shop sent.

### Phase 4: Correct under failure
- Ack after processing, reclaim messages from dead workers, idempotent upserts.
- Retries + dead-letter stream, bounded queues + 429 backpressure, graceful shutdown.
- **Chaos test:** kill workers mid-stream, then compare counts with what the shop sent.

**Done when:** the chaos test shows **0 lost, 0 duplicated**.

### Phase 5: Models
- Build datasets from clean-traffic sessions in Postgres; split by time.
- Train the LSTM and the timing detector on normal requests; add the rate detector.
- Compare against simple baselines ("unseen sequence", "unseen event type").
- Calibrate thresholds on validation; export to ONNX; save a versioned model folder.
- Scoring workers load a model version and score live.

**Done when:** one command trains a versioned model and writes a metrics report,
and the scoring workers use it live.

### Phase 6: Alerts + dashboard
- Alert manager: one alert per service per incident, dedup, explanation, auto-resolve.
- Query API + WebSocket.
- Dashboard: live throughput, queue depth, alerts, alert detail, Break/Fix buttons.

**Done when:** the 2-minute demo works end to end.

### Phase 7: Measure & optimize
- Load test: lines/s, end-to-end latency (line written → alert), p50/p99. State the hardware.
- Scale scoring workers 1 → 2 → 4 and chart it.
- Find the bottleneck; fix it (batching, bulk inserts, batched inference). ONNX vs PyTorch.

**Done when:** a script generates the benchmark table and scaling chart.

### Phase 8: Fault evaluation + polish
- Automated run of 20+ injected faults: % caught, time to detect, false alarms/hour.
- Ablation table: which detector catches which fault type.
- Re-calibrate thresholds on recent clean traffic (handles drift).
- Health checks, structured logs, API key on admin endpoints.
- README: architecture diagram, results tables, demo GIF, one-command setup.

**Done when:** anyone can clone, run one command, and reproduce every number in the README.

### Phase 9: Public deployment
- One small cloud VM (e.g. Oracle Cloud's free tier) running the same `docker compose`.
- HTTPS with Caddy; the dashboard public, admin endpoints behind a key.
- Break buttons rate-limited, and faults fix themselves after 2 minutes.
- Redeploy from GitHub Actions on push to `main`.

**Done when:** a public link shows the live demo, and a push updates it.

### Bonus: HDFS benchmark
Run the same detectors on the public [Loghub HDFS](https://github.com/logpai/loghub)
dataset to show they work on real logs we didn't generate (`data/HDFS.log` is
already downloaded). Things learned from a first attempt:
- Only 3% of HDFS sessions are unique, so a random split puts copies of training
  data in the test set. Use a **time split** and a **dedup split** instead.
- On sequences never seen in training, the LSTM reached F1 ≈ 0.91; a "have I
  seen this exact sequence" rule got 0.05.

---

## Project structure

```
src/logad/
  common/      # config, Redis + DB clients, shared types
  shipper/     # tails the shop's log files, sends batches to ingest
  ingest/      # ingest API
  parser/      # Drain3 workers + pattern registry
  sessions/    # session builder
  models/      # dataset, baselines, LSTM, timing, training, ONNX serving
  scoring/     # scoring workers
  rates/       # rate detector
  scoring/     # scoring workers
  alerts/      # alert manager
  api/         # query API + WebSocket + dashboard
src/shop/      # demo services, traffic generator, fault injector
eval/          # fault evaluation, load tests, HDFS benchmark
tests/
migrations/
docker-compose.yml
```

---

## Rules

1. Every number in the README comes from a script anyone can re-run.
2. Never test on data the model trained on. Split by time, tune on validation.
3. Labels come from the fault injector, never by hand.
4. Every claim has a test ("survives a crash" → a test kills a worker).
5. Report p99, not averages; state the hardware.
6. Write down trade-offs and the alternatives we rejected.
