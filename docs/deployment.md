# Deployment, scheduling & testing

## Build

```bash
scripts/install.sh            # cargo build --release -p psyrag
scripts/install.sh --link /usr/local/bin/psyrag   # + symlink onto PATH
```

Minimal dependencies (`serde`, `serde_json`, `tiny_http`) — builds fast and
offline on any recent stable Rust (tested on 1.75).

## Docker

```bash
docker compose up --build     # server on :8080, state on a named volume
```

The Dockerfile is multi-stage (Rust builder → slim runtime with just the `psyrag`
binary). The compose volume mounts `/data`, where the WAL, plasticity sidecar, and
durable trace log live — so `docker compose restart` preserves learned weights and
pending deferred traces.

```bash
curl -s localhost:8080/health
curl -s -X POST localhost:8080/ingest -H 'content-type: application/json' \
  -d '{"json":"[{\"name\":\"api\",\"type\":\"svc\",\"edges\":[{\"dst\":\"db\",\"kind\":\"CALLS\"}]}]"}'
open http://localhost:8080/     # the console
```

See the **[operations runbook](runbook.md)** for capacity planning, the
upgrade procedure, backup/restore drills, RPO/RTO, and the HA position.
Ready-made TLS termination configs ship in [`deploy/`](../deploy/)
(nginx + Caddy).

## Security

The server binds `127.0.0.1` by default. To expose it:

```bash
psyrag serve --addr 0.0.0.0:8080 --token "$ADMIN_TOKEN" --read-token "$VIEWER_TOKEN"
```

- `--token` (or `$PSYRAG_TOKEN`) — full read/write access via
  `Authorization: Bearer`.
- `--read-token` (or `$PSYRAG_READ_TOKEN`) — read endpoints only (retrieval
  must pass `adapt=false, trace=false`).
- Without a token the server runs in open mode and warns on non-loopback
  binds. `DELETE /db/{name}` is disabled entirely in open mode.
- TLS: terminate at a reverse proxy (nginx/Caddy/Cloud Run); the server
  speaks plain HTTP.
- **Content poisoning & GDPR**: label ingests with provenance
  (`origin: "user:alice/session:42"`). A source that turns out hostile can be
  quarantined instantly (`POST /quarantine`, trust 0.0 — reversible mask) and
  its facts hard-deleted (`POST /purge` / `psyrag purge` — rewrites the WAL
  without the data). Purge is also the deletion-by-subject path for GDPR;
  remember pre-existing archives/backups still hold the data.
- **Traces are sensitive data**: the durable trace log persists retrieval
  structure derived from queries/conversations to disk unencrypted, and the
  WAL persists ingested facts. Encrypt the volume (LUKS/EBS/PD encryption)
  in any deployment handling user content; an application-level redaction
  hook at trace-write time is on the roadmap.

## Multi-database (isolation)

One server can host many fully isolated databases (per tenant, per agent,
per environment):

```bash
psyrag --data-dir /data/dbs serve --addr 127.0.0.1:8080
curl -X POST localhost:8080/db/tenant-a          # create
curl -X POST localhost:8080/db/tenant-a/ingest -d @...   # use
curl localhost:8080/dbs                          # list
```

Each DB is a directory `/data/dbs/<name>/{wal,sidecar.json,config.json}` with
its own lock and its own `RwLock` — one DB's ingest or sleep never blocks
another DB's retrieval. A DB with a `config.json` overrides the server-wide
plasticity config. Bare routes (`/retrieve`, …) address the `default` DB, so
single-DB clients work unchanged. `--max-open-dbs` caps resident DBs
(LRU-evicting idle ones); back up a DB by copying its directory (stop the
server or use a filesystem snapshot).

## Observability

- **Metrics**: point Prometheus at `GET /metrics` (send the read token via
  `authorization` in the scrape config when auth is on). Alert suggestions:
  `psyrag_db_wedged > 0` (page — writes are failing), `rate(psyrag_requests_total{status="5xx"}[5m]) > 0`,
  p99 of `psyrag_request_duration_seconds` per route, and
  `psyrag_db_wal_lsn` growth without matching checkpoints (log bloat).
- **Logs**: `--log-format json` emits one object per line on stderr
  (`request` events with method/path/db/status/ms; `maintenance`,
  `serve_start/serve_stop`, `open_bind`, `*_failed` events). Ship stderr to
  your log stack; `text` format keeps `key=value` pairs for humans.
- **Built-in maintenance**: `--consolidate-every 10m --sleep-every 24h
  --checkpoint-every 24h` replaces external cron; each task takes the DB
  write lock, runs, persists, and logs a `maintenance` event (wedged DBs are
  skipped and logged).

## Capacity & failure containment

- **Quotas**: `--max-db-mb` / `--max-db-edges` bound each database;
  `--max-mem-mb` bounds the server (evicts idle DBs under pressure, then
  sheds ingest with 429). Watch `psyrag_db_approx_bytes` growth against
  quota and schedule checkpoints before hitting it.
- **Disk-full is fail-clean**: a WAL write failure wedges the database
  read-only (503 writes, 200 reads, `psyrag_db_wedged` = 1); restarting
  after freeing space replays exactly what was acked (the torn record is
  repaired). Exercised for real in smoke §16 on a 2 MB ram-disk.

## Scheduling sleep

`sleep` is an offline batch op, not on the retrieval path. Run it on a schedule:

- **Cron / systemd timer**: `curl -X POST localhost:8080/sleep` nightly against
  the running server. (The CLI form `psyrag --wal … sleep` only works when no
  server holds the WAL — the single-writer lock will refuse it otherwise.)
- **GCP**: Cloud Scheduler → a Cloud Run job invoking `POST /sleep` (or the CLI in
  a one-shot container). Once per night, or per-N episodes for high-traffic agents.

`consolidate` (the lighter daytime pass) can run more frequently — e.g. after each
batch of feedback, or every few minutes — to keep per-source budgets tidy without
the aggressive downscale.

## Persistence, checkpointing & backup

Everything learned is three files under the WAL directory:

- `<wal>` — the graph facts (WAL). Replaying it reconstructs the graph.
- `<wal>.psyrag.json` — the plasticity sidecar (weights, decay, homeostat).
- `<wal>.psyrag.json.traces.jsonl` — the durable trace log.

Operations:

- **Checkpoint** (bound log growth + restart time): `POST /checkpoint`
  against a live server, or `psyrag checkpoint` offline. Schedule alongside
  sleep (e.g. nightly). Old logs are archived as `<wal>.archive-<ms>` —
  rotate them off-box for point-in-time history.
- **Backup**: `psyrag backup --out DIR` (offline; takes the lock so the copy
  set is consistent) or snapshot the volume. Restore = copy the files back.
- **Verify**: `psyrag verify` — CRC sweep, replay, sidecar loadability.
  Run it after restoring a backup or before trusting a moved data dir.

For a shared or multi-region store, back the graph with Spanner and the
operational state with AlloyDB (see [architecture.md](architecture.md)).

## Scaling

- **Single process** handles a graph that fits in memory with in-memory-speed
  retrieval. Start here.
- **Tiered** (page-in or server-side neighborhood over Spanner) when the graph
  outgrows one machine or must be shared. The `GraphBackend` seam is the insertion
  point; the boundary is per-query, never per-edge.
- The server runs a worker-thread pool; reads (plain retrieval, stats, match,
  graph) share a read lock per database, mutations take that database's write
  lock. Databases are independent — mutations in one never block reads in
  another.

## Testing

Three ways to verify, plus the console for manual exploration.

### Unit, golden, property & fuzz-lite tests
```bash
cargo test --release
```
Beyond unit tests this runs: the **golden learning suite** (fixed corpus,
exact ranking expectations), the **format fixture zoo** (every historical
on-disk format replays forever; future formats refuse loudly), **property
tests** (per-source L1 renorm budget, homeostat bounds under adversarial
mass, finite activations under random load), and **fuzz-lite** (hundreds of
seeded byte-mutated WALs and thousands of garbage payloads — errors allowed,
panics and silent misreplay are not).

### Load/soak with SLO assertions
```bash
scripts/load.sh 15 8          # 15s, 8 threads; SLO_P95_MS=25 to tighten
```
Drives a mixed workload (80% retrieve / 15% feedback / 5% ingest) and then
asserts from the server's own Prometheus histograms: zero 5xx, retrieve p95
under the SLO, and a flat memory estimate across a read-only soak tail.
Reference numbers (GitHub-hosted Linux runner, release build, 300-node
corpus, 8 threads, 15 s): **~2,500 req/s mixed, retrieve p95 ≤ 5 ms, zero
errors, memory byte-stable across the soak**. Runs in CI with a generous
250 ms SLO to absorb shared-runner variance.

### kill -9 crash-recovery suite
```bash
scripts/crash.sh 5            # 5 rounds of SIGKILL mid-write-stream
```
Each round streams rapid ingests, SIGKILLs the server at a random moment,
then asserts: the WAL verifies (torn tail repaired, no corruption), **every
acked (2xx) write is present after restart** — the fsync contract under
real crashes — and the recovered server accepts new writes. The WAL
persists across rounds, so later rounds recover a log that has already
survived earlier crashes. Runs in CI on every push.

### Scripted end-to-end suite (the one to run)
```bash
scripts/smoke.sh              # ./target/release/psyrag on port 8791
# or: scripts/smoke.sh /path/to/psyrag 9000
```
Asserts, exiting non-zero on any failure:
1. ingest lands the edges,
2. server health,
3. traced retrieval yields positive mass,
4. **learning converges** — after 20 feedback+consolidate cycles crediting one
   node, its salience dominates its siblings (>1.5×),
5. **durable trace survives restart** — a trace issued before a kill is still
   creditable by a fresh process,
6. consolidation reports,
7. **sleep** downscales and protects,
8. **multidb** — isolation between databases + token auth scopes,
9. **single-writer lock** — the CLI is refused while the server owns the WAL,
10. **checkpoint** — the WAL shrinks, verifies, and learned salience survives
    the id renumbering (stable sidecar keys),
11. **backup** — manifest + consistent file set,
12. **idempotent retry** — a repeated Idempotency-Key returns the identical
    response from the replay cache and never double-applies,
13. **provenance** — quarantine masks a hostile origin from recall; purge
    removes its facts from the WAL bytes; unrelated facts survive restart,
14. **operability** — Prometheus counters + per-db gauges, API version
    header, JSON request logs, and the built-in maintenance scheduler,
15. **quotas** — over-quota ingest → 507 while maintenance still works,
16. **ENOSPC fault injection** — on a tiny ram-disk/tmpfs: disk-full ingest
    fails clean, the DB wedges (reads keep serving, writes 503), and a
    restart on the still-full volume recovers. Skipped where no ram-disk
    can be created,
17. **durable idempotency** — an Idempotency-Key retry AFTER a server
    restart still replays the original response from the fsynced log,
18. **per-db tokens + poisoning limits** — a db-scoped token works in its
    own database, gets 403 elsewhere and on server admin; the feedback
    rate limit returns 429.

Expected tail: `==== 36 passed, 0 failed ====` (41 with the ENOSPC section).

### Python / ADK
```bash
psyrag serve --addr 127.0.0.1:8080 &
cd python && python3 adapters.py     # RAG-citation, episodic, contrastive (live)
pip install google-adk && python3 agent_example.py   # full ADK agent
```
