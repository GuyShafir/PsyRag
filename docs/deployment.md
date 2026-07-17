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

### Unit tests
```bash
cargo test --release          # 26 tests: psyrag-graph (12) + psyrag-core (13) + monitor (1)
```

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
11. **backup** — manifest + consistent file set.

Expected tail: `==== 16 passed, 0 failed ====`.

### Python / ADK
```bash
psyrag serve --addr 127.0.0.1:8080 &
cd python && python3 adapters.py     # RAG-citation, episodic, contrastive (live)
pip install google-adk && python3 agent_example.py   # full ADK agent
```
