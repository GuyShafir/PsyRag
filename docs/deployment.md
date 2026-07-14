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

## Scheduling sleep

`sleep` is an offline batch op, not on the retrieval path. Run it on a schedule:

- **Cron / systemd timer**: `psyrag --wal /data/mem.wal sleep` nightly.
- **GCP**: Cloud Scheduler → a Cloud Run job invoking `POST /sleep` (or the CLI in
  a one-shot container). Once per night, or per-N episodes for high-traffic agents.

`consolidate` (the lighter daytime pass) can run more frequently — e.g. after each
batch of feedback, or every few minutes — to keep per-source budgets tidy without
the aggressive downscale.

## Persistence & backup

Everything learned is three files under the WAL directory:

- `<wal>` — the graph facts (WAL). Replaying it reconstructs the graph.
- `<wal>.psyrag.json` — the plasticity sidecar (weights, decay, homeostat).
- `<wal>.psyrag.json.traces.jsonl` — the durable trace log.

Back up the directory, or point them at a mounted/managed volume. For a shared or
multi-region store, back the graph with Spanner and the operational state with
AlloyDB (see [architecture.md](architecture.md)).

## Scaling

- **Single process** handles a graph that fits in memory with in-memory-speed
  retrieval. Start here.
- **Tiered** (page-in or server-side neighborhood over Spanner) when the graph
  outgrows one machine or must be shared. The `GraphBackend` seam is the insertion
  point; the boundary is per-query, never per-edge.
- Retrieval is read-only and lock-light; the server serializes mutations
  (ingest / feedback / consolidate / sleep) behind a single lock.

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
7. **sleep** downscales and protects.

Expected tail: `==== 7 passed, 0 failed ====`.

### Python / ADK
```bash
psyrag serve --addr 127.0.0.1:8080 &
cd python && python3 adapters.py     # RAG-citation, episodic, contrastive (live)
pip install google-adk && python3 agent_example.py   # full ADK agent
```
