# Reference

Complete reference for the `psyrag` binary: CLI commands, HTTP API, and every
config knob.

## Global flags

Apply to every subcommand:

| flag | default | meaning |
|------|---------|---------|
| `--wal PATH` | `psyrag.wal` | graph WAL (facts). Created if absent. |
| `--sidecar PATH` | `<wal>.psyrag.json` | plasticity state (weights, decay, homeostat) |
| `--config PATH` | built-in defaults | config JSON (see below) |
| `--data-dir DIR` | — | multi-database layout: each DB lives in `DIR/<name>/{wal,sidecar.json,config.json}` |
| `--db NAME` | `default` | which database to address (requires `--data-dir`) |

With `--data-dir`, per-DB config precedence is: explicit `--config` > the DB's
own `config.json` > built-in defaults. DB names match `[a-z0-9_-]{1,64}`.

## Durability & single-writer

- The WAL is CRC-framed (`crc32hex8:{json}` per line, `#psyrag-wal:v1` header);
  legacy plain-NDJSON logs replay transparently and are appended to in the new
  format. A torn final record (crash mid-append) is truncated and recovered
  automatically; corruption anywhere else refuses to open (restore from
  backup) — a silently skipped record would misalign all sidecar `EdgeId`s.
- Batch boundaries fsync the WAL; sidecar snapshots are written atomically
  (temp + fsync + rename); retrieval traces are fsynced on store. A 2xx from a
  mutating HTTP endpoint means the change is on disk.
- Sidecar snapshots (v2) key every entry by a **stable edge key** (FNV-1a of
  `src, dst, kind, valid_from`), so learned weights survive WAL compaction's
  id renumbering. v1 (positional) sidecars still load.
- `psyrag checkpoint` / `POST /checkpoint` bound WAL size and replay time;
  archives hold the dropped history.
- The WAL is held under an exclusive lock (`flock`): running the CLI against a
  WAL a live `psyrag serve` owns fails fast with a clear error. Use the HTTP
  API against a running server.
- **Visibility contract**: a write is visible to reads the moment its 2xx is
  returned (single in-process store, no replication lag) and durable at that
  same moment (fsync before ack). There is no read-your-writes gap to reason
  about.
- **Fail-clean writes**: if a WAL write/flush fails AFTER ops were applied in
  memory (e.g. ENOSPC), memory and disk have diverged and cannot be
  reconciled in-process — the database **wedges read-only** (writes return
  503, reads keep serving, `/dbs` shows `wedged`). Restart the server after
  fixing the disk; replay restores exactly what the WAL acked.
- **Formats are versioned and refuse forward loudly**: a WAL or sidecar
  written by a newer psyrag fails to open with an explicit message after a
  rollback — restore the pre-upgrade backup or upgrade again.

## CLI commands

### `psyrag config [--write PATH]`
Print the effective config as JSON, or with `--write` emit a fully-commented
example to `PATH`.

### `psyrag ingest --file F [--reconcile] [--cai] [--ts MS] [--origin LABEL]`
Ingest entity JSON into the WAL, then sync the sidecar.
- `--file` — entity array / NDJSON, or a Cloud Asset Inventory snapshot with `--cai`.
- `--reconcile` — treat the snapshot as ground truth (retire missing edges).
- `--cai` — parse a GCP CAI export (requires the `gcp` build feature).
- `--ts` — event time in ms (default: now).
- `--origin` — provenance label for every fact in the batch (a per-entity
  `"origin"` field in the payload overrides it). Conventions like
  `user:alice/session:42` enable per-source trust and purge-by-subject.

### `psyrag retrieve --seed N [--seed N…] [--depth D] [--fan F] [--top-k K] [--ts MS] [--adapt]`
Weighted spreading-activation retrieval.
- `--seed` — one or more seed node names (repeatable).
- `--depth` / `--fan` — spread hops / per-hop outflow factor (default from config).
- `--top-k` — number of results (default 10).
- `--adapt` — also feed the homeostat (updates `lambda_scale`, persists sidecar).

### `psyrag touch --edge src,dst,kind,R [--edge …] [--ts MS]`
Low-level manual reinforcement of specific edges by amount `R` (repeatable).

### `psyrag feedback --seed N… { --used NODE… | --credit name,score… | --reward R [--spread by_activation|uniform] } [--depth D] [--top-k K] [--ts MS]`
Usage feedback (the learning step). Choose one credit mode:
- `--used NODE…` — explicit: these surfaced nodes were useful.
- `--credit name,score…` — graded / contrastive (mixed signs allowed).
- `--reward R [--spread …]` — episodic: one scalar spread over surfaced nodes.

### `psyrag consolidate [--ts MS] [--apply-conflicts]`
Run a daytime consolidation cycle (materialize / prune / renorm + conflict
detection). `--apply-conflicts` journals detected supersessions (a truth change).

### `psyrag sleep [--ts MS]`
Run an offline sleep cycle (downscale + protected prune + renorm). Schedule this
nightly; it is not on the retrieval path.

### `psyrag stats`
Print sidecar + graph stats as JSON.

### `psyrag export-bq [--out DIR] [--dataset NAME] [--ts MS]`
Export the learned graph as BigQuery-ready NDJSON + DDL + GQL + a load script.
Defaults: `--out ./bq_out`, `--dataset psyrag`. No GCP credentials needed to
produce the artifacts. See [gcp/README.md](../gcp/README.md).

### `psyrag checkpoint [--no-archive]`
Compact the WAL down to the ops that reconstruct current open state (live
nodes/edges keep their original timestamps, so `valid_from` — and with it the
sidecar's stable edge keys — is preserved). The pre-compaction log is kept as
`<wal>.archive-<ms>` unless `--no-archive`. Outstanding retrieval traces are
invalidated (they reference pre-compaction ids). Offline form; against a live
server use `POST /checkpoint`.

### `psyrag verify`
Read-only integrity check: WAL structure (framing/CRC per record, torn tail
vs. mid-file corruption), a full replay (node/edge counts), and sidecar
loadability. Exits non-zero on corruption. Lock-free — safe against a live
server (may observe an in-flight tail).

### `psyrag backup --out DIR`
Consistent file-copy backup of the database (WAL + sidecar + trace log +
`config.json` if present) plus a `manifest.json`. Takes the WAL lock without
replaying, so it fails fast if a server owns the database — stop the server
first or use filesystem snapshots. Restore = copy the files back.

### `psyrag purge --origin PREFIX`
**Irreversibly delete** every fact whose provenance matches the prefix: nodes
whose current version came from it, edges observed from it, and edges
touching a purged node. The WAL is rewritten without the data (the purged
names are gone from the bytes, not just masked) and never archived by this
path. Learned salience for surviving edges carries over via stable keys.
Pre-existing archives and backups still contain the data — rotate them.
This is the GDPR deletion-by-subject path. Offline form; live servers use
`POST /purge`.

### `psyrag db {list | create NAME} --data-dir DIR`
Manage the multi-database layout from the CLI: `list` prints every database
under the data dir (with WAL size), `create` materializes a new one.

### `psyrag serve [flags]`
Run the HTTP server with the web console at `/`.

| flag | default | meaning |
|------|---------|---------|
| `--addr HOST:PORT` | `127.0.0.1:8080` | bind address (loopback by default; pair a public bind with `--token`) |
| `--data-dir DIR` | — | serve every database under `DIR` (multi-DB mode) |
| `--token T` | `$PSYRAG_TOKEN` | bearer token for full access; unset = open mode |
| `--read-token T` | `$PSYRAG_READ_TOKEN` | bearer token restricted to read endpoints |
| `--workers N` | `min(cores, 8)` | request worker threads |
| `--max-body-mb N` | 32 | request body cap (oversize → 413) |
| `--max-open-dbs N` | 64 | concurrently open databases (LRU-evicts idle ones) |
| `--max-db-mb N` | ∞ | per-DB size quota (estimate); at quota, `/ingest` → 507 while maintenance/feedback still work |
| `--max-db-edges N` | ∞ | per-DB edge-count quota, same semantics |
| `--max-mem-mb N` | ∞ | server memory budget over all open DBs; over budget idle DBs are evicted, then `/ingest` sheds with 429 |
| `--db-token NAME=TOKEN` | — | repeatable: full-access token **scoped to one database** (server admin + other DBs → 403) |
| `--max-credit R` | 100 | server-side clamp on feedback \|reward\|/\|score\| (0 = off) |
| `--max-feedback-per-min N` | ∞ | per-DB `/feedback` rate limit → 429 |
| `--ephemeral-traces` | off | keep retrieval traces in memory only (no trace data on disk; deferred credit does not survive restarts) |
| `--log-format F` | `text` | `json` for structured one-object-per-line logs on stderr |
| `--sleep-every D` | — | run sleep on every open DB each interval (`90s`/`30m`/`24h`) |
| `--consolidate-every D` | — | run consolidation each interval |
| `--checkpoint-every D` | — | run WAL checkpoint each interval |

The server drains and flushes every open database on SIGINT/SIGTERM (clean
Docker stops). Without `--data-dir` it serves the single `--wal` database.

### `psyrag monitor [--url URL] [--interval-ms N]`
Live terminal dashboard polling a running server's `/metrics` (open-mode
servers only; it sends no auth header).

## HTTP API

Base URL is the `serve` address. All bodies and responses are JSON.

**Databases.** Bare routes (below) address the `default` database. Prefix any
of them with `/db/{name}` to address another database (multi-DB mode):
`POST /db/tenant-a/retrieve`, `GET /db/tenant-a/stats`, … Databases are fully
isolated — separate WAL, sidecar, traces, config, and locks; one DB's ingest
never blocks another DB's retrieval.

**Capacity.** Growth quotas gate `/ingest` only — consolidate, checkpoint,
purge, sleep, and feedback always work on a full database, so there is
always a way back under quota. Sizes are the server's own structural
estimate (`approx_bytes` in `GET /dbs` and `psyrag_db_approx_bytes` in
metrics), not RSS.

**API version.** Every response carries `X-PsyRag-Api: 1`. The wire API is
versioned independently of the on-disk formats; breaking changes bump it.

**Idempotent retries.** Every mutating endpoint accepts an
`Idempotency-Key` header. A repeated (endpoint, key) within the window
(24h, 4096 entries per DB) replays the original response byte-identically
with `Idempotency-Replayed: true` — an at-least-once retry can never
double-ingest or double-apply credit. **Dedup is durable**: final outcomes
(2xx/4xx) are fsynced to `<sidecar>.idem.jsonl` *before* the response is
sent, so replay works across server restarts and crashes. 5xx are not
recorded (the retry should reprocess); concurrent duplicates get 409
(in-flight markers are memory-only — a marker that died with a crash should
be retryable). The Python client generates keys automatically and retries
with the same key.

**Auth.** Tokens come in three scopes: `--token` (full, all databases),
`--read-token` (read-only, all databases), and repeatable
`--db-token NAME=TOKEN` (full, but confined to `/db/NAME/...` — server-level
routes and other databases return 403). With any token set, every endpoint except
`/health`, `/live`, `/ready`, and the UI shell requires
`Authorization: Bearer <token>`. The read token may hit GET endpoints,
`POST /match`, and `POST /retrieve` only with `"adapt": false, "trace": false`
(both mutate state). Everything else needs the full token.

### `GET /dbs`
`{ "dbs": [{db, open, nodes?, edges?, traces?}] }` — every database on disk
plus its open state.

### `POST /db/{name}`
Create (or ensure) a database. Requires multi-DB mode and write scope.

### `DELETE /db/{name}`
Drop a database — closes it and deletes its directory. Irreversible, so it is
disabled unless the server runs with `--token`; returns 409 while requests are
in flight.

### `GET /metrics`
Prometheus exposition (text format): `psyrag_requests_total{route,status}`,
`psyrag_request_duration_seconds` histograms per route class,
`psyrag_uptime_seconds`, `psyrag_open_dbs`, and per-database gauges
(`psyrag_db_nodes/edges_live/edges_dead/lambda_scale/ewma_mass/traces/wedged/wal_lsn{db=...}`).
Request labels use a closed route-class set, so cardinality is bounded.
JSON stats remain at `GET /stats` (used by the console and `psyrag monitor`).

### `GET /live` and `GET /ready`
Liveness / readiness probes (readiness implies the default DB replayed
successfully at startup). Unauthenticated.

### `GET /` and `GET /ui`
The web console (HTML). See [integrations.md](integrations.md).

### `GET /health`
`{ "ok": true }`.

### `GET /stats` and `GET /metrics`
Sidecar + graph stats: `nodes`, `edges_total`, `edges_live`, `edges_dead`,
`lambda_scale`, `setpoint`, `ewma_mass`, `integral`, `weight_min/mean/max`.

### `GET /wal/tail/{offset}` and `GET /wal/sidecar`
Replication source for the warm standby (`psyrag standby`). `tail` returns
the durable (fsynced) WAL bytes from `offset` with `X-PsyRag-Wal-Id` /
`X-PsyRag-Wal-Len` headers; an offset past the end (log rewritten by
checkpoint/purge) is a 409 telling the follower to resync from 0.
`sidecar` snapshots and returns the current learned-weights file (204 if
nothing learned yet). Both expose raw facts and are **denied to read-only
tokens**. You normally never call these by hand — see the standby section
of the [runbook](runbook.md).

### `GET /config` and `PUT /config`
`GET` returns `{ config, persistent }` — the database's effective plasticity
config and whether edits persist (`true` in multi-DB mode, where they are
written to the DB's `config.json`; `false` in single-DB mode, where they
apply in-memory until restart). `PUT` takes a config JSON (same shape as
`psyrag config --write`; omitted fields take defaults), applies it **live**,
and re-resolves all derived state: per-edge decay (`authority_by_kind` /
`lambda_base` / `beta`), the trust mask, and the homeostat's controller
parameters (its runtime state — integral, EWMA — is kept). Learned weights
are never modified by a config change. `GET` is allowed under the read
scope; `PUT` requires write scope.

### `POST /ingest`
`{ "json": "<entities>", "reconcile"?: bool, "cai"?: bool, "ts"?: int }` →
`{ "edges", "nodes", "stale_retired" }`. A node's `props` may carry a
reserved `"embedding": [f32, ...]` (bring-your-own vector, any model/dim);
it is indexed for semantic seed selection and rides the WAL like any other
property — journaled, replayed, checkpointed, and purged with no format
change. Re-observing a node without an embedding (or retiring it) drops the
entry; the vector always reflects the node's current version.

### `POST /retrieve`
`{ "seeds"?: [..], "seed_vector"?: [f32], "seed_k"?: int, "depth"?, "fan"?, "top_k"?, "ts"?, "adapt"?: bool, "trace"?: bool, "explain"?: bool }`.
Returns `{ mass, lambda_scale, top: [{node, node_type, activation, origin}] }`.
**Semantic seeding**: `seed_vector` (a query embedding) resolves the
`seed_k` (default 4) nearest embedded nodes by cosine and unions them with
any named `seeds` before spreading; the chosen entry points are echoed back
as `resolved_seeds: [{node, score}]`. At least one of `seeds`/`seed_vector`
is required. If `"trace": true`, returns `{ result, trace_id }` and stores the
trace for deferred feedback. If `"explain": true`, the response carries
`explain.fired: [{src, dst, kind, delta}]` — the activation paths, in hop
order ("why did it recall X"). Explain and `seed_vector` are read-only and
allowed under the read scope. Retrieval is deterministic: identical inputs at
an identical `ts` return identical results (ties break by ingestion order).

### `POST /match`
`{ "tokens": [..], "limit"?, "mode"? }` → `{ "nodes": [names] }`. Resolve
free-text tokens to node names for seed selection. Default mode `"token"`
uses an inverted index over name tokens (case-insensitive, token-**prefix**
matching: `meter` finds `svc/metering-api`) — O(log N + hits), deterministic
ascending-id order. `"mode": "substring"` restores the legacy full-name
substring scan (O(nodes)) for mid-token needles.
**Vector mode**: `{ "vector": [f32], "limit"?, "mode": "vector" }` (mode
implied when `vector` is present) → `{ "nodes": [names], "hits": [{node,
score}], "indexed": N }`: cosine top-k over node embeddings, nearest first,
`indexed` being the number of embedded nodes. Entries whose dimension differs
from the query are skipped; an empty or zero query vector is a 400.

### `POST /feedback`
Provide **one** target and **one** credit spec.
- Target: `"seeds": [..]` (retrieve fresh at `ts`) **or** `"trace_id": N` (credit a
  stored trace — deferred).
- Credit: `"used": [names]` (explicit) **or** `"nodes": [[name,score]]`
  (graded/contrastive) **or** `"reward": R, "spread": "by_activation"|"uniform"`
  (episodic).
Returns `{ edges_reinforced, total_positive_r, total_negative_r, hits, misses }`.

### `POST /checkpoint`
`{ "archive"?: bool }` (default true) → `{ report: { ops_written, bytes_before,
bytes_after, archive }, traces_cleared, note }`. Compacts this database's WAL
in place (see `psyrag checkpoint`); the server keeps running and keeps its
full in-memory history until restart. Stored trace_ids are invalidated.

### `POST /quarantine`
`{ "origin_prefix": "...", "trust": 0.0 }` — set the trust level for a
provenance prefix (longest prefix wins). Trust multiplies edge salience at
retrieval time only: `0.0` removes the source's influence entirely, and
restoring `1.0` restores recall — learned weights are never modified. In
multi-DB mode the updated config persists to the DB's `config.json`.
Config equivalent: `trust_by_origin: {"user:mallory": 0.0}`.

### `POST /purge`
`{ "origin_prefix": "..." }` → `{ report: { nodes_dropped, edges_dropped,
... }, traces_cleared }`. Irreversible content deletion by provenance (see
`psyrag purge`); in-memory removal is immediate, no restart needed. Like
`DELETE /db/{name}`, disabled unless the server runs with `--token`.

### `POST /consolidate`
`{ "ts"?, "apply_conflicts"?: bool }` → `{ stats, conflicts, applied_ops }`.

### `POST /sleep`
`{ "ts"? }` → `{ downscaled, pruned, protected, renormalized_sources, live_after,
mean_weight_before, mean_weight_after }`.

### `GET /graph`
`{ nodes: [{id, name, type}], edges: [{source, target, kind, weight, open}],
truncated }`. Current weighted graph for visualization (bounded).

### `GET /traces`
`{ ids: [..], count }`. Stored trace ids.

### `GET /trace/{id}`
`{ t, surfaced: [{node, activation}], fired: [{src, dst, kind, delta}] }`.

## Config reference

Config is JSON (`psyrag config --write config.json` emits a commented example).
Every field is optional; omitted fields take the default shown.

### Update rule
| key | default | meaning |
|-----|---------|---------|
| `alpha` | 0.05 | reinforcement gain per unit clipped R |
| `lambda_base` | 0.05 | base decay rate per second |
| `beta` | 1.0 | authority sensitivity in `λ_base/(1+β·auth)` |
| `w0` | 0.5 | initial weight of a new edge |
| `r_clip` | 1.0 | per-touch clip on R |

### Homeostat
| key | default | meaning |
|-----|---------|---------|
| `setpoint` | 0.5 | target mean activated mass |
| `k_i` | 0.02 | integral gain |
| `ewma_beta` | 0.8 | smoothing on observed mass |
| `scale_min` / `scale_max` | 0.25 / 8.0 | bounds on `lambda_scale` |
| `integral_min` / `integral_max` | −500 / 500 | anti-windup clamp |

### Retrieval
| key | default | meaning |
|-----|---------|---------|
| `depth` | 2 | spread hops |
| `fan` | 0.9 | per-hop outflow factor |
| `activation_epsilon` | 1e-6 | prune propagation below this |

### Consolidation
| key | default | meaning |
|-----|---------|---------|
| `theta` | 0.01 | daytime prune floor |
| `norm_target` | 1.0 | per-source L1 budget |

**Config retroactivity.** `authority_by_kind`, `lambda_base`, `beta`, and
`trust_by_origin` are re-resolved against every edge on each load — editing
the config and restarting (or reloading a DB) applies the new decay/trust
behavior to existing edges, not just new ones.

### Authority & conflicts
| key | default | meaning |
|-----|---------|---------|
| `authority_by_kind` | `{}` | per-kind authority (raises decay resistance) |
| `authority_default` | 0.0 | authority for unlisted kinds |
| `functional_kinds` | `[]` | single-valued predicates (only these are conflict-checked) |

### Provenance & trust
| key | default | meaning |
|-----|---------|---------|
| `trust_by_origin` | `{}` | origin-prefix → trust multiplier on retrieval (longest prefix wins; 0.0 = quarantined, unlisted = 1.0) |

### Feedback
| key | default | meaning |
|-----|---------|---------|
| `feedback_gamma` | 0.5 | path-credit decay per hop upstream |
| `feedback_hit` | 1.0 | credit magnitude for a used node |
| `feedback_miss_penalty` | 0.0 | negative credit for surfaced-but-unused (0 = off) |

### Sleep
| key | default | meaning |
|-----|---------|---------|
| `sleep_downscale` | 0.6 | multiplicative weight downscale (<1) |
| `sleep_theta` | 0.05 | aggressive prune floor after downscale |
| `protect_top_frac` | 0.2 | top fraction by weight protected from the sleep prune |
