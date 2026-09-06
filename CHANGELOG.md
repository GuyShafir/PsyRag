# Changelog

## v0.7.0 — 2026-09-06

### The claim, measured
- `bench/`: a deterministic, zero-dependency adaptive-recall benchmark
  (`cargo run --release -p psyrag --example adaptive_bench`). 20 topics x 8
  candidates with a hidden useful subset invisible to text; attention-limited
  feedback (not an oracle); the useful set shifts mid-run. Adaptive recall
  climbs 0.28 -> 1.0, crashes below the static floor at the shift, and
  re-learns to ~0.88; the feedback-off ablation and BM25 never move. The
  README now leads with the chart, and CI asserts the claim on every push
  (`--check`), so it can never silently go stale. Two dynamics findings
  documented in `docs/bench.md` (prune floor x regime shift; depression +
  renormalization crushing post-shift candidates — issue #29).

### Exploration floor: negative feedback is now safe (issue #29)
- New `Config::explore_floor` (fraction of `w0`, default 0 = off). Negative
  credit could drive a live edge to exactly 0, where consolidation pruned it
  as dead and it could never be re-learned — a regime shift under
  contrastive feedback recovered to 0.03. With the floor, depression is
  bounded in stored state and every live edge keeps a minimum retrieval
  salience (a lens, like the trust mask; dead edges are never resurrected).
  Benchmark: contrastive feedback goes from worst (0.03) to best (0.97 vs
  0.88 positive-only). Turn it on whenever you feed negative credit. The
  benchmark gains a fourth series and CI now asserts the fix cannot regress.

## Unreleased

### Graph analytics over the wire
- `POST /blast` — blast-radius reachability at an instant, every hit carrying
  its full traversal path (`a -[K]-> b -[K]-> c`); direction down/up/both.
- `POST /diff` — the temporal diff ("what changed between t1 and t2") from
  the versioned history. Both were library-only since v0.3; now exposed over
  HTTP, in the console (Graph tab / Maintenance tab), and in the Python
  client (`blast()`, `diff()`).

### MCP hardening (follow-ups from the #19 review)
- Out-of-repo paths no longer leak into the graph; one sidecar save per
  Read/Edit event instead of two; model-facing `k`/`depth` clamped
  (1..=100 / 1..=8); socket lines capped at 1 MiB.

### Fuzzing (community contribution)
- `fuzz/` cargo-fuzz targets for WAL replay and entity JSON (by
  @VedantMadane, closing #26): workspace-excluded so the zero-dep build is
  untouched, weekly + manual time-budgeted runs with an evolving cached
  corpus and crash artifacts, plus a PR-time compile guard.

### Community
- `CONTRIBUTING.md` and issue templates; seeded good-first-issues
  (#23 `/graph` pagination, #24 PITR tooling, #25 cgroup-aware memory).

## v0.6.0 — 2026-07-28

### MCP: adaptive memory for Claude Code (community contribution)
- `psyrag mcp` (by @nanov — PsyRag's first outside contribution) embeds the
  engine and speaks MCP over stdio: one model-facing `psyrag_recall` tool,
  automatic ingestion of the agent's Read/Edit events into a file co-touch
  graph via a unix-socket hook shim (`psyrag mcp-send`), usage credit for
  recalled files that actually get opened, git-history cold start, and
  biology-matched maintenance (PreCompact → consolidate, >24h-stale startup
  → sleep). Memory lives in `.psyrag/` at the repo root. Install + hook
  wiring in docs/mcp.md. Zero new dependencies.

### WAL-shipping warm standby
- New `psyrag standby --primary URL` mode: a read-only warm replica that
  tails the primary's WAL over HTTP (`GET /wal/tail/{offset}`) into an exact
  local byte-copy, verified by a 64-byte overlap window on every poll —
  checkpoint/purge rewrites (or a swapped primary) are detected and trigger
  an automatic full resync. Learned plasticity weights ship too via periodic
  sidecar snapshots (`GET /wal/sidecar`), so ranking stays warm on the
  replica, not just facts.
- The standby serves the full read surface and refuses writes with 503.
  Failover is a deliberate manual promote: restart the same WAL under
  `psyrag serve`. RPO: facts ≤ one poll interval (default 1 s), weights ≤
  sidecar cadence (~5 s); RTO: reads are already up, writes = one restart.
- Replication endpoints are denied to read-only tokens. `scripts/standby.sh`
  drills the full lifecycle (replicate → read-only → weight shipping →
  checkpoint resync → primary kill → promote, zero acked-write loss) in CI.

### Semantic seed selection
- Nodes may carry a reserved `props.embedding` (bring-your-own vector, any
  model/dimension). It is indexed for cosine search and rides the existing
  ObserveNode op — journaled, replayed, checkpointed, and purged with no WAL
  format change; the index always reflects a node's current version.
- `POST /match` gains a vector mode (`{"vector": [...], "mode": "vector"}`)
  returning scored nearest nodes. `POST /retrieve` gains `seed_vector` +
  `seed_k`: the nearest embedded nodes are resolved and unioned with named
  `seeds` before spreading, and echoed back as `resolved_seeds`. Embeddings
  pick the entry points; the learned graph does the expansion.
- Console Retrieve tab gets a "vector" match mode; Python client gains
  `match_vector()` and `retrieve(seed_vector=, seed_k=)`. `seeds` is now
  optional on `/retrieve` (semantic-only retrieval).

## v0.5.0 — 2026-07-19

### Web console catches up with the server
- The console (served at `/`) now covers the whole v0.4.0 surface: bearer-token
  auth (full / read-only / db-scoped), a multi-database picker with
  create/drop, maintenance (sleep / checkpoint / consolidate), trust
  quarantine + purge-by-origin with type-to-confirm, ingest with provenance
  origin + CAI mode, `/touch`, seed search via `/match`, explain-mode
  retrieval with per-node graded feedback, a live settings editor, and a
  server panel (per-DB state, uptime, request counters/latency from
  `/metrics`). Tabs are deep-linkable (`/ui#trust`). Still a single
  self-contained HTML file with zero external assets.
- New API: `GET /config` (effective per-DB config; read scope) and
  `PUT /config` (replace config; applied live — decay/authority, trust mask,
  and homeostat parameters re-resolve without touching learned weights or
  controller runtime state; persists to the DB's `config.json` in multi-DB
  mode). `config` route class added to `/metrics`.

## v0.4.0 — 2026-07-18 (first tagged release)

The production-hardening release: PsyRag goes from prototype to a standalone
database with durability guarantees that are continuously proven in CI.

### Data integrity
- CRC-framed, versioned WAL with lineage identity; torn tails self-repair,
  mid-file corruption refuses loudly, legacy logs replay transparently.
- fsync contract: a 2xx means it is on disk — verified by a kill -9 crash
  suite (every acked write survives SIGKILL) and real ENOSPC fault injection
  on a full filesystem (fail-clean wedge, reads keep serving, recovery).
- Stable edge keys: learned plasticity state survives WAL
  checkpoint/compaction; sidecars are bound to their WAL (id + LSN) and the
  learning gap is measurable via `psyrag verify`.
- Checkpoint/compaction, consistent backup, and read-only verification
  tooling; single-writer flock; atomic snapshot writes everywhere.
- Durable idempotency: `Idempotency-Key` replays survive restarts (fsynced
  before ack); Python client auto-keys with same-key retries.

### Server
- Worker pool with per-database RwLock; request caps; honest 5xx on any
  persistence failure; graceful shutdown; deterministic retrieval with
  `explain=true`; wire-API versioning (`X-PsyRag-Api`).
- MultiDB: fully isolated named databases (`--data-dir`, `/db/{name}/...`),
  per-DB config/locks/quotas, LRU lifecycle.
- Auth: full / read-only / per-database bearer tokens; loopback default
  bind; TLS termination configs shipped (nginx, Caddy).
- Provenance: per-fact origin labels, trust quarantine (reversible
  retrieval-time mask), purge-by-subject (GDPR) removing data from the
  disk bytes; feedback poisoning limits (credit clamp + rate limit).
- Resource safety: per-DB size/edge quotas (507), server memory budget
  with idle eviction + load shedding (429), `--ephemeral-traces`.

### Operability
- Prometheus `/metrics` (bounded-cardinality request histograms + per-DB
  gauges incl. the wedged flag), structured JSON logging, built-in
  sleep/consolidate/checkpoint scheduling, ops runbook with RPO/RTO.

### Architecture
- The tiered-storage seam is real code: `psyrag_core::backend::GraphBackend`
  with an in-memory reference implementation and a conformance suite for
  future managed backends (Spanner/AlloyDB — roadmap).
- Indexed seed matching (token-prefix, O(log N + hits)).

### Verification (all enforced in CI on every push)
- 70 tests: unit, golden learning-quality, format fixture zoo (with the
  downgrade story), property tests, fuzz-lite, backend conformance.
- 36-41 assertion end-to-end smoke; kill -9 crash suite; load/soak with
  SLOs asserted from the server's own histograms (~2,500 req/s mixed,
  retrieve p95 ≤ 5 ms on CI runners); fmt + clippy -D warnings;
  cargo-deny + SBOM.

Zero runtime dependencies beyond serde, serde_json, and tiny_http.

## v0.3.1 and earlier

Pre-release prototype: temporal typed property graph, Hebbian plasticity
layer, spreading-activation retrieval, feedback/credit assignment, sleep
consolidation, ADK integration.
