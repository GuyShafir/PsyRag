# Changelog

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
