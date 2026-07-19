# PsyRag

[![CI](https://github.com/GuyShafir/PsyRag/actions/workflows/ci.yml/badge.svg)](https://github.com/GuyShafir/PsyRag/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/GuyShafir/PsyRag)](https://github.com/GuyShafir/PsyRag/releases/latest)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Adaptive graph memory that learns what to remember.**

PsyRag is a memory database for AI agents and knowledge systems. It stores facts
as a temporal, typed property graph, and — unlike vector or keyword memory — it
*adapts*: connections that get used grow stronger, connections that don't decay,
and an offline "sleep" cycle consolidates what matters. Recall reorders itself by
learned usefulness. The name is *psyche* + *RAG* — retrieval that behaves like a
mind. (Its edges are literally *engrams*: physical memory traces that strengthen
with use and fade without it.)

```
same query "pricing", before and after "metering" proved useful 20×:
  before:  model 0.15   uses 0.15   seats 0.15   metering 0.15   alpha 0.15
  after :  metering 0.33   model 0.04   uses 0.04   seats 0.04   alpha 0.04
```

Where a RAG store returns the same thing forever, PsyRag learns.

## Why it's different

| | vector / keyword memory | PsyRag |
|---|---|---|
| structure | flat chunks | temporal typed graph (relationships, validity) |
| recall | static similarity | weighted spreading activation that **learns from use** |
| forgetting | TTL / manual | biological: decay, homeostasis, sleep consolidation |
| truth changes | overwrite | native supersession (`[valid_from, valid_to)`) |
| signal | — | usage feedback (citations, task outcome, clicks) reshapes recall |
| provenance | — | per-fact origin, trust quarantine, purge-by-subject (GDPR) |

## Install

**Release binaries** (linux x86-64/arm64, macOS arm64) from the
[releases page](https://github.com/GuyShafir/PsyRag/releases):

```bash
# linux x86-64
curl -L https://github.com/GuyShafir/PsyRag/releases/latest/download/psyrag-v0.5.0-x86_64-unknown-linux-gnu.tar.gz | tar xz
# linux arm64 (Graviton, Ampere, Raspberry Pi)
curl -L https://github.com/GuyShafir/PsyRag/releases/latest/download/psyrag-v0.5.0-aarch64-unknown-linux-gnu.tar.gz | tar xz
# macOS (Apple silicon)
curl -L https://github.com/GuyShafir/PsyRag/releases/latest/download/psyrag-v0.5.0-aarch64-apple-darwin.tar.gz | tar xz

./psyrag --wal mem.wal serve
```

**Docker**:

```bash
docker run -p 8080:8080 -v psyrag-data:/data ghcr.io/guyshafir/psyrag:latest
```

**From source** (any recent stable Rust; the dependency tree is just
`serde`, `serde_json`, `tiny_http`):

```bash
scripts/install.sh          # cargo build --release -p psyrag
```

## Quickstart

```bash
# run it (web console at http://localhost:8080/)
psyrag --wal mem.wal serve --addr 127.0.0.1:8080

# teach it some facts, retrieve, and give feedback
psyrag --wal mem.wal ingest --file examples/inventory.json
psyrag --wal mem.wal retrieve --seed svc/api --depth 2
psyrag --wal mem.wal feedback --seed svc/api --used db/primary   # "that was useful"
psyrag --wal mem.wal sleep                                       # nightly consolidation
```

Or multi-tenant: `psyrag --data-dir /data/dbs serve --token $SECRET` gives every
tenant/agent an isolated database under `/db/{name}/…`.

## Production guarantees

Every claim below is enforced by CI on every push:

- **Durability** — CRC-framed, versioned, fsynced WAL with single-writer
  locking; a 2xx means it's on disk. Proven by a **kill -9 crash suite**
  (every acked write survives SIGKILL) and **real ENOSPC fault injection**
  (disk-full fails clean: reads keep serving, restart recovers exactly the
  acked writes).
- **Learned state survives operations** — sidecar weights are keyed by stable
  edge identity, so WAL checkpoint/compaction, purges, and restarts never
  lose what the graph has learned; sidecars are bound to their WAL (id + LSN)
  and the crash learning-gap is *measurable* (`psyrag verify`).
- **Idempotent writes** — `Idempotency-Key` replays are durable across
  restarts (fsynced before ack): at-least-once clients can never
  double-ingest or double-apply credit.
- **Multi-database isolation** — per-DB WAL/config/locks/quotas; one DB's
  ingest never blocks another's retrieval.
- **Security** — full / read-only / per-database bearer tokens; provenance
  per fact with **trust quarantine** (reversibly mask a hostile source) and
  **purge-by-subject** (GDPR: data removed from the disk bytes); feedback
  poisoning limits; TLS configs shipped in [`deploy/`](deploy/).
- **Operability** — Prometheus `/metrics`, structured JSON logs, built-in
  sleep/consolidate/checkpoint scheduling, backup/verify tooling, and an
  [ops runbook](docs/runbook.md) with RPO/RTO backed by the CI evidence.
- **Performance** — ~2,500 req/s mixed workload with retrieve p95 ≤ 5 ms on
  a stock CI runner; SLOs asserted from the server's own histograms in CI.
- **Verified adaptivity** — a golden learning-quality suite pins the
  *product*: recall must reorder around what proved useful, and stay
  deterministic, or CI fails.

See [CHANGELOG.md](CHANGELOG.md) for the full release story, and the
[releases page](https://github.com/GuyShafir/PsyRag/releases) for binaries
and the Docker image.

## Documentation

| doc | what |
|-----|------|
| [docs/concepts.md](docs/concepts.md) | the mental model and the full mechanism/math |
| [docs/architecture.md](docs/architecture.md) | crates, tiered memory, the `GraphBackend` seam, indexing, GCP |
| [docs/reference.md](docs/reference.md) | complete CLI, HTTP API, and config reference |
| [docs/integrations.md](docs/integrations.md) | web console + Python/ADK integration |
| [docs/deployment.md](docs/deployment.md) | Docker, security, observability, testing |
| [docs/runbook.md](docs/runbook.md) | operations: capacity, upgrades, backup drills, RPO/RTO |
| [python/README.md](python/README.md) | ADK memory service quickstart |
| [gcp/README.md](gcp/README.md) | Spanner / BigQuery backends (roadmap + export) |

## Components

PsyRag is one product, three Rust crates:

- **`psyrag-graph`** — the temporal typed property graph core (append-only,
  WAL, provenance, token index).
- **`psyrag-core`** — the adaptive memory engine (plasticity, retrieval,
  feedback, consolidation, sleep) and the `GraphBackend` tiered-storage seam.
- **`psyrag`** — the binary: server, CLI, web console.

Plus a **`python/`** package (ADK memory service, feedback adapters,
zero-dependency client with automatic idempotent retries).
