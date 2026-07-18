# PsyRag

**Adaptive graph memory that learns what to remember.**

PsyRag is a memory layer for AI agents and knowledge systems. It stores facts as
a temporal, typed property graph, and — unlike vector or keyword memory — it
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

## What's in the box

- **`psyrag`** — a single binary: HTTP server, CLI, and a built-in web console.
- **Adaptive retrieval** — weighted spreading activation with time-decayed edges,
  homeostatic normalization, and multi-mode usage feedback.
- **Sleep** — offline consolidation (synaptic downscaling + protected pruning).
- **ADK integration** — a drop-in `BaseMemoryService` for Google ADK agents whose
  recall learns from use, plus feedback adapters and a zero-dep client.
- **Optional GCP backends** — Spanner Graph as long-term system-of-record,
  BigQuery as the analytics/replay sink.
- **Durable** — CRC-framed, fsynced WAL; atomic sidecar snapshots; durable
  trace store; single-writer locking; graceful shutdown. A 2xx means it's on disk.
- **Multi-database** — one server, many fully isolated databases
  (`/db/{name}/…`): per-DB WAL, config, and locks. Per tenant, per agent, per env.
- **Auth** — bearer tokens with full and read-only scopes; loopback bind by default.

## Quickstart

```bash
scripts/install.sh                       # build the release binary
scripts/smoke.sh                         # 7-assertion end-to-end test

# run it (opens a web console at http://localhost:8080/)
./target/release/psyrag --wal mem.wal serve --addr 127.0.0.1:8080
```

```bash
# teach it some facts, retrieve, and give feedback
psyrag --wal mem.wal ingest --file inventory.json
psyrag --wal mem.wal retrieve --seed api --depth 2
psyrag --wal mem.wal feedback --seed api --used db      # "db was useful"
psyrag --wal mem.wal sleep                               # nightly consolidation
```

Docker:

```bash
docker compose up --build                # server on :8080, state on a volume
```

## Documentation

| doc | what |
|-----|------|
| [docs/concepts.md](docs/concepts.md) | the mental model and the full mechanism/math |
| [docs/architecture.md](docs/architecture.md) | crates, tiered memory, backend seam, indexing, GCP |
| [docs/reference.md](docs/reference.md) | complete CLI, HTTP API, and config reference |
| [docs/integrations.md](docs/integrations.md) | web console + Python/ADK integration |
| [docs/deployment.md](docs/deployment.md) | Docker, scaling, scheduling sleep, testing |
| [python/README.md](python/README.md) | ADK memory service quickstart |
| [gcp/README.md](gcp/README.md) | Spanner / BigQuery backends |

## Components

PsyRag is one product, three Rust crates:

- **`psyrag-graph`** — the temporal typed property graph core (append-only, WAL).
- **`psyrag-core`** — the adaptive memory engine (plasticity, retrieval, feedback,
  consolidation, sleep).
- **`psyrag`** — the binary: server, CLI, web console.

Plus a **`python/`** package (ADK memory service, adapters, client) and optional
**`gcp/`** backends.


