# GCP backends (optional)

PsyRag has two distinct places a GCP service can plug in. Don't
conflate them:

1. **Long-term memory (system of record)** — *replaces* the embedded WAL/graph, so
   psyrag-graph becomes a compute layer over managed storage. This is the online,
   hot path. **Spanner (Spanner Graph)** is the pick. See "Backend replacement".
2. **Analytics / replay sink** — a *cold* mirror for GQL analytics and sleep
   replay. **BigQuery**. This is what `export-bq` produces (below).

The architecture, the coarse `GraphBackend` seam, indexing, and the sleep/wake
flush between tiers are specified in `../docs/architecture.md` §7c–7e. Summary of the rule
that governs everything: **the backend boundary is per-query / per-cycle, never
per-edge** — `neighborhood(seeds, depth, at_ts)` returns a whole k-hop subgraph in
one round trip; the plasticity compute stays in-process.

## Backend replacement: Spanner Graph as long-term memory

Why Spanner for the system of record with real traversal:

- **Server-side traversal.** Native property graph + GQL means the k-hop
  `neighborhood()` fetch is one variable-length `MATCH`, not k round trips.
- **Locality.** Interleaved tables physically co-locate a node's edges — the
  managed equivalent of the in-process `out_adj`.
- **Temporal.** Model `[valid_from, valid_to)` as columns; index/partition on
  `valid_to` so the hot working set is only open edges (the current/history split
  in DESIGN §7d).
- **Scale + consistency** for a shared, multi-tenant store.

psyrag-graph still runs the plasticity (decay, weighting, homeostasis, sleep)
in-process on the returned subgraph. Spanner does *structure + durability*; the
layer does *salience*. **AlloyDB** is the better home if you'd rather keep the
operational state (weight columns, durable trace store) transactional and split it
from structure; **pgvector / Vertex Vector Search** is where semantic seed
selection plugs in (embeddings pick entry points, the graph does learned
expansion — DESIGN §7d.4). Implement `GraphBackend` for the store you choose; the
in-memory path is the reference implementation.

## Analytics / replay sink: BigQuery export

Export the **learned** graph (weights = salience, not raw topology) for GQL/SQL
analytics and as the sleep-replay cold tier.

## Produce the artifacts (no credentials needed)

```bash
psyrag --wal mem.wal export-bq --out ./bq_out --dataset psyrag
```

Writes into `./bq_out`:

| file | contents |
|------|----------|
| `nodes.jsonl` | resources (name, type) |
| `edges.jsonl` | weighted edges — `weight` is learned salience at export time, plus temporal validity |
| `traces.jsonl` | durable retrieval traces (what was recalled, provenance) |
| `schema.sql` | table DDL + a `CREATE PROPERTY GRAPH` |
| `queries.gql` | example GQL/SQL analytics |
| `load.sh` | `bq mk` + `bq load` for your project |

## Load into your project

```bash
export GOOGLE_CLOUD_PROJECT=your-project
cd bq_out && bash load.sh psyrag US
bq query --use_legacy_sql=false < schema.sql     # creates the property graph
```

## What you can then ask (see queries.gql)

- **Top learned dependencies** — plain SQL, `ORDER BY weight DESC`.
- **Highest-salience downstream paths from a resource** — GQL variable-length
  `MATCH p = (a)-[e:DependsOn]->{1,3}(b)`; the paths that mattered during real
  incidents float to the top.
- **Where salience concentrates** — per-source share of learned weight, i.e.
  which resources have a single dominant dependency vs. diffuse ones.

## Example: adaptive dependency analysis

A concrete use case: ingest Cloud Asset Inventory snapshots as the dependency
graph, retrieve the blast radius of a resource during an incident, and reward the
paths that led to the actual root cause when it resolves. Over many incidents the
graph learns which dependency edges matter, and that learned structure exports to
BigQuery as a property graph the whole org can query with GQL — for dashboards,
alerting, or capacity planning over salience rather than raw topology.

## Which GCP backend?

BigQuery is the analytics sink shown here (append-only, GQL property graphs). If
you'd rather back the *operational* state — the WAL, plasticity sidecar, or the
durable trace store — **AlloyDB** is a good fit (transactional, low-latency,
Postgres-compatible), and the trace store's NDJSON append model ports to an
AlloyDB table with minimal change. Spanner if you need multi-region. The export
seam here is deliberately simple so any of these can sit behind it.
