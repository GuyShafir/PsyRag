# Architecture

## Crates

PsyRag is one product built from three Rust crates plus a Python package:

- **`psyrag-graph`** — the temporal typed property graph core. Append-only,
  WAL-backed, with `TemporalGraph` (in-memory) and `PersistentGraph` (WAL). Owns
  truth. Reusable on its own.
- **`psyrag-core`** — the adaptive memory engine: the plasticity sidecar, weighted
  spreading-activation retrieval, feedback / credit assignment, consolidation,
  sleep, and persistence. Depends on `psyrag-graph`.
- **`psyrag`** — the binary: HTTP server, CLI, and the built-in web console. Deps
  are deliberately minimal (`serde`, `serde_json`, `tiny_http`) so it builds fast
  and offline on any recent Rust toolchain.
- **`python/`** — the ADK integration: a drop-in `BaseMemoryService`, feedback
  adapters, and a zero-dependency client.

The sidecar keys on the graph's **stable, dense `EdgeId`**, which is what makes
plasticity state a set of parallel columns (fast, cache-friendly, vectorizable)
rather than a second graph. It persists as JSON alongside the WAL; since WAL
replay is deterministic, ids line up across restarts.

## Single-process (default)

The whole graph + sidecar live in memory, WAL for durability, plasticity sidecar
and a durable trace log on disk. Retrieval is pointer-chasing over adjacency plus
a vectorized `exp` over the weight column — nanosecond-scale. This is the right
mode until the graph outgrows one machine's memory or you need a shared,
multi-tenant store.

## Tiered: PsyRag as a layer over long-term storage

To scale beyond one process — and to make the store a managed GCP service —
PsyRag becomes a **compute layer over a pluggable backend**. The governing rule,
learned from graph-traversal-over-a-database experience:

> **The backend boundary is per-query / per-cycle, never per-edge.**

Spreading activation is pointer-chasing; round-tripping to a managed database per
hop (milliseconds × thousands of edges) is fatal. So the seam returns whole
subgraphs:

```rust
trait GraphBackend {
    fn append_ops(&mut self, ops: &[Op]);                      // durable write
    fn neighborhood(&self, seeds, depth, at_ts) -> SubGraph;   // ONE call, k-hop temporal subgraph
    fn load_weights(&self, keys: &[EdgeKey]) -> Vec<PlasticityState>;   // batch
    fn flush_weights(&mut self, deltas: &[(EdgeKey, PlasticityState)]); // batch checkpoint
    fn checkpoint(&mut self);                                  // the "sleep" flush hook
}
```

This yields a two-tier system that mirrors biological memory:

- **Working memory** = in-process graph + sidecar. Fast, volatile, bounded. The
  complementary-learning-systems "fast learner."
- **Long-term memory** = the GCP backend. Durable, huge, shared, slower. The
  "slow learner." System of record.
- **Sleep** = the batch job that reconciles them (flush working-memory deltas to
  long-term store; replay the trace log; downscale).

Two access modes behind the trait:

- **(A) Page-in** the working set at scope start (an incident, a user, a project —
  working sets are naturally bounded), do all retrieve/feedback in memory, flush
  at sleep. For high-QPS retrieval.
- **(B) Server-side neighborhood** — each retrieval issues one `neighborhood()`
  query; PsyRag weights the returned subgraph in-process. For agent memory (an
  LLM in the loop → seconds of latency), the single round trip is free.

## GCP backends

Two distinct roles — don't conflate them:

### Long-term memory (system of record) → Spanner Graph

The online, hot path. Why Spanner:

- **Server-side traversal** — native property graph + GQL means `neighborhood()`
  is one variable-length `MATCH`, not k round trips.
- **Locality** — interleaved tables physically co-locate a node's edges (the
  managed equivalent of in-process adjacency).
- **Temporal** — model `[valid_from, valid_to)` as columns; index/partition on
  `valid_to` so the hot set is only open edges.
- **Scale + strong consistency** for a shared multi-tenant store.

PsyRag still runs the plasticity (decay, weighting, homeostasis, sleep) in-process
on the returned subgraph. Spanner does *structure + durability*; PsyRag does
*salience*.

**AlloyDB** is the better home if you'd rather keep the *operational state* (weight
columns, durable trace store) transactional and split from structure.
**pgvector / Vertex Vector Search** is where semantic seed selection plugs in
(embeddings pick entry points, the graph does learned expansion).

### Analytics / replay sink → BigQuery

*Not* the online tier (seconds, not millis). BigQuery is the cold mirror: the
append-only op log and sleep checkpoints land there for GQL/SQL analytics over
the **learned** weights. This is what `psyrag export-bq` produces. See
[gcp/README.md](../gcp/README.md).

## Indexing & optimization

What has to be indexed, and how it maps to a backend:

1. **Current/history split — the dominant temporal optimization.** Hot traversal
   only wants edges *open at now*, but append-only means retired edges accumulate.
   Keep **open edges** in the hot, indexed set; **closed edges** are cold history,
   touched only for time-travel. In Spanner: index on `valid_to` or a table split.
2. **Adjacency locality.** `out_edges(u)` must be one contiguous read — in-process
   a `Vec<Vec<EdgeId>>`, in Spanner interleaved edges. The most important backend
   index choice.
3. **Lazy decay ⇒ no weight index.** State is `(w_last, t_last)`; "weight at now"
   is one `exp` on read. The backend stores two scalars per edge and never indexes
   a time-series of weights. Writes happen only on `touch`.
4. **Seed resolution — where embeddings belong.** The built-in `/match` substring
   scan is O(N) (fine at small scale). At scale: a tokenized text index, **or** a
   vector index for semantic seed selection. The strong hybrid: embeddings pick
   entry-point seeds, the plasticity graph does the learned-salience expansion.
5. **Defer index materialization to sleep.** Live weights mutate on every touch —
   don't index a hot float. Sleep materializes decay and writes an indexed
   snapshot (top-salient per source, weight-clustered edges) for analytics/serving.
   Indexing is a nightly product, not a per-write cost.
6. **Hot layer stays SoA.** In working memory the four plasticity columns are
   parallel arrays indexed by `EdgeId`; decay over N edges is a tight,
   autovectorized `exp` loop. The backend is the durable mirror, not the compute
   substrate.

## Durability

- **WAL** (`<wal>`) — the graph facts (NDJSON of ops), replayed on open.
- **Sidecar** (`<wal>.psyrag.json`) — plasticity weights, decay state, homeostat.
- **Trace log** (`<sidecar>.traces.jsonl`) — durable retrieval traces for deferred
  feedback; bounded FIFO with compaction, survives restarts.

All three live under one directory; mount it as a volume (Docker) or back it with
AlloyDB to persist learned state and pending deferred credit across restarts.
