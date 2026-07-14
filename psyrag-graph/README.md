# psyrag-graph

The temporal typed property graph core of [PsyRag](../README.md).

An **append-only**, **temporal**, **typed** property graph for inventories of
typed entities (cloud resources, Kubernetes objects, CMDB records, service
catalogs). Ingestion is deterministic — no LLM in the ingest path. Nodes and
edges are versioned with validity intervals `[valid_from, valid_to)`, giving
native supersession, diffs, and blast-radius traversal over any past instant.

```rust
use psyrag_graph::{TemporalGraph, Direction};

let mut g = TemporalGraph::new();
// ... ingest entities ...

// What changed in a window?
let d = g.diff(t0, t1);

// If this node breaks, what's affected, and via which chain?
for r in g.blast_radius(node, t1, Direction::Up, 5) {
    println!("{}", r.path);
}
```

This crate owns *truth* (what is, and when). The adaptive **salience** layer —
weighted retrieval, plasticity, feedback, consolidation, sleep — lives in
[`psyrag-core`](../psyrag-core), keyed by this graph's stable edge ids. See the
[main documentation](../docs/concepts.md) for the full model.

Optional `gcp` feature adds Cloud Asset Inventory snapshot ingestion.

License: Apache-2.0.
