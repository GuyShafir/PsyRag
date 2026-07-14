# driftgraph

**Embeddable temporal typed property graph for any inventory of typed entities.** Git for your system state: append-only, diffable, blast-radius aware.

Most GraphRAG tooling parses unstructured text into fuzzy probabilistic graphs. Deterministic domains — cloud resources, Kubernetes objects, CMDB records, IoT fleets, service catalogs — are the opposite problem: relationships are already strictly typed and defined by the system of record. driftgraph ingests them deterministically — **no LLM in the ingest path** — and gives an agent the three queries that actually matter in an incident:

```rust
use driftgraph::{TemporalGraph, Direction};
use driftgraph::gcp::ingest_snapshot;

let mut g = TemporalGraph::new();
ingest_snapshot(&mut g, &cai_export_t1, t1)?;   // GCP Cloud Asset Inventory JSON
let zombies = ingest_snapshot(&mut g, &cai_export_t2, t2)?; // deleted resources pruned

// 1. What changed in the last 5 minutes?
let d = g.diff(t2 - 300_000, t2);

// 2. If this VPC breaks, what is affected — and via which dependency chain?
for r in g.blast_radius(vpc, t2, Direction::Up, 5) {
    println!("{}", r.path);
    // //run/.../services/api <[REFERENCES]- //compute/.../subnet-a <[REFERENCES]- vpc
}

// 3. What did this look like before the incident?
let node = g.node(g.id_of(name).unwrap());
let then = node.version_at(t_incident - 60_000);
```

## Why not Neo4j / Memgraph / pgvector?

You can build this on those — [Cartography](https://github.com/lyft/cartography) does, and it's good. driftgraph makes three different bets:

1. **Temporal is the product, not a feature.** Every node version and edge carries an `[observed_at, retired_at)` interval. History is never destroyed; `diff` and time-travel are native, not modeled on top of a mutable graph.
2. **Embedded beats client-server at this scale.** A large enterprise org is 10⁵–10⁶ assets. That is not a distributed-systems problem. On 200k nodes / 400k edges: full diff in ~8ms, 1000-node blast radius in ~600µs, in-process, no network hop. Run it inside your agent's Cloud Run container.
3. **Paths are the payload.** `blast_radius` returns the traversal path for every reached node, rendered for direct LLM prompt injection. The model doesn't guess *why* a service is affected — it's told the exact dependency chain.

## What it is not

- Not a DAG store. Cloud graphs have cycles (peering, IAM, PSC); all traversals are cycle-safe.
- Not a query language. Typed traversal primitives are the API; your agent calls them as tools.
- Not a vector database. Inventory queries are traversals and filters. If you need NL→node-type mapping, that's a tiny index that belongs in your app layer.

## Generic entities (any domain)

The universal front door: one JSON shape, array or NDJSON.

```json
{ "name": "sensor/kitchen-motion", "type": "zigbee/MotionSensor",
  "props": { "battery": 87 },
  "edges": [ { "dst": "hub/main", "kind": "PAIRED_WITH" },
             { "dst": "automation/alarm", "kind": "TRIGGERS" } ] }
```

Two ingestion modes with different truth semantics:

- **Snapshot** (`reconcile=true`): the input is a complete picture of the domain. Nodes it didn't assert are retired, and — the subtle one — open edges of *re-observed* sources that weren't re-asserted are retired too. Rewiring a reference (sensor moves from lights to alarm) closes the old edge even though both endpoints stay alive. Sources not in the snapshot keep their edges: partial data is not evidence of absence.
- **Incremental** (`reconcile=false`, or `observe`/`retire`/`link`/`unlink`): event-driven updates, nothing inferred.

Domain adapters are just functions from a domain record to `(Vec<Op>, asserted_names)` — the core (`snapshot.rs`) owns everything after that. The GCP adapter is ~150 lines; write yours in an afternoon.

## GCP adapter (feature `gcp`, default on)

Disable default features for a pure domain-agnostic core with zero cloud code. Feed the adapter Cloud Asset Inventory exports or real-time feed events (JSON array or NDJSON). Edges are extracted deterministically:

- `CONTAINS` — from the CAI `ancestors` chain (org → folder → project → asset).
- `REFERENCES` — any string in `resource.data` that is a full resource name (`//service.googleapis.com/...`) or a compute selfLink URL, normalized. This single rule captures most of GCP's cross-resource wiring (subnet→network, service→subnet, SQL→network, ...) with zero per-type schema code. Referenced-but-not-yet-ingested targets become placeholder nodes, upgraded in place when their real record arrives — traversals work across partial ingestion.

Full-snapshot reconciliation retires everything the snapshot stopped asserting: resources deleted without an event converge on the next snapshot instead of haunting your agent's context window as zombies.

## Persistence (v0.2)

The WAL *is* the database: a NDJSON stream of name-addressed ops. Replaying it reproduces identical state including full temporal history — the natural persistence for an append-only model. Torn tails (crash mid-write) are dropped on replay, never fatal. Reconciliation is journaled by effect (explicit retirement ops), so the log never contains state that depends on replay order.

```rust
let mut pg = PersistentGraph::open("inventory.wal")?;
pg.ingest_cai_snapshot(&cai_json, ts)?;
// process dies, redeploys...
let pg = PersistentGraph::open("inventory.wal")?;  // full history intact
```

## Python (v0.2)

Built with PyO3/maturin. The API is dict-shaped on purpose — register the methods directly as agent tools.

```python
import driftgraph

g = driftgraph.Graph("inventory.wal")          # or Graph() for in-memory
zombies = g.ingest_cai_snapshot(cai_json, ts)  # names of pruned resources
g.diff(t1, t2)                        # {"nodes_added": [...], "nodes_changed": [...], ...}
g.blast_radius(vpc, ts, "up", 5)      # [{"node":..., "type":..., "depth":..., "path":...}]
g.node_at(name, ts_before_incident)   # time travel: {"type":..., "props": {...}}
```

```
pip install maturin
cd driftgraph-py && maturin build --release
```

## Roadmap

- **v0.4** — IAM edge extraction for the GCP adapter (bindings → `CAN_ACT_AS` / `CAN_READ` edges — the traversals that actually find blast radii in security incidents); Kubernetes adapter; WAL segment rotation + compaction.
- **v0.5** — bitemporal upgrade (valid-time axis alongside transaction time); AWS Config adapter.

## License

Apache-2.0
