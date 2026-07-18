//! The tiered-storage seam: `GraphBackend` is the boundary behind which a
//! managed store (Spanner Graph, AlloyDB, ...) can become PsyRag's long-term
//! memory while plasticity compute stays in-process.
//!
//! The governing rule (docs/architecture.md): **the backend boundary is
//! per-query / per-cycle, never per-edge.** Spreading activation is
//! pointer-chasing; a round trip per hop is fatal. So `neighborhood()`
//! returns a whole materialized k-hop subgraph in one call, and weight
//! traffic moves in batches keyed by stable edge keys (which survive both
//! WAL compaction and backend-side re-storage).
//!
//! `InMemoryBackend` is the reference implementation and conformance
//! baseline: a managed-backend implementation is correct exactly when the
//! conformance suite passes against it too. Managed backends remain roadmap
//! — implementing one means taking real client-library dependencies, a
//! deliberate departure from the zero-dep core.

use psyrag_graph::graph::{NodeId, Ts};
use psyrag_graph::{Op, TemporalGraph};
use std::collections::HashMap;

/// Plasticity state for one edge in the long-term store, addressed by the
/// stable edge key (`stable_edge_key`): last materialized weight and its
/// timestamp. Lazy decay means the store never needs a weight time-series.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EdgeState {
    pub w: f32,
    pub t_last: Ts,
}

/// Long-term graph storage seam. All methods are batch/subgraph-granular.
pub trait GraphBackend {
    /// Durably append journal ops (the write path; name-addressed, so the
    /// backend needs no knowledge of process-local ids).
    fn append_ops(&mut self, ops: &[Op]) -> Result<(), String>;

    /// ONE round trip: materialize the k-hop neighborhood of `seeds` as it
    /// was alive at `at_ts`, as a standalone `TemporalGraph` the plasticity
    /// layer can traverse in-process. Nodes/edges outside the horizon are
    /// absent, not masked.
    fn neighborhood(&self, seeds: &[&str], depth: u32, at_ts: Ts) -> Result<TemporalGraph, String>;

    /// Batch-load persisted plasticity state for the given stable edge keys
    /// (absent keys are simply missing from the result).
    fn load_weights(&self, keys: &[u64]) -> Result<HashMap<u64, EdgeState>, String>;

    /// Batch-checkpoint learned deltas (the sleep-time flush).
    fn flush_weights(&mut self, deltas: &[(u64, EdgeState)]) -> Result<(), String>;

    /// Durability barrier: after Ok, everything appended/flushed so far
    /// survives a crash of the backend client.
    fn checkpoint(&mut self) -> Result<(), String>;
}

/// Reference implementation over the embedded graph — the semantics a
/// managed backend must reproduce.
#[derive(Default)]
pub struct InMemoryBackend {
    graph: TemporalGraph,
    weights: HashMap<u64, EdgeState>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn graph(&self) -> &TemporalGraph {
        &self.graph
    }
}

impl GraphBackend for InMemoryBackend {
    fn append_ops(&mut self, ops: &[Op]) -> Result<(), String> {
        for op in ops {
            self.graph.apply(op);
        }
        Ok(())
    }

    fn neighborhood(&self, seeds: &[&str], depth: u32, at_ts: Ts) -> Result<TemporalGraph, String> {
        let g = &self.graph;
        // BFS over edges alive at `at_ts`, collecting member nodes.
        let mut member: Vec<bool> = vec![false; g.node_count()];
        let mut frontier: Vec<NodeId> = Vec::new();
        for s in seeds {
            if let Some(id) = g.id_of(s) {
                if g.node(id).alive_at(at_ts) && !member[id as usize] {
                    member[id as usize] = true;
                    frontier.push(id);
                }
            }
        }
        for _ in 0..depth {
            let mut next = Vec::new();
            for &u in &frontier {
                for &eid in g.out_edge_ids(u) {
                    let e = g.edge(eid);
                    if e.alive_at(at_ts) && g.node(e.dst).alive_at(at_ts) && !member[e.dst as usize]
                    {
                        member[e.dst as usize] = true;
                        next.push(e.dst);
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        // Materialize members + edges among members as a standalone graph,
        // preserving names, types, origins, and validity timestamps (so
        // stable edge keys computed on the subgraph match the store's).
        let mut out = TemporalGraph::new();
        for (id, is_member) in member.iter().enumerate() {
            if !is_member {
                continue;
            }
            let id = id as NodeId;
            let n = g.node(id);
            let Some(v) = n.versions.iter().rev().find(|v| v.alive_covers(at_ts)) else {
                continue;
            };
            let origin = (v.origin_id != 0).then(|| g.origins.resolve(v.origin_id));
            if n.placeholder {
                out.observe_placeholder_from(&n.name, g.node_type(id), v.observed_at, origin);
            } else {
                out.observe_node_from(
                    &n.name,
                    g.node_type(id),
                    v.props_value(),
                    v.observed_at,
                    origin,
                );
            }
        }
        for eid in 0..g.edge_count() {
            let e = g.edge(eid as u32);
            if !e.alive_at(at_ts) || !member[e.src as usize] || !member[e.dst as usize] {
                continue;
            }
            let s = out.id_of(g.node_name(e.src)).unwrap();
            let d = out.id_of(g.node_name(e.dst)).unwrap();
            let origin = (e.origin_id != 0).then(|| g.origins.resolve(e.origin_id).to_string());
            out.observe_edge_from(s, d, g.kind_str(e.kind_id), e.valid_from, origin.as_deref());
        }
        Ok(out)
    }

    fn load_weights(&self, keys: &[u64]) -> Result<HashMap<u64, EdgeState>, String> {
        Ok(keys
            .iter()
            .filter_map(|k| self.weights.get(k).map(|s| (*k, *s)))
            .collect())
    }

    fn flush_weights(&mut self, deltas: &[(u64, EdgeState)]) -> Result<(), String> {
        for (k, s) in deltas {
            self.weights.insert(*k, *s);
        }
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<(), String> {
        Ok(()) // in-memory: nothing to persist
    }
}

/// Small extension so the neighborhood materializer can ask "which version
/// covers t" without duplicating interval logic.
trait VersionCovers {
    fn alive_covers(&self, t: Ts) -> bool;
}
impl VersionCovers for psyrag_graph::graph::NodeVersion {
    fn alive_covers(&self, t: Ts) -> bool {
        self.observed_at <= t && t < self.retired_at
    }
}
