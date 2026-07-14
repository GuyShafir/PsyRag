//! Domain-agnostic snapshot ingestion.
//!
//! Any domain adapter (cloud, Kubernetes, CMDB, IoT fleet, org chart, ...)
//! reduces to the same contract: translate each domain record into
//! `(Vec<Op>, asserted_names)`. This module owns everything after that —
//! applying ops through a sink (in-memory or WAL-journaled) and full-snapshot
//! reconciliation for both nodes and edges.
//!
//! Reconciliation semantics ("the snapshot is the truth"):
//! - Node: any live, non-placeholder node the snapshot didn't assert is
//!   retired. (Deletions that produced no event converge — no zombies.)
//! - Edge: edges are owned by their source's observation. If a node was
//!   re-observed in this snapshot, any of its open outgoing edges that the
//!   snapshot didn't re-assert is retired. This catches reference
//!   retargeting where both endpoints stay alive (A moves from B to C: the
//!   A->B edge must close even though B still exists).
//!   Sources NOT observed in this snapshot keep their edges: a partial or
//!   scoped snapshot is not evidence of absence.

use crate::graph::{Op, TemporalGraph, Ts};
use std::collections::HashSet;

/// Where ops go. `TemporalGraph` applies directly; `PersistentGraph`
/// journals to the WAL first.
pub trait OpSink {
    fn record(&mut self, op: Op) -> Result<(), String>;
    fn graph(&self) -> &TemporalGraph;
}

impl OpSink for TemporalGraph {
    fn record(&mut self, op: Op) -> Result<(), String> {
        self.apply(&op);
        Ok(())
    }
    fn graph(&self) -> &TemporalGraph {
        self
    }
}

/// One domain record, translated: the ops to apply and the node names the
/// record asserts exist.
pub type Batch = (Vec<Op>, Vec<String>);

/// Ingest a full snapshot taken at `ts` through `sink`. If `reconcile` is
/// true, applies snapshot-is-truth semantics afterwards. Returns the names
/// of retired (zombie) nodes.
pub fn ingest_snapshot_ops<S: OpSink>(
    sink: &mut S,
    batches: impl IntoIterator<Item = Batch>,
    ts: Ts,
    reconcile: bool,
) -> Result<Vec<String>, String> {
    let mut seen_nodes: HashSet<String> = HashSet::new();
    // Sources genuinely observed (ObserveNode), not just referenced.
    let mut observed_srcs: HashSet<String> = HashSet::new();
    // Edges asserted this snapshot, as (src, dst, kind).
    let mut seen_edges: HashSet<(String, String, String)> = HashSet::new();

    for (ops, asserted) in batches {
        for op in ops {
            match &op {
                Op::ObserveNode { name, .. } => {
                    observed_srcs.insert(name.clone());
                }
                Op::ObserveEdge { src, dst, kind, .. } => {
                    seen_edges.insert((src.clone(), dst.clone(), kind.clone()));
                }
                _ => {}
            }
            sink.record(op)?;
        }
        seen_nodes.extend(asserted);
    }

    if !reconcile {
        return Ok(Vec::new());
    }

    // Edge reconciliation first: retire edges that re-observed sources
    // stopped asserting. (Node retirement below also closes edges, so edge
    // reconciliation must not double-close — retire_edge is a no-op on
    // closed edges, so order is safe either way; doing edges first keeps
    // the journal semantically explicit.)
    let mut stale_edges: Vec<Op> = Vec::new();
    for src in &observed_srcs {
        for (dst, kind) in sink.graph().open_out_edges_named(src) {
            let key = (src.clone(), dst.clone(), kind.clone());
            if !seen_edges.contains(&key) {
                stale_edges.push(Op::RetireEdge {
                    src: src.clone(),
                    dst,
                    kind,
                    ts,
                });
            }
        }
    }
    for op in stale_edges {
        sink.record(op)?;
    }

    // Node reconciliation.
    let stale = sink.graph().stale_nodes(&seen_nodes, ts);
    for name in &stale {
        sink.record(Op::RetireNode {
            name: name.clone(),
            ts,
        })?;
    }
    Ok(stale)
}
