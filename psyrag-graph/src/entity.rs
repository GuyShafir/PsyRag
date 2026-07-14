//! Generic entity ingestion — the domain-agnostic front door.
//!
//! Anything with stable names, types, properties, and typed relationships
//! fits: Kubernetes objects, CMDB records, IoT fleets, service catalogs,
//! org charts, BOMs. One JSON shape (array or NDJSON):
//!
//! ```json
//! {
//!   "name": "sensor/kitchen-motion-1",
//!   "type": "zigbee/MotionSensor",
//!   "props": { "battery": 87, "fw": "1.4.2" },
//!   "edges": [
//!     { "dst": "hub/main", "kind": "PAIRED_WITH" },
//!     { "dst": "automation/kitchen-lights", "kind": "TRIGGERS",
//!       "dst_type": "ha/Automation" }
//!   ]
//! }
//! ```
//!
//! Semantics match the cloud adapter exactly:
//! - The entity asserts its own existence; edge targets become placeholder
//!   nodes (typed by `dst_type` if given) until their own record arrives.
//! - Full-snapshot ingestion reconciles: unasserted nodes and unasserted
//!   edges of re-observed sources are retired.

use crate::graph::{Op, TemporalGraph, Ts};
use crate::snapshot::{ingest_snapshot_ops, Batch, OpSink};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct Entity {
    pub name: String,
    #[serde(rename = "type")]
    pub entity_type: String,
    #[serde(default)]
    pub props: Value,
    #[serde(default)]
    pub edges: Vec<EntityEdge>,
}

#[derive(Debug, Deserialize)]
pub struct EntityEdge {
    pub dst: String,
    pub kind: String,
    #[serde(default)]
    pub dst_type: Option<String>,
}

/// Translate one entity into an op batch.
pub fn entity_ops(e: &Entity, ts: Ts) -> Batch {
    let mut ops = vec![Op::ObserveNode {
        name: e.name.clone(),
        asset_type: e.entity_type.clone(),
        props: e.props.clone(),
        ts,
    }];
    for edge in &e.edges {
        ops.push(Op::ObservePlaceholder {
            name: edge.dst.clone(),
            inferred_type: edge
                .dst_type
                .clone()
                .unwrap_or_else(|| "unknown/unknown".to_string()),
            ts,
        });
        ops.push(Op::ObserveEdge {
            src: e.name.clone(),
            dst: edge.dst.clone(),
            kind: edge.kind.clone(),
            ts,
        });
    }
    (ops, vec![e.name.clone()])
}

/// Parse entities from a JSON array or NDJSON.
pub fn parse_entities(json: &str) -> Result<Vec<Entity>, String> {
    if json.trim_start().starts_with('[') {
        serde_json::from_str(json).map_err(|e| e.to_string())
    } else {
        json.lines()
            .filter(|l| !l.trim().is_empty())
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())
    }
}

/// Ingest entities observed at `ts` into any sink. With `reconcile`, the
/// input is treated as a full snapshot of the domain (zombie pruning + edge
/// reconciliation); without, it's an incremental observation batch.
pub fn ingest_entities<S: OpSink>(
    sink: &mut S,
    json: &str,
    ts: Ts,
    reconcile: bool,
) -> Result<Vec<String>, String> {
    let entities = parse_entities(json)?;
    ingest_snapshot_ops(sink, entities.iter().map(|e| entity_ops(e, ts)), ts, reconcile)
}

/// Convenience for the in-memory graph.
pub fn ingest_entities_mem(
    g: &mut TemporalGraph,
    json: &str,
    ts: Ts,
    reconcile: bool,
) -> Result<Vec<String>, String> {
    ingest_entities(g, json, ts, reconcile)
}
