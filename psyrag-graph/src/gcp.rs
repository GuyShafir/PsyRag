//! GCP Cloud Asset Inventory ingestion.
//!
//! Consumes the CAI asset JSON shape (export or real-time feed):
//!   { "name": "//compute.googleapis.com/projects/p/zones/z/instances/i",
//!     "assetType": "compute.googleapis.com/Instance",
//!     "ancestors": ["projects/123", "folders/456", "organizations/789"],
//!     "resource": { "data": { ... } } }
//!
//! Edge extraction is deterministic — no LLM anywhere in the ingest path:
//! 1. CONTAINS edges from the `ancestors` chain (org -> folder -> project ->
//!    asset). CRM ancestors become nodes themselves (placeholders until their
//!    own CAI records arrive).
//! 2. REFERENCES edges from any string in `resource.data` that is a full
//!    resource name ("//service.googleapis.com/...") or a compute selfLink
//!    URL ("https://www.googleapis.com/compute/v1/..."), normalized to full
//!    resource name form. This one rule covers the bulk of GCP's cross-
//!    resource wiring (subnet->network, instance->subnet, backend->ig, ...)
//!    without per-type schema code. Typed extractors can override later.

use crate::graph::{Op, TemporalGraph, Ts};
use crate::snapshot::ingest_snapshot_ops;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;

pub const KIND_CONTAINS: &str = "CONTAINS";
pub const KIND_REFERENCES: &str = "REFERENCES";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaiAsset {
    pub name: String,
    pub asset_type: String,
    #[serde(default)]
    pub ancestors: Vec<String>,
    #[serde(default)]
    pub resource: Option<CaiResource>,
}

#[derive(Debug, Deserialize)]
pub struct CaiResource {
    #[serde(default)]
    pub data: Value,
}

/// "projects/123" -> ("//cloudresourcemanager.googleapis.com/projects/123",
///                     "cloudresourcemanager.googleapis.com/Project")
fn crm_node(ancestor: &str) -> Option<(String, &'static str)> {
    let t = match ancestor.split('/').next()? {
        "projects" => "cloudresourcemanager.googleapis.com/Project",
        "folders" => "cloudresourcemanager.googleapis.com/Folder",
        "organizations" => "cloudresourcemanager.googleapis.com/Organization",
        _ => return None,
    };
    Some((
        format!("//cloudresourcemanager.googleapis.com/{ancestor}"),
        t,
    ))
}

/// Normalize a string to a full resource name if it looks like one.
fn as_full_resource_name(s: &str) -> Option<String> {
    if s.starts_with("//") && s.contains(".googleapis.com/") {
        return Some(s.to_string());
    }
    // compute selfLink form
    for prefix in [
        "https://www.googleapis.com/compute/v1/",
        "https://compute.googleapis.com/compute/v1/",
    ] {
        if let Some(rest) = s.strip_prefix(prefix) {
            return Some(format!("//compute.googleapis.com/{rest}"));
        }
    }
    None
}

/// Best-effort type inference for placeholder targets:
/// "//compute.googleapis.com/projects/p/global/networks/n"
///   -> "compute.googleapis.com/networks" (collection name; refined when the
///      real asset record arrives and upgrades the placeholder).
fn infer_type(full_name: &str) -> String {
    let body = full_name.trim_start_matches("//");
    let mut parts = body.split('/');
    let service = parts.next().unwrap_or("unknown");
    let segs: Vec<&str> = parts.collect();
    // collection is the second-to-last segment for .../collection/id names
    if segs.len() >= 2 {
        format!("{}/{}", service, segs[segs.len() - 2])
    } else {
        format!("{service}/unknown")
    }
}

fn collect_refs(v: &Value, self_name: &str, out: &mut Vec<String>) {
    match v {
        Value::String(s) => {
            if let Some(full) = as_full_resource_name(s) {
                if full != self_name {
                    out.push(full);
                }
            }
        }
        Value::Array(a) => a.iter().for_each(|x| collect_refs(x, self_name, out)),
        Value::Object(o) => o.values().for_each(|x| collect_refs(x, self_name, out)),
        _ => {}
    }
}

/// Deterministically translate one CAI asset into an op stream. Returns
/// (ops, asserted_names): `asserted` is what this asset claims exists
/// (itself + CRM ancestors), for snapshot reconciliation.
pub fn asset_ops(asset: &CaiAsset, ts: Ts) -> (Vec<Op>, Vec<String>) {
    let mut ops = Vec::new();
    let mut asserted = vec![asset.name.clone()];
    let props = asset
        .resource
        .as_ref()
        .map(|r| r.data.clone())
        .unwrap_or(Value::Null);

    ops.push(Op::ObserveNode {
        name: asset.name.clone(),
        asset_type: asset.asset_type.clone(),
        props: props.clone(),
        ts,
    });

    // 1. Containment chain. CAI ancestors are ordered leaf -> root.
    let mut child = asset.name.clone();
    for anc in &asset.ancestors {
        let Some((anc_name, anc_type)) = crm_node(anc) else { continue };
        if anc_name == asset.name {
            continue; // a project's own CAI record lists itself first
        }
        ops.push(Op::ObservePlaceholder {
            name: anc_name.clone(),
            inferred_type: anc_type.to_string(),
            ts,
        });
        ops.push(Op::ObserveEdge {
            src: anc_name.clone(),
            dst: child,
            kind: KIND_CONTAINS.to_string(),
            ts,
        });
        asserted.push(anc_name.clone());
        child = anc_name;
    }

    // 2. Reference edges out of resource.data.
    let mut refs = Vec::new();
    collect_refs(&props, &asset.name, &mut refs);
    let mut seen: HashSet<String> = HashSet::new();
    for r in refs {
        if !seen.insert(r.clone()) {
            continue;
        }
        ops.push(Op::ObservePlaceholder {
            name: r.clone(),
            inferred_type: infer_type(&r),
            ts,
        });
        ops.push(Op::ObserveEdge {
            src: asset.name.clone(),
            dst: r,
            kind: KIND_REFERENCES.to_string(),
            ts,
        });
    }
    (ops, asserted)
}

/// Ingest one CAI asset observed at `ts`. Returns the node names this asset
/// asserted, for snapshot reconciliation.
pub fn ingest_asset(g: &mut TemporalGraph, asset: &CaiAsset, ts: Ts) -> Vec<String> {
    let (ops, asserted) = asset_ops(asset, ts);
    for op in &ops {
        g.apply(op);
    }
    asserted
}

/// Parse a CAI snapshot (JSON array or NDJSON) into assets.
pub fn parse_snapshot(json: &str) -> Result<Vec<CaiAsset>, String> {
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

/// Ingest a complete snapshot (JSON array or newline-delimited JSON of CAI
/// assets) taken at `ts`, then reconcile: anything alive that the snapshot
/// didn't assert gets retired at `ts`. Returns names of pruned zombies.
/// Ingest a complete CAI snapshot (JSON array or NDJSON) taken at `ts`, with
/// full snapshot-is-truth reconciliation. Returns names of pruned zombies.
pub fn ingest_snapshot(g: &mut TemporalGraph, json: &str, ts: Ts) -> Result<Vec<String>, String> {
    let assets = parse_snapshot(json)?;
    ingest_snapshot_ops(g, assets.iter().map(|a| asset_ops(a, ts)), ts, true)
}
