//! Optional GCP backend: export the *learned* graph to BigQuery.
//!
//! psyrag-graph is append-only and temporal; BigQuery is append-only and analytical
//! — a natural sink. This writes the plasticity-weighted graph as BQ-ready NDJSON
//! plus DDL (including a `CREATE PROPERTY GRAPH`), a `bq load` script, and example
//! GQL/SQL. No GCP credentials are needed to produce the artifacts; loading them
//! uses the operator's own project. The pitch for Google CEs: run GQL over the
//! salience the layer *learned*, not just the raw inventory.

use crate::engine::Engine;
use psyrag_graph::graph::T_MAX;
use std::fmt::Write as _;
use std::fs;

pub fn export_bq(e: &Engine, out_dir: &str, ts: i64, dataset: &str) -> Result<(), String> {
    fs::create_dir_all(out_dir).map_err(|x| x.to_string())?;
    let g = e.pg.graph();

    // nodes.jsonl — keyed by name (stable across processes/WAL replay)
    let mut nodes = String::new();
    for id in 0..g.node_count() {
        writeln!(
            nodes,
            "{}",
            serde_json::json!({"name": g.node_name(id as u32), "type": g.node_type(id as u32)})
        )
        .ok();
    }
    fs::write(format!("{out_dir}/nodes.jsonl"), nodes).map_err(|x| x.to_string())?;

    // edges.jsonl — current weighted edges (weight = learned salience at `ts`)
    let mut edges = String::new();
    for u in 0..g.node_count() {
        let src = g.node_name(u as u32);
        for &eid in g.out_edge_ids(u as u32) {
            let ed = g.edge(eid);
            let w = e.layer.effective_weight(eid, ts);
            writeln!(
                edges,
                "{}",
                serde_json::json!({
                    "src": src,
                    "dst": g.node_name(ed.dst),
                    "kind": g.kind_str(ed.kind_id),
                    "weight": w,
                    "open": ed.valid_to == T_MAX,
                    "valid_from": ed.valid_from,
                    "valid_to": if ed.valid_to == T_MAX { serde_json::Value::Null } else { serde_json::json!(ed.valid_to) },
                    "exported_at": ts
                })
            )
            .ok();
        }
    }
    fs::write(format!("{out_dir}/edges.jsonl"), edges).map_err(|x| x.to_string())?;

    // traces.jsonl — the durable feedback traces (provenance of what was recalled)
    let mut traces = String::new();
    for id in e.traces.ids() {
        if let Some(tr) = e.traces.get(id) {
            let surfaced: Vec<_> = tr
                .surfaced()
                .iter()
                .map(|(nid, a)| serde_json::json!({"node": g.node_name(*nid), "activation": a}))
                .collect();
            writeln!(
                traces,
                "{}",
                serde_json::json!({"id": id, "t": tr.t(), "surfaced": surfaced,
                    "edges_fired": tr.edges_fired()})
            )
            .ok();
        }
    }
    fs::write(format!("{out_dir}/traces.jsonl"), traces).map_err(|x| x.to_string())?;

    // schema.sql — tables + a BigQuery property graph over them
    let schema = format!(
        r#"-- BigQuery schema for psyrag-graph-plasticity export.
-- Load the NDJSON files into these tables (see load.sh), then query the
-- property graph with GQL.

CREATE SCHEMA IF NOT EXISTS `{ds}`;

CREATE TABLE IF NOT EXISTS `{ds}.nodes` (
  name STRING NOT NULL,
  type STRING
);

CREATE TABLE IF NOT EXISTS `{ds}.edges` (
  src STRING NOT NULL,
  dst STRING NOT NULL,
  kind STRING,
  weight FLOAT64,        -- LEARNED salience (higher = surfaced-and-useful more often)
  open BOOL,             -- currently valid (not superseded)
  valid_from INT64,      -- transaction time (ms)
  valid_to INT64,        -- NULL while open
  exported_at INT64
);

CREATE TABLE IF NOT EXISTS `{ds}.traces` (
  id INT64,
  t INT64,
  surfaced JSON,
  edges_fired INT64
);

-- Property graph: query learned dependencies with GQL.
CREATE OR REPLACE PROPERTY GRAPH `{ds}.dgp_graph`
  NODE TABLES (
    `{ds}.nodes` KEY (name) LABEL Resource PROPERTIES (name, type)
  )
  EDGE TABLES (
    `{ds}.edges`
      KEY (src, dst, kind)
      SOURCE KEY (src) REFERENCES `{ds}.nodes` (name)
      DESTINATION KEY (dst) REFERENCES `{ds}.nodes` (name)
      LABEL DependsOn PROPERTIES (src, dst, kind, weight, open)
  );
"#,
        ds = dataset
    );
    fs::write(format!("{out_dir}/schema.sql"), schema).map_err(|x| x.to_string())?;

    // queries.gql — example analytics over the learned graph
    let queries = format!(
        r#"-- Example analytics over the LEARNED graph (BigQuery GQL + SQL).

-- 1) Top-20 most salient (learned-useful) dependencies, plain SQL:
SELECT src, kind, dst, weight
FROM `{ds}.edges`
WHERE open
ORDER BY weight DESC
LIMIT 20;

-- 2) GQL: for a failing resource, the highest-salience downstream paths
--    (the dependencies that repeatedly mattered during real incidents):
SELECT dep_path, total_weight
FROM GRAPH_TABLE(`{ds}.dgp_graph`
  MATCH p = (a:Resource {{name: 'api'}})-[e:DependsOn]->{{1,3}}(b:Resource)
  WHERE e.open
  COLUMNS (
    TO_JSON_STRING(ARRAY_AGG(b.name)) AS dep_path,
    SUM(e.weight) AS total_weight
  )
)
ORDER BY total_weight DESC
LIMIT 10;

-- 3) Where is salience concentrating? Per-source share of learned weight:
SELECT src,
       COUNTIF(open) AS live_edges,
       ROUND(MAX(weight), 4) AS top_weight,
       ROUND(SAFE_DIVIDE(MAX(weight), SUM(weight)), 3) AS concentration
FROM `{ds}.edges`
WHERE open
GROUP BY src
ORDER BY top_weight DESC
LIMIT 20;
"#,
        ds = dataset
    );
    fs::write(format!("{out_dir}/queries.gql"), queries).map_err(|x| x.to_string())?;

    // load.sh — create dataset + load tables with your own project
    let load = format!(
        r#"#!/usr/bin/env bash
# Load the exported graph into BigQuery. Requires the `bq` CLI + auth.
#   export GOOGLE_CLOUD_PROJECT=your-project
set -euo pipefail
DS="${{1:-{ds}}}"
LOC="${{2:-US}}"

bq --location="$LOC" mk -f --dataset "$DS" || true
bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect "$DS.nodes"  nodes.jsonl
bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect "$DS.edges"  edges.jsonl
bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect "$DS.traces" traces.jsonl
echo ">> loaded. Create the property graph:"
echo "   bq query --use_legacy_sql=false < schema.sql"
"#,
        ds = dataset
    );
    fs::write(format!("{out_dir}/load.sh"), load).map_err(|x| x.to_string())?;

    Ok(())
}
