//! HTTP service: holds the WAL-backed graph + plasticity sidecar in memory and
//! exposes the coarse per-query / per-cycle surface. This is the loop you drive
//! while iterating; `psyrag monitor` polls `/metrics`.

use std::sync::{Arc, Mutex};

use psyrag_graph::PersistentGraph;
use psyrag_core::{Config, Credit, PlasticityLayer, Spread};
use serde::Deserialize;
use tiny_http::{Method, Response, Server};

use crate::engine::{now_ms, Engine};

/// Self-contained management + visualization UI (served at `/` and `/ui`).
const UI_HTML: &str = include_str!("ui.html");

#[derive(Deserialize)]
struct IngestReq {
    json: String,
    #[serde(default)]
    reconcile: bool,
    #[serde(default)]
    cai: bool,
    ts: Option<i64>,
}
#[derive(Deserialize)]
struct RetrieveReq {
    seeds: Vec<String>,
    depth: Option<u32>,
    fan: Option<f32>,
    #[serde(default = "d_topk")]
    top_k: usize,
    ts: Option<i64>,
    #[serde(default = "d_true")]
    adapt: bool,
    /// Store the trace and return a `trace_id` for deferred feedback.
    #[serde(default)]
    trace: bool,
}
fn d_match_limit() -> usize {
    16
}
fn d_topk() -> usize {
    10
}
fn d_true() -> bool {
    true
}
#[derive(Deserialize)]
struct TouchReq {
    // (src, dst, kind, r)
    edges: Vec<(String, String, String, f32)>,
    ts: Option<i64>,
}
#[derive(Deserialize, Default)]
struct ConsolidateReq {
    ts: Option<i64>,
    #[serde(default)]
    apply_conflicts: bool,
}
/// Feedback in any mode. Provide EITHER `trace_id` (deferred: credit a stored
/// trace) OR `seeds` (stateless: retrieve fresh at `ts`, then credit). Credit
/// spec, in priority order: `nodes` (graded/contrastive) > `reward`+`spread`
/// (episodic) > `used` (explicit hits).
#[derive(Deserialize, Default)]
struct FeedbackReq {
    trace_id: Option<u64>,
    seeds: Option<Vec<String>>,
    used: Option<Vec<String>>,
    nodes: Option<Vec<(String, f32)>>,
    reward: Option<f32>,
    spread: Option<Spread>,
    depth: Option<u32>,
    fan: Option<f32>,
    #[serde(default = "d_topk")]
    top_k: usize,
    ts: Option<i64>,
}

fn json_resp<T: serde::Serialize>(v: &T, code: u16) -> Response<std::io::Cursor<Vec<u8>>> {
    let body = serde_json::to_vec(v).unwrap_or_default();
    Response::from_data(body)
        .with_status_code(code)
        .with_header("Content-Type: application/json".parse::<tiny_http::Header>().unwrap())
}
fn err(msg: &str, code: u16) -> Response<std::io::Cursor<Vec<u8>>> {
    json_resp(&serde_json::json!({ "error": msg }), code)
}

pub fn run(addr: &str, wal: &str, sidecar: &str, cfg: Config) -> Result<(), String> {
    let pg = PersistentGraph::open(wal)?;
    let mut layer = PlasticityLayer::new(cfg);
    layer.load_if_exists(sidecar)?;
    layer.sync(pg.graph());

    let engine = Arc::new(Mutex::new(Engine {
        pg,
        layer,
        sidecar_path: sidecar.to_string(),
        traces: crate::engine::TraceStore::open(4096, &format!("{sidecar}.traces.jsonl")),
    }));

    let server = Server::http(addr).map_err(|e| e.to_string())?;
    eprintln!("psyrag serve on http://{addr}  (wal={wal} sidecar={sidecar})");

    for mut request in server.incoming_requests() {
        let method = request.method().clone();
        let url = request.url().to_string();
        let path = url.split('?').next().unwrap_or("/").to_string();
        let mut body = String::new();
        let _ = request.as_reader().read_to_string(&mut body);

        let resp = handle(&engine, &method, &path, &body);
        let _ = request.respond(resp);
    }
    Ok(())
}

fn handle(
    engine: &Arc<Mutex<Engine>>,
    method: &Method,
    path: &str,
    body: &str,
) -> Response<std::io::Cursor<Vec<u8>>> {
    match (method, path) {
        (Method::Get, "/ui") | (Method::Get, "/ui/") => Response::from_string(UI_HTML)
            .with_status_code(200)
            .with_header("Content-Type: text/html; charset=utf-8".parse::<tiny_http::Header>().unwrap()),
        (Method::Get, "/graph") => {
            let guard = engine.lock().unwrap();
            let e = &*guard;
            let g = e.pg.graph();
            let now = now_ms();
            let limit = 500usize;
            let mut nodes = Vec::new();
            for id in 0..g.node_count() {
                nodes.push(serde_json::json!({
                    "id": id, "name": g.node_name(id as u32), "type": g.node_type(id as u32)
                }));
            }
            let mut edges = Vec::new();
            'outer: for u in 0..g.node_count() {
                for &eid in g.out_edge_ids(u as u32) {
                    let ed = g.edge(eid);
                    let w = e.layer.effective_weight(eid, now);
                    edges.push(serde_json::json!({
                        "source": u, "target": ed.dst, "kind": g.kind_str(ed.kind_id),
                        "weight": w, "open": ed.valid_to == psyrag_graph::graph::T_MAX
                    }));
                    if edges.len() >= limit {
                        break 'outer;
                    }
                }
            }
            json_resp(&serde_json::json!({"nodes": nodes, "edges": edges, "truncated": edges.len() >= limit}), 200)
        }
        (Method::Get, "/traces") => {
            let guard = engine.lock().unwrap();
            let e = &*guard;
            json_resp(&serde_json::json!({"ids": e.traces.ids(), "count": e.traces.len()}), 200)
        }
        (m, p) if *m == Method::Get && p.starts_with("/trace/") => {
            let id: Option<u64> = p.trim_start_matches("/trace/").parse().ok();
            let guard = engine.lock().unwrap();
            let e = &*guard;
            let g = e.pg.graph();
            match id.and_then(|i| e.traces.get(i)) {
                Some(tr) => {
                    let surfaced: Vec<_> = tr.surfaced().iter().map(|(nid, a)| {
                        serde_json::json!({"node": g.node_name(*nid), "activation": a})
                    }).collect();
                    let fired: Vec<_> = tr.fired().iter().map(|(eid, u, v, d)| {
                        serde_json::json!({
                            "src": g.node_name(*u), "dst": g.node_name(*v),
                            "kind": g.kind_str(g.edge(*eid).kind_id), "delta": d
                        })
                    }).collect();
                    json_resp(&serde_json::json!({"t": tr.t(), "surfaced": surfaced, "fired": fired}), 200)
                }
                None => err("unknown trace id", 404),
            }
        }
        (Method::Get, "/health") => json_resp(&serde_json::json!({"ok": true}), 200),
        (Method::Get, "/stats") | (Method::Get, "/metrics") => {
            let guard = engine.lock().unwrap(); let e = &*guard;
            json_resp(&e.layer.stats(e.pg.graph()), 200)
        }
        (Method::Post, "/ingest") => {
            let r: IngestReq = match serde_json::from_str(body) {
                Ok(r) => r,
                Err(e) => return err(&format!("bad body: {e}"), 400),
            };
            let ts = r.ts.unwrap_or_else(now_ms);
            let mut guard = engine.lock().unwrap(); let e = &mut *guard;
            let res = if r.cai {
                #[cfg(feature = "gcp")]
                {
                    e.pg.ingest_cai_snapshot(&r.json, ts)
                }
                #[cfg(not(feature = "gcp"))]
                {
                    Err("built without gcp feature".to_string())
                }
            } else {
                e.pg.ingest_entities(&r.json, ts, r.reconcile)
            };
            match res {
                Ok(stale) => {
                    e.layer.sync(e.pg.graph());
                    json_resp(&serde_json::json!({"stale_retired": stale, "edges": e.pg.graph().edge_count()}), 200)
                }
                Err(msg) => err(&msg, 400),
            }
        }
        (Method::Post, "/retrieve") => {
            let r: RetrieveReq = match serde_json::from_str(body) {
                Ok(r) => r,
                Err(e) => return err(&format!("bad body: {e}"), 400),
            };
            let ts = r.ts.unwrap_or_else(now_ms);
            let mut guard = engine.lock().unwrap(); let e = &mut *guard;
            let seeds: Vec<&str> = r.seeds.iter().map(|s| s.as_str()).collect();
            let depth = r.depth.unwrap_or(e.layer.cfg.depth);
            let fan = r.fan.unwrap_or(e.layer.cfg.fan);
            let result = if r.adapt {
                // retrieve then observe
                let mut res = e.layer.retrieve(e.pg.graph(), &seeds, depth, fan, r.top_k, ts);
                let s = e.layer.observe(res.mass);
                res.lambda_scale = s;
                res
            } else {
                e.layer.retrieve(e.pg.graph(), &seeds, depth, fan, r.top_k, ts)
            };
            if r.trace {
                let (res2, tr) =
                    e.layer.retrieve_traced(e.pg.graph(), &seeds, depth, fan, r.top_k, ts);
                let id = e.traces.put(tr);
                json_resp(&serde_json::json!({"result": res2, "trace_id": id}), 200)
            } else {
                json_resp(&result, 200)
            }
        }
        (Method::Post, "/touch") => {
            let r: TouchReq = match serde_json::from_str(body) {
                Ok(r) => r,
                Err(e) => return err(&format!("bad body: {e}"), 400),
            };
            let ts = r.ts.unwrap_or_else(now_ms);
            let mut guard = engine.lock().unwrap(); let e = &mut *guard;
            let mut touches = Vec::new();
            let mut missed = 0;
            for (s, d, k, rr) in &r.edges {
                match e.layer.edge_id(e.pg.graph(), s, d, k) {
                    Some(eid) => touches.push((eid, *rr)),
                    None => missed += 1,
                }
            }
            e.layer.touch(&touches, ts);
            json_resp(&serde_json::json!({"touched": touches.len(), "missed": missed}), 200)
        }
        (Method::Post, "/feedback") => {
            let r: FeedbackReq = match serde_json::from_str(body) {
                Ok(r) => r,
                Err(e) => return err(&format!("bad body: {e}"), 400),
            };
            let ts = r.ts.unwrap_or_else(now_ms);
            let mut guard = engine.lock().unwrap();
            let e = &mut *guard;
            // build the Credit (nodes > reward > used)
            let hit = e.layer.cfg.feedback_hit;
            let credit = if let Some(nodes) = r.nodes.clone() {
                Credit::Nodes(nodes)
            } else if let Some(reward) = r.reward {
                Credit::Episodic { reward, spread: r.spread.unwrap_or(Spread::ByActivation) }
            } else if let Some(used) = r.used.clone() {
                Credit::Nodes(used.into_iter().map(|n| (n, hit)).collect())
            } else {
                return err("feedback needs one of: nodes, reward, used", 400);
            };
            // get the trace: deferred (trace_id) or stateless (recompute)
            let trace = if let Some(id) = r.trace_id {
                match e.traces.get(id) {
                    Some(t) => t,
                    None => return err("unknown trace_id (evicted or never issued)", 404),
                }
            } else if let Some(seeds) = r.seeds.clone() {
                let sref: Vec<&str> = seeds.iter().map(|s| s.as_str()).collect();
                let depth = r.depth.unwrap_or(e.layer.cfg.depth);
                let fan = r.fan.unwrap_or(e.layer.cfg.fan);
                let (_res, tr) = e.layer.retrieve_traced(e.pg.graph(), &sref, depth, fan, r.top_k, ts);
                tr
            } else {
                return err("feedback needs trace_id or seeds", 400);
            };
            let report = e.layer.apply_credit(e.pg.graph(), &trace, &credit, ts);
            let _ = e.layer.save(&e.sidecar_path);
            json_resp(&report, 200)
        }
        (Method::Post, "/match") => {
            #[derive(Deserialize)]
            struct MatchReq {
                tokens: Vec<String>,
                #[serde(default = "d_match_limit")]
                limit: usize,
            }
            let r: MatchReq = match serde_json::from_str(body) {
                Ok(r) => r,
                Err(e) => return err(&format!("bad body: {e}"), 400),
            };
            let guard = engine.lock().unwrap();
            let e = &*guard;
            let g = e.pg.graph();
            let toks: Vec<String> = r.tokens.iter().map(|t| t.to_lowercase()).collect();
            let mut hits = Vec::new();
            for id in 0..g.node_count() {
                let name = g.node_name(id as u32);
                let lname = name.to_lowercase();
                if toks.iter().any(|t| !t.is_empty() && lname.contains(t.as_str())) {
                    hits.push(name.to_string());
                    if hits.len() >= r.limit {
                        break;
                    }
                }
            }
            json_resp(&serde_json::json!({ "nodes": hits }), 200)
        }
        (Method::Post, "/sleep") => {
            let ts: i64 = serde_json::from_str::<serde_json::Value>(body)
                .ok()
                .and_then(|v| v.get("ts").and_then(|x| x.as_i64()))
                .unwrap_or_else(now_ms);
            let mut guard = engine.lock().unwrap();
            let e = &mut *guard;
            let rep = {
                let g = e.pg.graph();
                e.layer.sleep(g, ts)
            };
            let _ = e.layer.save(&e.sidecar_path);
            json_resp(&rep, 200)
        }
        (Method::Post, "/consolidate") => {
            let r: ConsolidateReq = if body.trim().is_empty() {
                ConsolidateReq::default()
            } else {
                match serde_json::from_str(body) {
                    Ok(r) => r,
                    Err(e) => return err(&format!("bad body: {e}"), 400),
                }
            };
            let ts = r.ts.unwrap_or_else(now_ms);
            let mut guard = engine.lock().unwrap(); let e = &mut *guard;
            let (stats, conflicts) = {
                let g = e.pg.graph();
                e.layer.consolidate(g, ts)
            };
            // Optionally journal supersession ops (a TRUTH change) to the WAL.
            let mut applied = 0;
            if r.apply_conflicts {
                for c in &conflicts {
                    for op in &c.superseded {
                        if e.pg.record_op(op.clone()).is_ok() {
                            applied += 1;
                        }
                    }
                }
                let _ = e.pg.flush();
                e.layer.sync(e.pg.graph());
            }
            let _ = e.layer.save(&e.sidecar_path);
            json_resp(&serde_json::json!({"stats": stats, "conflicts": conflicts, "applied_ops": applied}), 200)
        }
        (Method::Get, "/") => Response::from_string(UI_HTML)
            .with_status_code(200)
            .with_header("Content-Type: text/html; charset=utf-8".parse::<tiny_http::Header>().unwrap()),
        _ => err("not found", 404),
    }
}
