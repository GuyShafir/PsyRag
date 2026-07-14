//! psyrag — psyrag-graph plasticity CLI (dependency-free arg parsing).
//!
//! Commands:
//!   psyrag config [--write PATH]
//!   psyrag ingest --file F [--reconcile] [--cai] [--ts MS]
//!   psyrag retrieve --seed N [--seed N...] [--depth D] [--fan F] [--top-k K] [--ts MS] [--adapt]
//!   psyrag touch --edge src,dst,kind,R [--edge ...] [--ts MS]
//!   psyrag feedback --seed N... {--used NODE... | --credit name,score... | --reward R [--spread by_activation|uniform]} [--depth D] [--ts MS]
//!   psyrag consolidate [--ts MS] [--apply-conflicts]
//!   psyrag stats
//!   psyrag serve [--addr HOST:PORT]
//!   psyrag monitor [--url URL] [--interval-ms N]
//! Global: --wal PATH  --sidecar PATH  --config PATH.json

mod args;
mod config;
mod engine;
mod export;
mod metrics;
mod monitor;
mod serve;

use args::Args;
use psyrag_graph::PersistentGraph;
use psyrag_core::PlasticityLayer;
use engine::{now_ms, Engine};

const USAGE: &str = "psyrag <command> [flags]
commands: config ingest retrieve touch feedback consolidate sleep stats export-bq serve monitor
global:   --wal PATH  --sidecar PATH  --config PATH.json
run `psyrag config` to see all tunables.";

fn sidecar_path(a: &Args) -> String {
    a.get("sidecar")
        .map(String::from)
        .unwrap_or_else(|| format!("{}.psyrag.json", a.get_or("wal", "psyrag.wal")))
}

fn open_engine(a: &Args) -> Result<Engine, String> {
    let cfg = config::load(a.get("config"))?;
    let wal = a.get_or("wal", "psyrag.wal");
    let pg = PersistentGraph::open(wal)?;
    let scp = sidecar_path(a);
    let mut layer = PlasticityLayer::new(cfg);
    layer.load_if_exists(&scp)?;
    layer.sync(pg.graph());
    Ok(Engine { pg, layer, sidecar_path: scp, traces: engine::TraceStore::in_memory(4096) })
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let a = Args::parse(argv);
    if let Err(e) = dispatch(&a) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn dispatch(a: &Args) -> Result<(), String> {
    match a.subcommand() {
        Some("config") => {
            if let Some(path) = a.get("write") {
                std::fs::write(path, config::example_json()).map_err(|e| e.to_string())?;
                println!("wrote example config to {path}");
            } else {
                let cfg = config::load(a.get("config"))?;
                println!("{}", serde_json::to_string_pretty(&cfg).unwrap());
            }
        }
        Some("ingest") => {
            let file = a.get("file").ok_or("--file required")?;
            let mut e = open_engine(a)?;
            let json = std::fs::read_to_string(file).map_err(|x| x.to_string())?;
            let t = a.get_i64("ts").unwrap_or_else(now_ms);
            let stale = if a.has("cai") {
                #[cfg(feature = "gcp")]
                { e.pg.ingest_cai_snapshot(&json, t)? }
                #[cfg(not(feature = "gcp"))]
                { return Err("built without gcp feature".into()); }
            } else {
                e.pg.ingest_entities(&json, t, a.has("reconcile"))?
            };
            e.layer.sync(e.pg.graph());
            e.layer.save(&e.sidecar_path)?;
            println!("{}", serde_json::json!({
                "edges": e.pg.graph().edge_count(),
                "nodes": e.pg.graph().node_count(),
                "stale_retired": stale,
            }));
        }
        Some("retrieve") => {
            let seeds = a.get_all("seed");
            if seeds.is_empty() {
                return Err("at least one --seed required".into());
            }
            let mut e = open_engine(a)?;
            let t = a.get_i64("ts").unwrap_or_else(now_ms);
            let seed_refs: Vec<&str> = seeds.iter().map(|s| s.as_str()).collect();
            let d = a.get_u32("depth").unwrap_or(e.layer.cfg.depth);
            let f = a.get_f32("fan").unwrap_or(e.layer.cfg.fan);
            let k = a.get_usize("top-k").unwrap_or(10);
            let res = if a.has("adapt") {
                let mut r = e.layer.retrieve(e.pg.graph(), &seed_refs, d, f, k, t);
                r.lambda_scale = e.layer.observe(r.mass);
                e.layer.save(&e.sidecar_path)?;
                r
            } else {
                e.layer.retrieve(e.pg.graph(), &seed_refs, d, f, k, t)
            };
            println!("{}", serde_json::to_string_pretty(&res).unwrap());
        }
        Some("feedback") => {
            let seeds = a.get_all("seed");
            if seeds.is_empty() {
                return Err("at least one --seed required".into());
            }
            let mut e = open_engine(a)?;
            let t = a.get_i64("ts").unwrap_or_else(now_ms);
            let seed_refs: Vec<&str> = seeds.iter().map(|s| s.as_str()).collect();
            let d = a.get_u32("depth").unwrap_or(e.layer.cfg.depth);
            let f = a.get_f32("fan").unwrap_or(e.layer.cfg.fan);
            let k = a.get_usize("top-k").unwrap_or(10);
            // credit mode: --reward (episodic) | --credit name,score (graded) | --used (explicit)
            let report = if let Some(reward) = a.get_f32("reward") {
                let spread = match a.get_or("spread", "by_activation") {
                    "uniform" => psyrag_core::Spread::Uniform,
                    _ => psyrag_core::Spread::ByActivation,
                };
                e.layer.feedback_reward(e.pg.graph(), &seed_refs, d, f, k, t, reward, spread)
            } else if a.has("credit") {
                let mut nodes = Vec::new();
                for spec in a.get_all("credit") {
                    let p: Vec<&str> = spec.split(',').collect();
                    if p.len() != 2 {
                        return Err(format!("bad --credit '{spec}', want name,score"));
                    }
                    let s: f32 = p[1].parse().map_err(|_| "score must be a number")?;
                    nodes.push((p[0].to_string(), s));
                }
                let (_r, trace) = e.layer.retrieve_traced(e.pg.graph(), &seed_refs, d, f, k, t);
                e.layer.apply_credit(e.pg.graph(), &trace, &psyrag_core::Credit::Nodes(nodes), t)
            } else {
                let used = a.get_all("used");
                let used_refs: Vec<&str> = used.iter().map(|s| s.as_str()).collect();
                e.layer.feedback(e.pg.graph(), &seed_refs, d, f, k, t, &used_refs)
            };
            e.layer.save(&e.sidecar_path)?;
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
        }
        Some("touch") => {
            let specs = a.get_all("edge");
            if specs.is_empty() {
                return Err("at least one --edge src,dst,kind,R required".into());
            }
            let mut e = open_engine(a)?;
            let t = a.get_i64("ts").unwrap_or_else(now_ms);
            let mut touches = Vec::new();
            let mut missed = 0;
            for spec in &specs {
                let p: Vec<&str> = spec.split(',').collect();
                if p.len() != 4 {
                    return Err(format!("bad --edge '{spec}', want src,dst,kind,R"));
                }
                let r: f32 = p[3].parse().map_err(|_| "R must be a number")?;
                match e.layer.edge_id(e.pg.graph(), p[0], p[1], p[2]) {
                    Some(eid) => touches.push((eid, r)),
                    None => missed += 1,
                }
            }
            e.layer.touch(&touches, t);
            e.layer.save(&e.sidecar_path)?;
            println!("{}", serde_json::json!({"touched": touches.len(), "missed": missed}));
        }
        Some("consolidate") => {
            let mut e = open_engine(a)?;
            let t = a.get_i64("ts").unwrap_or_else(now_ms);
            let (stats, conflicts) = {
                let g = e.pg.graph();
                e.layer.consolidate(g, t)
            };
            let mut applied = 0;
            if a.has("apply-conflicts") {
                for c in &conflicts {
                    for op in &c.superseded {
                        if e.pg.record_op(op.clone()).is_ok() {
                            applied += 1;
                        }
                    }
                }
                e.pg.flush()?;
                e.layer.sync(e.pg.graph());
            }
            e.layer.save(&e.sidecar_path)?;
            println!("{}", serde_json::to_string_pretty(&serde_json::json!({
                "stats": stats, "conflicts": conflicts, "applied_ops": applied
            })).unwrap());
        }
        Some("stats") => {
            let e = open_engine(a)?;
            println!("{}", serde_json::to_string_pretty(&e.layer.stats(e.pg.graph())).unwrap());
        }
        Some("sleep") => {
            let mut e = open_engine(a)?;
            let t = a.get_i64("ts").unwrap_or_else(now_ms);
            let rep = {
                let g = e.pg.graph();
                e.layer.sleep(g, t)
            };
            e.layer.save(&e.sidecar_path)?;
            println!("{}", serde_json::to_string_pretty(&rep).unwrap());
        }
        Some("export-bq") => {
            let out = a.get_or("out", "./bq_out").to_string();
            let dataset = a.get_or("dataset", "psyrag").to_string();
            let e = open_engine(a)?;
            let t = a.get_i64("ts").unwrap_or_else(now_ms);
            export::export_bq(&e, &out, t, &dataset)?;
            println!("{}", serde_json::json!({
                "out": out, "dataset": dataset,
                "files": ["nodes.jsonl","edges.jsonl","traces.jsonl","schema.sql","queries.gql","load.sh"]
            }));
        }
        Some("serve") => {
            let cfg = config::load(a.get("config"))?;
            let addr = a.get_or("addr", "0.0.0.0:8080").to_string();
            let wal = a.get_or("wal", "psyrag.wal").to_string();
            serve::run(&addr, &wal, &sidecar_path(a), cfg)?;
        }
        Some("monitor") => {
            let url = a.get_or("url", "http://127.0.0.1:8080").to_string();
            let interval = a.get_i64("interval-ms").unwrap_or(500) as u64;
            monitor::run(&url, interval)?;
        }
        Some(other) => return Err(format!("unknown command '{other}'\n{USAGE}")),
        None => {
            println!("{USAGE}");
        }
    }
    Ok(())
}
