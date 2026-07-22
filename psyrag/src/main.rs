//! psyrag — psyrag-graph plasticity CLI (dependency-free arg parsing).
//!
//! Commands:
//!   psyrag config [--write PATH]
//!   psyrag ingest --file F [--reconcile] [--cai] [--ts MS] [--origin LABEL]
//!   psyrag retrieve --seed N [--seed N...] [--depth D] [--fan F] [--top-k K] [--ts MS] [--adapt]
//!   psyrag touch --edge src,dst,kind,R [--edge ...] [--ts MS]
//!   psyrag feedback --seed N... {--used NODE... | --credit name,score... | --reward R [--spread by_activation|uniform]} [--depth D] [--ts MS]
//!   psyrag consolidate [--ts MS] [--apply-conflicts]
//!   psyrag stats
//!   psyrag checkpoint [--no-archive]        (compact the WAL, archive history)
//!   psyrag verify                           (read-only WAL + sidecar check)
//!   psyrag backup --out DIR                 (consistent file-copy backup)
//!   psyrag purge --origin PREFIX            (drop every fact from a provenance)
//!   psyrag db {list | create NAME}          (requires --data-dir)
//!   psyrag serve [--addr HOST:PORT] [--token T] [--read-token T] [--workers N]
//!                [--max-body-mb N] [--max-open-dbs N] [--log-format json|text]
//!                [--sleep-every D] [--consolidate-every D] [--checkpoint-every D]
//!                [--max-db-mb N] [--max-db-edges N] [--max-mem-mb N]
//!                [--db-token NAME=TOKEN ...] [--max-credit R] [--max-feedback-per-min N]
//!                [--ephemeral-traces]
//!   psyrag monitor [--url URL] [--interval-ms N]
//! Global: --wal PATH  --sidecar PATH  --config PATH.json
//!         --data-dir DIR  --db NAME     (multi-database layout)

mod args;
mod client;
mod config;
mod engine;
mod export;
mod log;
mod metrics;
mod mcp;
mod monitor;
mod prom;
mod serve;

use args::Args;
use engine::{now_ms, Engine};
use psyrag_core::PlasticityLayer;
use psyrag_graph::PersistentGraph;
use std::path::Path;

const USAGE: &str = "psyrag <command> [flags]
commands: config ingest retrieve touch feedback consolidate sleep stats
          checkpoint verify backup purge export-bq db serve standby monitor mcp
standby:  --primary URL [--primary-token T] [--follow-db NAME] [--poll-ms N]
          --wal PATH --addr HOST:PORT   (read-only warm replica of a primary)
global:   --wal PATH  --sidecar PATH  --config PATH.json  --data-dir DIR  --db NAME
run `psyrag config` to see all tunables.";

/// Resolve the (wal, sidecar, per-db-config) paths for the addressed
/// database. `--data-dir DIR [--db NAME]` selects the multi-DB layout
/// `DIR/NAME/{wal,sidecar.json,config.json}`; otherwise the legacy
/// `--wal`/`--sidecar` flags address a single default database.
fn db_paths(a: &Args) -> Result<(String, String, Option<String>), String> {
    match a.get("data-dir") {
        Some(root) => {
            let db = a.get_or("db", serve::DEFAULT_DB);
            if !serve::valid_db_name(db) {
                return Err(format!("invalid --db '{db}' (want [a-z0-9_-]{{1,64}})"));
            }
            let dir = Path::new(root).join(db);
            std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
            let cfg = dir.join("config.json");
            Ok((
                dir.join("wal").to_string_lossy().into_owned(),
                dir.join("sidecar.json").to_string_lossy().into_owned(),
                cfg.is_file().then(|| cfg.to_string_lossy().into_owned()),
            ))
        }
        None => {
            if a.get("db").is_some() {
                return Err("--db requires --data-dir (multi-database layout)".into());
            }
            let wal = a.get_or("wal", "psyrag.wal").to_string();
            let sidecar = a
                .get("sidecar")
                .map(String::from)
                .unwrap_or_else(|| format!("{wal}.psyrag.json"));
            Ok((wal, sidecar, None))
        }
    }
}

/// Parse a human interval: "90s", "30m", "24h", "7d", or bare seconds.
fn parse_every(s: &str) -> Result<std::time::Duration, String> {
    let (num, mult) = match s.chars().last() {
        Some('s') => (&s[..s.len() - 1], 1u64),
        Some('m') => (&s[..s.len() - 1], 60),
        Some('h') => (&s[..s.len() - 1], 3600),
        Some('d') => (&s[..s.len() - 1], 86_400),
        _ => (s, 1),
    };
    let n: u64 = num
        .parse()
        .map_err(|_| format!("bad interval '{s}' (want e.g. 90s, 30m, 24h)"))?;
    if n == 0 {
        return Err(format!("interval '{s}' must be > 0"));
    }
    Ok(std::time::Duration::from_secs(n * mult))
}

pub(crate) fn open_engine(a: &Args) -> Result<Engine, String> {
    let (wal, scp, db_cfg) = db_paths(a)?;
    // Config precedence: explicit --config > the db's config.json > defaults.
    let cfg = config::load(a.get("config").or(db_cfg.as_deref()))?;
    let pg = PersistentGraph::open(&wal)?;
    let mut layer = PlasticityLayer::new(cfg);
    // Bind BEFORE load so a sidecar from a different WAL is refused loudly.
    layer.set_wal_binding(pg.wal_id(), pg.lsn());
    layer.load_if_exists(pg.graph(), &scp)?;
    layer.sync(pg.graph());
    Ok(Engine {
        pg,
        layer,
        sidecar_path: scp,
        traces: engine::TraceStore::in_memory(4096),
        idem: engine::IdemStore::in_memory(4096, serve::IDEM_WINDOW_MS),
        wedged: None,
    })
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
                {
                    e.pg.ingest_cai_snapshot(&json, t)?
                }
                #[cfg(not(feature = "gcp"))]
                {
                    return Err("built without gcp feature".into());
                }
            } else {
                e.pg.ingest_entities_from(&json, t, a.has("reconcile"), a.get("origin"))?
            };
            e.layer.sync(e.pg.graph());
            e.save_sidecar()?;
            println!(
                "{}",
                serde_json::json!({
                    "edges": e.pg.graph().edge_count(),
                    "nodes": e.pg.graph().node_count(),
                    "stale_retired": stale,
                })
            );
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
                e.save_sidecar()?;
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
                e.layer
                    .feedback_reward(e.pg.graph(), &seed_refs, d, f, k, t, reward, spread)
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
                let (_r, trace) = e
                    .layer
                    .retrieve_traced(e.pg.graph(), &seed_refs, d, f, k, t);
                e.layer
                    .apply_credit(e.pg.graph(), &trace, &psyrag_core::Credit::Nodes(nodes), t)
            } else {
                let used = a.get_all("used");
                let used_refs: Vec<&str> = used.iter().map(|s| s.as_str()).collect();
                e.layer
                    .feedback(e.pg.graph(), &seed_refs, d, f, k, t, &used_refs)
            };
            e.save_sidecar()?;
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
            e.save_sidecar()?;
            println!(
                "{}",
                serde_json::json!({"touched": touches.len(), "missed": missed})
            );
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
            e.save_sidecar()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "stats": stats, "conflicts": conflicts, "applied_ops": applied
                }))
                .unwrap()
            );
        }
        Some("stats") => {
            let e = open_engine(a)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&e.layer.stats(e.pg.graph())).unwrap()
            );
        }
        Some("sleep") => {
            let mut e = open_engine(a)?;
            let t = a.get_i64("ts").unwrap_or_else(now_ms);
            let rep = {
                let g = e.pg.graph();
                e.layer.sleep(g, t)
            };
            e.save_sidecar()?;
            println!("{}", serde_json::to_string_pretty(&rep).unwrap());
        }
        Some("export-bq") => {
            let out = a.get_or("out", "./bq_out").to_string();
            let dataset = a.get_or("dataset", "psyrag").to_string();
            let e = open_engine(a)?;
            let t = a.get_i64("ts").unwrap_or_else(now_ms);
            export::export_bq(&e, &out, t, &dataset)?;
            println!(
                "{}",
                serde_json::json!({
                    "out": out, "dataset": dataset,
                    "files": ["nodes.jsonl","edges.jsonl","traces.jsonl","schema.sql","queries.gql","load.sh"]
                })
            );
        }
        Some("purge") => {
            let prefix = a.get("origin").ok_or("--origin PREFIX required")?;
            let mut e = open_engine(a)?;
            let snap = e.layer.snapshot_keys(e.pg.graph());
            let report = e.pg.purge(prefix)?;
            e.layer.restore_keys(e.pg.graph(), &snap);
            e.save_sidecar()?;
            let trace_log = format!("{}.traces.jsonl", e.sidecar_path);
            if Path::new(&trace_log).exists() {
                std::fs::write(&trace_log, b"").map_err(|x| x.to_string())?;
            }
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
        }
        Some("checkpoint") => {
            let mut e = open_engine(a)?;
            let archive = !a.has("no-archive");
            let report = e.pg.compact(archive)?;
            e.save_sidecar()?;
            // The serve-mode trace log (if any) references pre-compaction ids.
            let trace_log = format!("{}.traces.jsonl", e.sidecar_path);
            let mut traces_cleared = false;
            if Path::new(&trace_log).exists() {
                std::fs::write(&trace_log, b"").map_err(|x| x.to_string())?;
                traces_cleared = true;
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "report": report, "traces_cleared": traces_cleared,
                }))
                .unwrap()
            );
        }
        Some("verify") => {
            let (wal, scp, _) = db_paths(a)?;
            let rep = psyrag_graph::persist::verify_wal(&wal)?;
            let g = psyrag_graph::persist::replay_readonly(&wal)?;
            // Sidecar checks: loadability, WAL-lineage binding, and the
            // learning gap (touches/feedback are not journaled, so learning
            // applied after the sidecar's LSN is lost on crash — report it).
            let sidecar_check = if Path::new(&scp).exists() {
                let mut layer = psyrag_core::PlasticityLayer::new(config::load(a.get("config"))?);
                layer.set_wal_binding(rep.wal_id.as_deref(), rep.records as u64);
                match layer.load_if_exists(&g, &scp) {
                    Ok(()) => {
                        let (bound_id, bound_lsn) = match &layer.loaded_binding {
                            Some((id, lsn)) => (Some(id.clone()), Some(*lsn)),
                            None => (None, None),
                        };
                        let gap = bound_lsn.map(|l| (rep.records as u64).saturating_sub(l));
                        serde_json::json!({
                            "loadable": true,
                            "bound_wal_id": bound_id,
                            "as_of_lsn": bound_lsn,
                            "wal_lsn": rep.records,
                            "learning_gap_ops": gap,
                        })
                    }
                    Err(e) => serde_json::json!({"loadable": false, "error": e}),
                }
            } else {
                serde_json::json!({"loadable": null, "note": "no sidecar file"})
            };
            let healthy = rep.corrupt.is_none();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "wal": rep,
                    "graph": {"nodes": g.node_count(), "edges": g.edge_count()},
                    "sidecar": sidecar_check,
                    "healthy": healthy,
                }))
                .unwrap()
            );
            if !healthy {
                return Err("WAL verification failed (mid-file corruption)".into());
            }
        }
        Some("backup") => {
            let out = a.get("out").ok_or("--out DIR required")?;
            let (wal, scp, cfg_path) = db_paths(a)?;
            // Hold the WAL lock (without replaying) so the copies are a
            // consistent set; fails fast if a server owns this database.
            let _lock = psyrag_graph::persist::lock_wal_standalone(&wal)?;
            std::fs::create_dir_all(out).map_err(|x| x.to_string())?;
            let mut copied = Vec::new();
            let trace_log = format!("{scp}.traces.jsonl");
            let mut sources = vec![wal.clone(), scp.clone(), trace_log];
            if let Some(c) = cfg_path {
                sources.push(c);
            }
            for src in sources {
                let p = Path::new(&src);
                if !p.is_file() {
                    continue;
                }
                let name = p.file_name().unwrap().to_string_lossy().into_owned();
                let dst = Path::new(out).join(&name);
                let bytes = std::fs::copy(p, &dst).map_err(|x| format!("copy {src}: {x}"))?;
                copied.push(serde_json::json!({"file": name, "bytes": bytes}));
            }
            let manifest = serde_json::json!({
                "psyrag_backup": 1, "taken_ms": now_ms(), "source_wal": wal, "files": copied,
            });
            std::fs::write(
                Path::new(out).join("manifest.json"),
                serde_json::to_string_pretty(&manifest).unwrap(),
            )
            .map_err(|x| x.to_string())?;
            println!("{}", serde_json::to_string_pretty(&manifest).unwrap());
        }
        Some("db") => {
            let root = a
                .get("data-dir")
                .ok_or("db command requires --data-dir DIR")?;
            match a.positionals.get(1).map(|s| s.as_str()) {
                Some("list") => {
                    let mut rows = Vec::new();
                    if let Ok(rd) = std::fs::read_dir(root) {
                        for e in rd.filter_map(|e| e.ok()) {
                            if !e.path().is_dir() {
                                continue;
                            }
                            let name = e.file_name().to_string_lossy().into_owned();
                            if !serve::valid_db_name(&name) {
                                continue;
                            }
                            let wal_len = std::fs::metadata(e.path().join("wal"))
                                .map(|m| m.len())
                                .unwrap_or(0);
                            rows.push(serde_json::json!({
                                "db": name,
                                "wal_bytes": wal_len,
                                "has_config": e.path().join("config.json").is_file(),
                            }));
                        }
                    }
                    rows.sort_by(|x, y| x["db"].as_str().cmp(&y["db"].as_str()));
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({"dbs": rows})).unwrap()
                    );
                }
                Some("create") => {
                    let name = a
                        .positionals
                        .get(2)
                        .ok_or("usage: psyrag db create NAME --data-dir DIR")?;
                    if !serve::valid_db_name(name) {
                        return Err(format!(
                            "invalid db name '{name}' (want [a-z0-9_-]{{1,64}})"
                        ));
                    }
                    let dir = Path::new(root).join(name);
                    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
                    println!(
                        "{}",
                        serde_json::json!({"created": name, "dir": dir.display().to_string()})
                    );
                }
                _ => return Err("usage: psyrag db {list | create NAME} --data-dir DIR".into()),
            }
        }
        Some("serve") => {
            let cfg = config::load(a.get("config"))?;
            // Loopback by default: exposing the store is an explicit choice
            // (pair a public bind with --token).
            let addr = a.get_or("addr", "127.0.0.1:8080").to_string();
            let wal = a.get_or("wal", "psyrag.wal").to_string();
            let sidecar = a
                .get("sidecar")
                .map(String::from)
                .unwrap_or_else(|| format!("{wal}.psyrag.json"));
            match a.get_or("log-format", "text") {
                "json" => log::set_format(log::Format::Json),
                "text" => log::set_format(log::Format::Text),
                other => return Err(format!("bad --log-format '{other}' (want json|text)")),
            }
            let every = |key: &str| -> Result<Option<std::time::Duration>, String> {
                a.get(key).map(parse_every).transpose()
            };
            let workers = a.get_usize("workers").unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get().min(8))
                    .unwrap_or(4)
            });
            let opts = serve::ServeOpts {
                addr,
                data_dir: a.get("data-dir").map(std::path::PathBuf::from),
                wal,
                sidecar,
                cfg,
                token: a
                    .get("token")
                    .map(String::from)
                    .or_else(|| std::env::var("PSYRAG_TOKEN").ok()),
                read_token: a
                    .get("read-token")
                    .map(String::from)
                    .or_else(|| std::env::var("PSYRAG_READ_TOKEN").ok()),
                max_body: a.get_usize("max-body-mb").unwrap_or(32) * 1024 * 1024,
                workers,
                max_open_dbs: a.get_usize("max-open-dbs").unwrap_or(64),
                sleep_every: every("sleep-every")?,
                consolidate_every: every("consolidate-every")?,
                checkpoint_every: every("checkpoint-every")?,
                max_db_bytes: a.get_usize("max-db-mb").unwrap_or(0) * 1024 * 1024,
                max_db_edges: a.get_usize("max-db-edges").unwrap_or(0),
                max_mem_bytes: a.get_usize("max-mem-mb").unwrap_or(0) * 1024 * 1024,
                db_tokens: {
                    let mut m = std::collections::HashMap::new();
                    for spec in a.get_all("db-token") {
                        let Some((db, tok)) = spec.split_once('=') else {
                            return Err(format!("bad --db-token '{spec}', want NAME=TOKEN"));
                        };
                        if !serve::valid_db_name(db) {
                            return Err(format!("bad --db-token db name '{db}'"));
                        }
                        m.insert(db.to_string(), tok.to_string());
                    }
                    m
                },
                max_credit: a.get_f32("max-credit").unwrap_or(100.0),
                max_feedback_per_min: a.get_u32("max-feedback-per-min").unwrap_or(0),
                ephemeral_traces: a.has("ephemeral-traces"),
                standby: None,
            };
            serve::run(opts)?;
        }
        Some("standby") => {
            // Read-only warm standby: follow a primary's WAL, serve reads.
            let cfg = config::load(a.get("config"))?;
            let addr = a.get_or("addr", "127.0.0.1:8080").to_string();
            let wal = a.get_or("wal", "psyrag-standby.wal").to_string();
            let sidecar = a
                .get("sidecar")
                .map(String::from)
                .unwrap_or_else(|| format!("{wal}.psyrag.json"));
            match a.get_or("log-format", "text") {
                "json" => log::set_format(log::Format::Json),
                "text" => log::set_format(log::Format::Text),
                other => return Err(format!("bad --log-format '{other}' (want json|text)")),
            }
            let primary_url = a
                .get("primary")
                .ok_or("standby needs --primary URL (e.g. http://primary:8080)")?
                .to_string();
            let poll_ms = a.get_usize("poll-ms").unwrap_or(1000).max(100) as u64;
            let opts = serve::ServeOpts {
                addr,
                data_dir: None,
                wal,
                sidecar,
                cfg,
                token: a
                    .get("token")
                    .map(String::from)
                    .or_else(|| std::env::var("PSYRAG_TOKEN").ok()),
                read_token: a
                    .get("read-token")
                    .map(String::from)
                    .or_else(|| std::env::var("PSYRAG_READ_TOKEN").ok()),
                db_tokens: std::collections::HashMap::new(),
                max_body: a.get_usize("max-body-mb").unwrap_or(32) * 1024 * 1024,
                workers: a.get_usize("workers").unwrap_or(4).max(1),
                max_open_dbs: 64,
                sleep_every: None,
                consolidate_every: None,
                checkpoint_every: None,
                max_db_bytes: 0,
                max_db_edges: 0,
                max_mem_bytes: 0,
                max_credit: 100.0,
                max_feedback_per_min: 0,
                ephemeral_traces: false,
                standby: Some(serve::StandbyOpts {
                    primary_url,
                    // The standby authenticates to the primary with its own
                    // token (write/admin scope on the primary's /wal/*).
                    token: a
                        .get("primary-token")
                        .map(String::from)
                        .or_else(|| std::env::var("PSYRAG_PRIMARY_TOKEN").ok()),
                    db: a.get_or("follow-db", "default").to_string(),
                    poll: std::time::Duration::from_millis(poll_ms),
                }),
            };
            serve::run(opts)?;
        }
        Some("monitor") => {
            let url = a.get_or("url", "http://127.0.0.1:8080").to_string();
            let interval = a.get_i64("interval-ms").unwrap_or(500) as u64;
            monitor::run(&url, interval)?;
        }
        Some("mcp") => mcp::run_mcp(a)?,
        Some("mcp-send") => mcp::run_mcp_send(a)?,
        Some(other) => return Err(format!("unknown command '{other}'\n{USAGE}")),
        None => {
            println!("{USAGE}");
        }
    }
    Ok(())
}
