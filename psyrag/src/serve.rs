//! HTTP service: a registry of independent databases (each a WAL-backed graph
//! + plasticity sidecar + durable trace store), served by a small worker pool.
//!
//! Isolation model: every database has its own `RwLock<Engine>`, its own WAL
//! flock, and its own on-disk directory — one DB's ingest or sleep never
//! blocks another DB's retrieval. Routes are `/db/{name}/...`; the bare
//! legacy routes (`/retrieve`, ...) map to the `default` database.
//!
//! Durability contract: a 2xx from a mutating endpoint means the change is on
//! disk (WAL fsynced / sidecar atomically replaced / trace fsynced). Failures
//! to persist are 5xx, never silently swallowed.

use std::collections::HashMap;
use std::io::Read as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use psyrag_core::{Config, Credit, PlasticityLayer, Spread};
use psyrag_graph::PersistentGraph;
use serde::Deserialize;
use tiny_http::{Method, Request, Response, Server};

use crate::engine::{now_ms, Engine, TraceStore};
use crate::log;
use crate::prom;

/// Self-contained management + visualization UI (served at `/` and `/ui`).
/// The console talks to the bare routes, i.e. the `default` database.
const UI_HTML: &str = include_str!("ui.html");

pub const DEFAULT_DB: &str = "default";
const RECV_TICK: Duration = Duration::from_millis(300);

type Resp = Response<std::io::Cursor<Vec<u8>>>;

// ===========================================================================
// Options & registry
// ===========================================================================

pub struct ServeOpts {
    pub addr: String,
    /// Multi-DB root: each database lives in `<data_dir>/<name>/`. None =
    /// legacy single-DB mode using `wal`/`sidecar` below for `default`.
    pub data_dir: Option<PathBuf>,
    pub wal: String,
    pub sidecar: String,
    /// Server-wide default plasticity config; a DB's `config.json` overrides.
    pub cfg: Config,
    /// Bearer token for full access. None (with no read_token) = open mode.
    pub token: Option<String>,
    /// Bearer token restricted to read endpoints.
    pub read_token: Option<String>,
    /// Per-database bearer tokens: full access, but ONLY to that database's
    /// routes (server-level admin denied). db name -> token.
    pub db_tokens: HashMap<String, String>,
    pub max_body: usize,
    pub workers: usize,
    pub max_open_dbs: usize,
    /// Built-in maintenance intervals (None = operator schedules externally).
    pub sleep_every: Option<Duration>,
    pub consolidate_every: Option<Duration>,
    pub checkpoint_every: Option<Duration>,
    /// Per-database growth quotas (0 = unlimited). Exceeding either rejects
    /// further /ingest with 507; maintenance and feedback stay allowed so a
    /// full database can still be consolidated, checkpointed, or purged.
    pub max_db_bytes: usize,
    pub max_db_edges: usize,
    /// Server-wide memory budget over all open DBs (0 = unlimited). Over
    /// budget: idle DBs are evicted; if still over, /ingest sheds with 429.
    pub max_mem_bytes: usize,
    /// Server-side bound on feedback credit magnitudes (|reward| and
    /// per-node |score| are clamped to this; 0 disables). Guards learned
    /// weights against one hostile/buggy client, on top of the layer's
    /// per-edge r_clip.
    pub max_credit: f32,
    /// Per-database /feedback rate limit per minute (0 = off) -> 429.
    pub max_feedback_per_min: u32,
    /// Keep retrieval traces in memory only (nothing trace-derived touches
    /// disk; deferred credit then does not survive restarts).
    pub ephemeral_traces: bool,
}

#[derive(Clone, PartialEq)]
enum Scope {
    Full,
    Read,
    /// Full access, restricted to one named database.
    Db(String),
}
impl Scope {
    fn can_write(&self) -> bool {
        !matches!(self, Scope::Read)
    }
}

/// A JSON response as (status, bytes) — kept in this form so idempotent
/// replays can cache and re-serve it byte-identically.
struct JsonOut {
    code: u16,
    body: Vec<u8>,
}
fn jout<T: serde::Serialize>(v: &T, code: u16) -> JsonOut {
    JsonOut {
        code,
        body: serde_json::to_vec(v).unwrap_or_default(),
    }
}
fn jerr(msg: &str, code: u16) -> JsonOut {
    jout(&serde_json::json!({ "error": msg }), code)
}
impl JsonOut {
    fn into_resp(self, replayed: bool) -> Resp {
        let mut r = Response::from_data(self.body)
            .with_status_code(self.code)
            .with_header(
                "Content-Type: application/json"
                    .parse::<tiny_http::Header>()
                    .unwrap(),
            );
        if replayed {
            r = r.with_header(
                "Idempotency-Replayed: true"
                    .parse::<tiny_http::Header>()
                    .unwrap(),
            );
        }
        r
    }
}

/// Idempotency parameters: replay window, entry cap (both enforced by the
/// durable engine-level `IdemStore`), and the staleness bound for the
/// in-memory in-flight markers that serialize concurrent duplicates.
pub const IDEM_CAP: usize = 4096;
pub const IDEM_WINDOW_MS: i64 = 24 * 3600 * 1000;
const IDEM_INFLIGHT_STALE: Duration = Duration::from_secs(60);

struct Db {
    engine: RwLock<Engine>,
    last_used: Mutex<Instant>,
    /// In-flight idempotency keys (memory-only: an in-flight marker that
    /// died with a crash SHOULD be retryable).
    inflight: Mutex<HashMap<String, Instant>>,
    /// Sliding-minute /feedback counter for the rate limit.
    feedback_window: Mutex<(Instant, u32)>,
    /// Where this DB's config.json lives (multi-DB mode) — quarantine
    /// updates persist there so trust survives restarts.
    cfg_path: Option<PathBuf>,
}

struct Registry {
    opts: ServeOpts,
    dbs: RwLock<HashMap<String, Arc<Db>>>,
    metrics: prom::RequestMetrics,
    started: Instant,
}

pub fn valid_db_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'))
}

fn open_engine_at(
    wal: &str,
    sidecar: &str,
    cfg: Config,
    ephemeral_traces: bool,
) -> Result<Engine, String> {
    let pg = PersistentGraph::open(wal)?;
    let mut layer = PlasticityLayer::new(cfg);
    // Bind BEFORE load so a sidecar from a different WAL is refused loudly.
    layer.set_wal_binding(pg.wal_id(), pg.lsn());
    layer.load_if_exists(pg.graph(), sidecar)?;
    layer.sync(pg.graph());
    let traces = if ephemeral_traces {
        TraceStore::in_memory(4096)
    } else {
        TraceStore::open(4096, &format!("{sidecar}.traces.jsonl"))
    };
    let idem =
        crate::engine::IdemStore::open(IDEM_CAP, IDEM_WINDOW_MS, &format!("{sidecar}.idem.jsonl"));
    Ok(Engine {
        pg,
        layer,
        sidecar_path: sidecar.to_string(),
        traces,
        idem,
        wedged: None,
    })
}

impl Registry {
    /// Resolve a database, lazily opening it. `create` also materializes the
    /// on-disk directory (multi-DB mode). Errors are (status, message).
    fn resolve(&self, name: &str, create: bool) -> Result<Arc<Db>, (u16, String)> {
        if !valid_db_name(name) {
            return Err((
                400,
                format!("invalid db name '{name}' (want [a-z0-9_-]{{1,64}})"),
            ));
        }
        if let Some(db) = self.dbs.read().unwrap().get(name) {
            *db.last_used.lock().unwrap() = Instant::now();
            return Ok(db.clone());
        }
        let mut map = self.dbs.write().unwrap();
        if let Some(db) = map.get(name) {
            *db.last_used.lock().unwrap() = Instant::now();
            return Ok(db.clone());
        }
        // Resolve on-disk paths + per-DB config.
        let mut cfg_path: Option<PathBuf> = None;
        let (wal, sidecar, cfg) = match &self.opts.data_dir {
            None => {
                if name != DEFAULT_DB {
                    return Err((
                        400,
                        "multi-database mode requires the server to run with --data-dir".into(),
                    ));
                }
                (
                    self.opts.wal.clone(),
                    self.opts.sidecar.clone(),
                    self.opts.cfg.clone(),
                )
            }
            Some(root) => {
                let dir = root.join(name);
                if !dir.is_dir() {
                    // `default` is materialized on demand so a bare server
                    // works out of the box; other DBs must be created.
                    if create || name == DEFAULT_DB {
                        std::fs::create_dir_all(&dir)
                            .map_err(|e| (500, format!("create {}: {e}", dir.display())))?;
                    } else {
                        return Err((
                            404,
                            format!("unknown db '{name}' (create it with POST /db/{name})"),
                        ));
                    }
                }
                let cp = dir.join("config.json");
                let cfg = if cp.is_file() {
                    crate::config::load(Some(cp.to_str().unwrap_or_default()))
                        .map_err(|e| (500, format!("db '{name}' config: {e}")))?
                } else {
                    self.opts.cfg.clone()
                };
                cfg_path = Some(cp);
                (
                    dir.join("wal").to_string_lossy().into_owned(),
                    dir.join("sidecar.json").to_string_lossy().into_owned(),
                    cfg,
                )
            }
        };
        // Respect the open-DB cap: evict the least-recently-used idle entry.
        if map.len() >= self.opts.max_open_dbs {
            let victim = map
                .iter()
                .filter(|(_, db)| Arc::strong_count(db) == 1) // idle: only the map holds it
                .min_by_key(|(_, db)| *db.last_used.lock().unwrap())
                .map(|(k, _)| k.clone());
            match victim {
                Some(k) => {
                    // Dropping the Arc closes the engine: WAL flock released,
                    // buffered records flushed by PersistentGraph::drop.
                    map.remove(&k);
                }
                None => {
                    return Err((
                        503,
                        format!(
                            "open database limit reached ({}) and none are idle",
                            self.opts.max_open_dbs
                        ),
                    ));
                }
            }
        }
        let engine = open_engine_at(&wal, &sidecar, cfg, self.opts.ephemeral_traces)
            .map_err(|e| (503, e))?;
        let db = Arc::new(Db {
            engine: RwLock::new(engine),
            last_used: Mutex::new(Instant::now()),
            inflight: Mutex::new(HashMap::new()),
            feedback_window: Mutex::new((Instant::now(), 0)),
            cfg_path,
        });
        map.insert(name.to_string(), db.clone());
        Ok(db)
    }

    /// Authenticate a request. Open mode (no tokens configured) grants Full.
    fn auth(&self, req: &Request) -> Result<Scope, Resp> {
        if self.opts.token.is_none()
            && self.opts.read_token.is_none()
            && self.opts.db_tokens.is_empty()
        {
            return Ok(Scope::Full);
        }
        let presented = req
            .headers()
            .iter()
            .find(|h| h.field.equiv("Authorization"))
            .map(|h| h.value.as_str().to_string())
            .and_then(|v| v.strip_prefix("Bearer ").map(String::from));
        let Some(presented) = presented else {
            return Err(err("missing Authorization: Bearer token", 401));
        };
        if self
            .opts
            .token
            .as_deref()
            .is_some_and(|t| ct_eq(t, &presented))
        {
            return Ok(Scope::Full);
        }
        if self
            .opts
            .read_token
            .as_deref()
            .is_some_and(|t| ct_eq(t, &presented))
        {
            return Ok(Scope::Read);
        }
        for (db, tok) in &self.opts.db_tokens {
            if ct_eq(tok, &presented) {
                return Ok(Scope::Db(db.clone()));
            }
        }
        Err(err("invalid token", 401))
    }

    /// Estimated bytes for one engine: graph structures + sidecar columns.
    fn engine_bytes(e: &Engine) -> usize {
        e.pg.graph().approx_bytes() + e.pg.graph().edge_count() * 33
    }

    /// Sum the estimate over open DBs (skipping write-locked ones).
    fn open_bytes(&self) -> usize {
        self.dbs
            .read()
            .unwrap()
            .values()
            .filter_map(|db| db.engine.try_read().ok().map(|e| Self::engine_bytes(&e)))
            .sum()
    }

    /// Evict idle (unreferenced) DBs, least-recently-used first, until the
    /// server estimate is at or under `budget`. Returns evicted names.
    fn evict_until(&self, budget: usize) -> Vec<String> {
        let mut evicted = Vec::new();
        loop {
            if self.open_bytes() <= budget {
                break;
            }
            let mut map = self.dbs.write().unwrap();
            let victim = map
                .iter()
                .filter(|(_, db)| Arc::strong_count(db) == 1)
                .min_by_key(|(_, db)| *db.last_used.lock().unwrap())
                .map(|(k, _)| k.clone());
            match victim {
                Some(k) => {
                    map.remove(&k);
                    evicted.push(k);
                }
                None => break, // nothing idle left to evict
            }
        }
        evicted
    }

    /// Flush every open database: WAL fsync + sidecar snapshot. Used at
    /// graceful shutdown; failures are reported, not ignored.
    fn flush_all(&self) {
        let map = self.dbs.read().unwrap();
        for (name, db) in map.iter() {
            let mut e = db.engine.write().unwrap();
            if let Err(er) = e.pg.flush() {
                log::error(
                    "shutdown_flush_failed",
                    serde_json::json!({"db": name, "error": er}),
                );
            }
            if let Err(er) = e.save_sidecar() {
                log::error(
                    "shutdown_sidecar_failed",
                    serde_json::json!({"db": name, "error": er}),
                );
            }
        }
    }
}

/// Constant-time string equality (token comparison).
fn ct_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

// ===========================================================================
// Request DTOs (unchanged wire shapes)
// ===========================================================================

#[derive(Deserialize)]
struct IngestReq {
    json: String,
    #[serde(default)]
    reconcile: bool,
    #[serde(default)]
    cai: bool,
    ts: Option<i64>,
    /// Provenance label for every fact in this batch (per-entity `origin`
    /// in the payload overrides it). Conventions like
    /// "user:alice/session:42" enable trust levels and purge-by-subject.
    #[serde(default)]
    origin: Option<String>,
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
    /// Return the activation paths (fired edges) alongside the result.
    /// Read-only; allowed under the read scope.
    #[serde(default)]
    explain: bool,
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

fn json_resp<T: serde::Serialize>(v: &T, code: u16) -> Resp {
    let body = serde_json::to_vec(v).unwrap_or_default();
    Response::from_data(body)
        .with_status_code(code)
        .with_header(
            "Content-Type: application/json"
                .parse::<tiny_http::Header>()
                .unwrap(),
        )
}
fn err(msg: &str, code: u16) -> Resp {
    json_resp(&serde_json::json!({ "error": msg }), code)
}
fn html(body: &str) -> Resp {
    Response::from_string(body)
        .with_status_code(200)
        .with_header(
            "Content-Type: text/html; charset=utf-8"
                .parse::<tiny_http::Header>()
                .unwrap(),
        )
}

// ===========================================================================
// Shutdown signal (zero-dependency SIGINT/SIGTERM handler)
// ===========================================================================

static STOP: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
fn install_signal_handlers() {
    extern "C" {
        fn signal(signum: i32, handler: extern "C" fn(i32)) -> usize;
    }
    extern "C" fn on_sig(_s: i32) {
        // Async-signal-safe: a store on a static atomic.
        STOP.store(true, Ordering::SeqCst);
    }
    const SIGINT: i32 = 2;
    const SIGTERM: i32 = 15;
    unsafe {
        signal(SIGINT, on_sig);
        signal(SIGTERM, on_sig);
    }
}
#[cfg(not(unix))]
fn install_signal_handlers() {}

// ===========================================================================
// Server loop
// ===========================================================================

pub fn run(opts: ServeOpts) -> Result<(), String> {
    let workers = opts.workers.max(1);
    let addr = opts.addr.clone();
    let reg = Arc::new(Registry {
        opts,
        dbs: RwLock::new(HashMap::new()),
        metrics: prom::RequestMetrics::new(),
        started: Instant::now(),
    });
    // Eager-open the default DB: startup fails loudly on a locked/corrupt
    // WAL instead of surfacing it on the first request.
    reg.resolve(DEFAULT_DB, true).map_err(|(_, m)| m)?;

    let server = Arc::new(Server::http(&addr).map_err(|e| e.to_string())?);
    install_signal_handlers();

    let loopback =
        addr.starts_with("127.") || addr.starts_with("localhost") || addr.starts_with("[::1]");
    if !loopback && reg.opts.token.is_none() && reg.opts.read_token.is_none() {
        log::warn(
            "open_bind",
            serde_json::json!({
                "addr": addr,
                "msg": "serving without --token: anyone who can reach this port has full read/write access",
            }),
        );
    }
    log::info(
        "serve_start",
        serde_json::json!({
            "addr": addr,
            "workers": workers,
            "data_dir": reg.opts.data_dir.as_ref().map(|p| p.display().to_string()),
            "wal": if reg.opts.data_dir.is_none() { Some(reg.opts.wal.clone()) } else { None },
            "auth": reg.opts.token.is_some() || reg.opts.read_token.is_some(),
        }),
    );
    // Console-friendly one-liner regardless of log format.
    eprintln!("psyrag serve on http://{addr}");

    // Built-in maintenance: one scheduler thread runs due tasks against
    // every open, non-wedged database (write lock per db per task).
    let maintenance = {
        let reg = reg.clone();
        std::thread::spawn(move || maintenance_loop(&reg))
    };

    let mut handles = Vec::new();
    for _ in 0..workers {
        let server = server.clone();
        let reg = reg.clone();
        handles.push(std::thread::spawn(move || loop {
            if STOP.load(Ordering::SeqCst) {
                break;
            }
            match server.recv_timeout(RECV_TICK) {
                Ok(Some(request)) => handle_request(&reg, request),
                Ok(None) => continue,
                Err(_) => {
                    if STOP.load(Ordering::SeqCst) {
                        break;
                    }
                }
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    let _ = maintenance.join();
    log::info("serve_drain", serde_json::json!({}));
    reg.flush_all();
    log::info(
        "serve_stop",
        serde_json::json!({"uptime_s": reg.started.elapsed().as_secs()}),
    );
    Ok(())
}

/// Run scheduled sleep/consolidate/checkpoint against every open database.
/// Ticks coarsely; a task fires when its interval has elapsed since its last
/// firing (first firing is one full interval after startup).
fn maintenance_loop(reg: &Arc<Registry>) {
    let tasks: Vec<(&str, Duration)> = [
        ("consolidate", reg.opts.consolidate_every),
        ("sleep", reg.opts.sleep_every),
        ("checkpoint", reg.opts.checkpoint_every),
    ]
    .into_iter()
    .filter_map(|(n, d)| d.map(|d| (n, d)))
    .collect();
    if tasks.is_empty() {
        return;
    }
    let mut last: HashMap<&str, Instant> =
        tasks.iter().map(|(n, _)| (*n, Instant::now())).collect();
    loop {
        if STOP.load(Ordering::SeqCst) {
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
        for (name, every) in &tasks {
            if last[name].elapsed() < *every {
                continue;
            }
            last.insert(name, Instant::now());
            let dbs: Vec<(String, Arc<Db>)> = reg
                .dbs
                .read()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            for (db_name, db) in dbs {
                let mut guard = db.engine.write().unwrap();
                let e = &mut *guard;
                if e.wedged.is_some() {
                    log::warn(
                        "maintenance_skip",
                        serde_json::json!({"db": db_name, "task": name, "reason": "wedged"}),
                    );
                    continue;
                }
                let ts = now_ms();
                let outcome: Result<serde_json::Value, String> = match *name {
                    "consolidate" => {
                        let (st, _conflicts) = {
                            let g = e.pg.graph();
                            e.layer.consolidate(g, ts)
                        };
                        e.save_sidecar()
                            .map(|_| serde_json::to_value(st).unwrap_or_default())
                    }
                    "sleep" => {
                        let rep = {
                            let g = e.pg.graph();
                            e.layer.sleep(g, ts)
                        };
                        e.save_sidecar()
                            .map(|_| serde_json::to_value(rep).unwrap_or_default())
                    }
                    "checkpoint" => e.pg.compact(true).and_then(|rep| {
                        e.save_sidecar()?;
                        e.traces.clear()?;
                        Ok(serde_json::to_value(rep).unwrap_or_default())
                    }),
                    _ => unreachable!(),
                };
                match outcome {
                    Ok(rep) => log::info(
                        "maintenance",
                        serde_json::json!({"db": db_name, "task": name, "report": rep}),
                    ),
                    Err(er) => {
                        if *name == "checkpoint" {
                            e.wedge(&er);
                        }
                        log::error(
                            "maintenance_failed",
                            serde_json::json!({"db": db_name, "task": name, "error": er}),
                        );
                    }
                }
            }
        }
    }
}

fn handle_request(reg: &Arc<Registry>, mut request: Request) {
    let started = Instant::now();
    let method = request.method().clone();
    let url = request.url().to_string();
    let path = url.split('?').next().unwrap_or("/").to_string();

    // Unauthenticated surface: health probes + the static UI shell.
    let resp = match (&method, path.as_str()) {
        (Method::Get, "/health") | (Method::Get, "/live") => {
            json_resp(&serde_json::json!({"ok": true}), 200)
        }
        // Readiness: the default DB opened at startup, so serving == ready.
        (Method::Get, "/ready") => json_resp(&serde_json::json!({"ready": true}), 200),
        (Method::Get, "/") | (Method::Get, "/ui") | (Method::Get, "/ui/") => html(UI_HTML),
        _ => match reg.auth(&request) {
            Err(resp) => resp,
            Ok(scope) => {
                let idem_key = request
                    .headers()
                    .iter()
                    .find(|h| h.field.equiv("Idempotency-Key"))
                    .map(|h| h.value.as_str().to_string());
                // Bounded body read (413 on oversize) for methods that carry one.
                let body = if matches!(method, Method::Post | Method::Put | Method::Delete) {
                    match read_body(&mut request, reg.opts.max_body) {
                        Ok(b) => b,
                        Err(resp) => {
                            let _ = request.respond(resp);
                            return;
                        }
                    }
                } else {
                    String::new()
                };
                route(reg, scope, &method, &path, &body, idem_key.as_deref())
            }
        },
    };
    let status = resp.status_code().0;
    let dur = started.elapsed();
    // Metrics + request log use the db-relative route so /db/{x}/retrieve
    // and /retrieve share a class (bounded label cardinality).
    let (db_name, db_route) = split_route(&path);
    reg.metrics.record(prom::classify(&db_route), status, dur);
    let resp = resp.with_header("X-PsyRag-Api: 1".parse::<tiny_http::Header>().unwrap());
    let _ = request.respond(resp);
    log::info(
        "request",
        serde_json::json!({
            "method": method.to_string(),
            "path": path,
            "db": db_name,
            "status": status,
            "ms": (dur.as_secs_f64() * 1000.0 * 1000.0).round() / 1000.0,
        }),
    );
}

fn read_body(request: &mut Request, max: usize) -> Result<String, Resp> {
    if let Some(len) = request.body_length() {
        if len > max {
            return Err(err(&format!("body too large ({len} > {max} bytes)"), 413));
        }
    }
    let mut body = String::new();
    request
        .as_reader()
        .take(max as u64 + 1)
        .read_to_string(&mut body)
        .map_err(|e| err(&format!("read body: {e}"), 400))?;
    if body.len() > max {
        return Err(err(&format!("body too large (> {max} bytes)"), 413));
    }
    Ok(body)
}

/// Split a path into (db name, db-relative route). Bare routes address the
/// default DB; `/db/{name}` itself yields an empty route (create/drop/info).
fn split_route(path: &str) -> (String, String) {
    if let Some(rest) = path.strip_prefix("/db/") {
        match rest.split_once('/') {
            Some((name, tail)) => {
                let tail = tail.trim_end_matches('/');
                (
                    name.to_string(),
                    if tail.is_empty() {
                        String::new()
                    } else {
                        format!("/{tail}")
                    },
                )
            }
            None => (rest.trim_end_matches('/').to_string(), String::new()),
        }
    } else {
        (DEFAULT_DB.to_string(), path.to_string())
    }
}

fn route(
    reg: &Arc<Registry>,
    scope: Scope,
    method: &Method,
    path: &str,
    body: &str,
    idem_key: Option<&str>,
) -> Resp {
    // Server-level admin routes (denied to db-scoped tokens).
    if path == "/dbs" && *method == Method::Get {
        if matches!(scope, Scope::Db(_)) {
            return err("this token is database-scoped", 403);
        }
        return list_dbs(reg);
    }
    if path == "/metrics" && *method == Method::Get {
        if matches!(scope, Scope::Db(_)) {
            return err("this token is database-scoped", 403);
        }
        return prometheus_metrics(reg);
    }
    let (name, db_route) = split_route(path);
    // A db-scoped token is confined to its own database's routes; the
    // server-level surface (/dbs, /metrics, db create/drop) is denied.
    if let Scope::Db(allowed) = &scope {
        if *allowed != name || db_route.is_empty() {
            return err(
                &format!("this token is scoped to database '{allowed}'"),
                403,
            );
        }
    }

    // /db/{name} itself: create / drop.
    if db_route.is_empty() {
        return match method {
            Method::Post => {
                if scope != Scope::Full {
                    return err("write scope required", 403);
                }
                if reg.opts.data_dir.is_none() {
                    return err(
                        "creating databases requires the server to run with --data-dir",
                        400,
                    );
                }
                match reg.resolve(&name, true) {
                    Ok(_) => json_resp(&serde_json::json!({"db": name, "ready": true}), 200),
                    Err((code, msg)) => err(&msg, code),
                }
            }
            Method::Delete => drop_db(reg, scope, &name),
            _ => err("use POST to create or DELETE to drop a database", 405),
        };
    }

    match reg.resolve(&name, false) {
        Ok(db) => handle_db(reg, &db, scope, method, &db_route, body, idem_key),
        Err((code, msg)) => err(&msg, code),
    }
}

/// Prometheus exposition: request counters/histograms plus per-database
/// state gauges sampled at scrape time. Busy databases (write-locked) are
/// skipped for gauges rather than blocking the scrape.
fn prometheus_metrics(reg: &Arc<Registry>) -> Resp {
    let mut out = String::with_capacity(4096);
    out.push_str("# TYPE psyrag_uptime_seconds gauge\n");
    out.push_str(&format!(
        "psyrag_uptime_seconds {}\n",
        reg.started.elapsed().as_secs()
    ));
    reg.metrics.render(&mut out);
    let dbs = reg.dbs.read().unwrap();
    out.push_str(&format!(
        "# TYPE psyrag_open_dbs gauge\npsyrag_open_dbs {}\n",
        dbs.len()
    ));
    out.push_str("# TYPE psyrag_db_nodes gauge\n# TYPE psyrag_db_edges_live gauge\n# TYPE psyrag_db_edges_dead gauge\n# TYPE psyrag_db_lambda_scale gauge\n# TYPE psyrag_db_ewma_mass gauge\n# TYPE psyrag_db_traces gauge\n# TYPE psyrag_db_wedged gauge\n# TYPE psyrag_db_wal_lsn gauge\n# TYPE psyrag_db_approx_bytes gauge\n");
    if reg.opts.max_mem_bytes > 0 {
        out.push_str(&format!(
            "# TYPE psyrag_mem_budget_bytes gauge\npsyrag_mem_budget_bytes {}\n",
            reg.opts.max_mem_bytes
        ));
    }
    for (name, db) in dbs.iter() {
        let Ok(e) = db.engine.try_read() else {
            continue;
        };
        let st = e.layer.stats(e.pg.graph());
        out.push_str(&format!("psyrag_db_nodes{{db=\"{name}\"}} {}\n", st.nodes));
        out.push_str(&format!(
            "psyrag_db_edges_live{{db=\"{name}\"}} {}\n",
            st.edges_live
        ));
        out.push_str(&format!(
            "psyrag_db_edges_dead{{db=\"{name}\"}} {}\n",
            st.edges_dead
        ));
        out.push_str(&format!(
            "psyrag_db_lambda_scale{{db=\"{name}\"}} {}\n",
            st.lambda_scale
        ));
        out.push_str(&format!(
            "psyrag_db_ewma_mass{{db=\"{name}\"}} {}\n",
            st.ewma_mass
        ));
        out.push_str(&format!(
            "psyrag_db_traces{{db=\"{name}\"}} {}\n",
            e.traces.len()
        ));
        out.push_str(&format!(
            "psyrag_db_wedged{{db=\"{name}\"}} {}\n",
            u8::from(e.wedged.is_some())
        ));
        out.push_str(&format!(
            "psyrag_db_wal_lsn{{db=\"{name}\"}} {}\n",
            e.pg.lsn()
        ));
        out.push_str(&format!(
            "psyrag_db_approx_bytes{{db=\"{name}\"}} {}\n",
            Registry::engine_bytes(&e)
        ));
    }
    Response::from_string(out)
        .with_status_code(200)
        .with_header(
            "Content-Type: text/plain; version=0.0.4; charset=utf-8"
                .parse::<tiny_http::Header>()
                .unwrap(),
        )
}

/// Enforce per-DB quotas (507) and the server memory budget (429, after
/// attempting idle eviction). None = capacity available.
fn check_capacity(reg: &Arc<Registry>, db: &Arc<Db>) -> Option<Resp> {
    let (bytes, edges) = {
        let e = db.engine.read().unwrap();
        (Registry::engine_bytes(&e), e.pg.graph().edge_count())
    };
    let o = &reg.opts;
    if o.max_db_bytes > 0 && bytes >= o.max_db_bytes {
        return Some(err(
            &format!("database over size quota ({bytes} >= {} bytes); checkpoint/purge to reclaim, or raise --max-db-mb", o.max_db_bytes),
            507,
        ));
    }
    if o.max_db_edges > 0 && edges >= o.max_db_edges {
        return Some(err(
            &format!("database over edge quota ({edges} >= {}); checkpoint/purge to reclaim, or raise --max-db-edges", o.max_db_edges),
            507,
        ));
    }
    if o.max_mem_bytes > 0 && reg.open_bytes() >= o.max_mem_bytes {
        let evicted = reg.evict_until(o.max_mem_bytes);
        if !evicted.is_empty() {
            log::warn("budget_eviction", serde_json::json!({"evicted": evicted}));
        }
        if reg.open_bytes() >= o.max_mem_bytes {
            log::warn(
                "load_shed",
                serde_json::json!({"used": reg.open_bytes(), "budget": o.max_mem_bytes}),
            );
            return Some(err(
                &format!("server over memory budget ({} >= {} bytes) and no idle databases to evict; ingest shed", reg.open_bytes(), o.max_mem_bytes),
                429,
            ));
        }
    }
    None
}

fn list_dbs(reg: &Arc<Registry>) -> Resp {
    let open = reg.dbs.read().unwrap();
    let mut names: Vec<String> = match &reg.opts.data_dir {
        Some(root) => std::fs::read_dir(root)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.path().is_dir())
                    .filter_map(|e| e.file_name().into_string().ok())
                    .filter(|n| valid_db_name(n))
                    .collect()
            })
            .unwrap_or_default(),
        None => vec![DEFAULT_DB.to_string()],
    };
    names.sort();
    names.dedup();
    for n in open.keys() {
        if !names.contains(n) {
            names.push(n.clone());
        }
    }
    let rows: Vec<serde_json::Value> = names
        .iter()
        .map(|n| match open.get(n) {
            Some(db) => match db.engine.try_read() {
                Ok(e) => serde_json::json!({
                    "db": n, "open": true,
                    "nodes": e.pg.graph().node_count(),
                    "edges": e.pg.graph().edge_count(),
                    "traces": e.traces.len(),
                    "idem_entries": e.idem.len(),
                    "wedged": e.wedged,
                    "approx_bytes": Registry::engine_bytes(&e),
                }),
                Err(_) => serde_json::json!({"db": n, "open": true, "busy": true}),
            },
            None => serde_json::json!({"db": n, "open": false}),
        })
        .collect();
    json_resp(&serde_json::json!({"dbs": rows}), 200)
}

fn drop_db(reg: &Arc<Registry>, scope: Scope, name: &str) -> Resp {
    if scope != Scope::Full {
        return err("write scope required", 403);
    }
    // Dropping a database is irreversible; refuse entirely unless the
    // operator opted into authentication.
    if reg.opts.token.is_none() {
        return err(
            "DELETE /db/{name} is disabled unless the server runs with --token",
            403,
        );
    }
    let Some(root) = &reg.opts.data_dir else {
        return err(
            "dropping databases requires the server to run with --data-dir",
            400,
        );
    };
    if !valid_db_name(name) {
        return err("invalid db name", 400);
    }
    {
        let mut map = reg.dbs.write().unwrap();
        if let Some(db) = map.get(name) {
            if Arc::strong_count(db) > 1 {
                return err("database busy (requests in flight); retry", 409);
            }
            map.remove(name); // drop closes the engine and releases the flock
        }
    }
    let dir = root.join(name);
    if dir.is_dir() {
        if let Err(e) = std::fs::remove_dir_all(&dir) {
            return err(&format!("remove {}: {e}", dir.display()), 500);
        }
    }
    json_resp(&serde_json::json!({"dropped": name}), 200)
}

// ===========================================================================
// Per-database endpoints
// ===========================================================================

fn handle_db(
    reg: &Arc<Registry>,
    db: &Arc<Db>,
    scope: Scope,
    method: &Method,
    path: &str,
    body: &str,
    idem_key: Option<&str>,
) -> Resp {
    match (method, path) {
        (Method::Get, "/ui") => html(UI_HTML),
        (Method::Get, "/graph") => {
            let e = db.engine.read().unwrap();
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
            json_resp(
                &serde_json::json!({"nodes": nodes, "edges": edges, "truncated": edges.len() >= limit}),
                200,
            )
        }
        (Method::Get, "/traces") => {
            let e = db.engine.read().unwrap();
            json_resp(
                &serde_json::json!({"ids": e.traces.ids(), "count": e.traces.len()}),
                200,
            )
        }
        (m, p) if *m == Method::Get && p.starts_with("/trace/") => {
            let id: Option<u64> = p.trim_start_matches("/trace/").parse().ok();
            let e = db.engine.read().unwrap();
            let g = e.pg.graph();
            match id.and_then(|i| e.traces.get(i)) {
                Some(tr) => {
                    let surfaced: Vec<_> = tr.surfaced().iter().map(|(nid, a)| {
                        serde_json::json!({"node": g.node_name(*nid), "activation": a})
                    }).collect();
                    let fired: Vec<_> = tr
                        .fired()
                        .iter()
                        .map(|(eid, u, v, d)| {
                            serde_json::json!({
                                "src": g.node_name(*u), "dst": g.node_name(*v),
                                "kind": g.kind_str(g.edge(*eid).kind_id), "delta": d
                            })
                        })
                        .collect();
                    json_resp(
                        &serde_json::json!({"t": tr.t(), "surfaced": surfaced, "fired": fired}),
                        200,
                    )
                }
                None => err("unknown trace id", 404),
            }
        }
        (Method::Get, "/health") => json_resp(&serde_json::json!({"ok": true}), 200),
        (Method::Get, "/stats") => {
            let e = db.engine.read().unwrap();
            json_resp(&e.layer.stats(e.pg.graph()), 200)
        }
        (Method::Post, "/match") => {
            #[derive(Deserialize)]
            struct MatchReq {
                tokens: Vec<String>,
                #[serde(default = "d_match_limit")]
                limit: usize,
                /// "token" (default): indexed token-prefix matching,
                /// O(log N + hits). "substring": legacy full-name substring
                /// scan, O(nodes) — for mid-token needles.
                #[serde(default)]
                mode: Option<String>,
            }
            let r: MatchReq = match serde_json::from_str(body) {
                Ok(r) => r,
                Err(e) => return err(&format!("bad body: {e}"), 400),
            };
            let e = db.engine.read().unwrap();
            let g = e.pg.graph();
            let hits: Vec<String> = match r.mode.as_deref() {
                Some("substring") => {
                    let toks: Vec<String> = r.tokens.iter().map(|t| t.to_lowercase()).collect();
                    let mut hits = Vec::new();
                    for id in 0..g.node_count() {
                        let name = g.node_name(id as u32);
                        let lname = name.to_lowercase();
                        if toks
                            .iter()
                            .any(|t| !t.is_empty() && lname.contains(t.as_str()))
                        {
                            hits.push(name.to_string());
                            if hits.len() >= r.limit {
                                break;
                            }
                        }
                    }
                    hits
                }
                _ => g
                    .match_tokens(&r.tokens, r.limit)
                    .into_iter()
                    .map(|id| g.node_name(id).to_string())
                    .collect(),
            };
            json_resp(&serde_json::json!({ "nodes": hits }), 200)
        }
        (Method::Post, "/retrieve") => {
            let r: RetrieveReq = match serde_json::from_str(body) {
                Ok(r) => r,
                Err(e) => return err(&format!("bad body: {e}"), 400),
            };
            if scope == Scope::Read && (r.adapt || r.trace) {
                return err(
                    "read scope: retrieval must set adapt=false and trace=false (both mutate state)",
                    403,
                );
            }
            let ts = r.ts.unwrap_or_else(now_ms);
            let seeds: Vec<&str> = r.seeds.iter().map(|s| s.as_str()).collect();
            // Renders a trace's fired edges as the explain payload.
            let explain_json = |g: &psyrag_graph::TemporalGraph, tr: &psyrag_core::Trace| {
                let fired: Vec<_> = tr
                    .fired()
                    .iter()
                    .map(|(eid, u, v, d)| {
                        serde_json::json!({
                            "src": g.node_name(*u), "dst": g.node_name(*v),
                            "kind": g.kind_str(g.edge(*eid).kind_id), "delta": d,
                        })
                    })
                    .collect();
                serde_json::json!({"fired": fired})
            };
            if r.adapt || r.trace {
                let mut guard = db.engine.write().unwrap();
                let e = &mut *guard;
                let depth = r.depth.unwrap_or(e.layer.cfg.depth);
                let fan = r.fan.unwrap_or(e.layer.cfg.fan);
                let (mut res, tr) =
                    e.layer
                        .retrieve_traced(e.pg.graph(), &seeds, depth, fan, r.top_k, ts);
                if r.adapt {
                    res.lambda_scale = e.layer.observe(res.mass);
                }
                let explain = r.explain.then(|| explain_json(e.pg.graph(), &tr));
                if r.trace {
                    match e.traces.put(tr) {
                        Ok(id) => {
                            let mut body = serde_json::json!({"result": res, "trace_id": id});
                            if let Some(x) = explain {
                                body["explain"] = x;
                            }
                            json_resp(&body, 200)
                        }
                        Err(msg) => err(&format!("trace not persisted: {msg}"), 500),
                    }
                } else if let Some(x) = explain {
                    json_resp(&serde_json::json!({"result": res, "explain": x}), 200)
                } else {
                    json_resp(&res, 200)
                }
            } else {
                let e = db.engine.read().unwrap();
                let depth = r.depth.unwrap_or(e.layer.cfg.depth);
                let fan = r.fan.unwrap_or(e.layer.cfg.fan);
                if r.explain {
                    let (res, tr) =
                        e.layer
                            .retrieve_traced(e.pg.graph(), &seeds, depth, fan, r.top_k, ts);
                    json_resp(
                        &serde_json::json!({"result": res, "explain": explain_json(e.pg.graph(), &tr)}),
                        200,
                    )
                } else {
                    let res = e
                        .layer
                        .retrieve(e.pg.graph(), &seeds, depth, fan, r.top_k, ts);
                    json_resp(&res, 200)
                }
            }
        }
        _ => {
            // Everything below mutates the database.
            if !scope.can_write() {
                return err("write scope required", 403);
            }
            // A wedged database (WAL write failed after in-memory apply)
            // refuses writes; reads above keep serving.
            if let Some(w) = db.engine.read().unwrap().wedged.clone() {
                return err(
                    &format!("database is wedged read-only (WAL write failed: {w}); restart the server after fixing the disk"),
                    503,
                );
            }
            // Growth quotas gate ingest only: maintenance (consolidate,
            // checkpoint, purge, sleep) and feedback must keep working on a
            // full database or there is no way back under quota.
            if path == "/ingest" {
                if let Some(resp) = check_capacity(reg, db) {
                    return resp;
                }
            }
            let Some(key) = idem_key else {
                return handle_db_write(reg, db, method, path, body).into_resp(false);
            };
            // Idempotent retry: replay the stored final response for a
            // repeated (endpoint, key) — durably, so the dedup survives a
            // restart. Concurrent duplicates are serialized in memory (an
            // in-flight marker that died with a crash SHOULD be retryable).
            let cache_key = format!("{method} {path} {key}");
            if let Some((code, body)) = db.engine.read().unwrap().idem.get(&cache_key) {
                return JsonOut {
                    code,
                    body: body.into_bytes(),
                }
                .into_resp(true);
            }
            {
                let mut inflight = db.inflight.lock().unwrap();
                inflight.retain(|_, at| at.elapsed() < IDEM_INFLIGHT_STALE);
                match inflight.get(&cache_key) {
                    Some(_) => {
                        return err(
                            "a request with this Idempotency-Key is already in flight",
                            409,
                        );
                    }
                    None => {
                        inflight.insert(cache_key.clone(), Instant::now());
                    }
                }
            }
            let out = handle_db_write(reg, db, method, path, body);
            if out.code < 500 {
                // Persist the final outcome (2xx/4xx) BEFORE responding, so
                // an acked response is always replayable. If persisting the
                // dedup record fails we still return the ORIGINAL response:
                // the operation itself is already durable, and a 5xx here
                // would trigger an immediate retry with no dedup record —
                // the exact double-apply this exists to prevent.
                let body_str = String::from_utf8_lossy(&out.body).into_owned();
                if let Err(er) = db
                    .engine
                    .write()
                    .unwrap()
                    .idem
                    .put(&cache_key, out.code, &body_str)
                {
                    log::error(
                        "idem_persist_failed",
                        serde_json::json!({"key": cache_key, "error": er}),
                    );
                }
            }
            db.inflight.lock().unwrap().remove(&cache_key);
            out.into_resp(false)
        }
    }
}

fn handle_db_write(
    reg: &Arc<Registry>,
    db: &Arc<Db>,
    method: &Method,
    path: &str,
    body: &str,
) -> JsonOut {
    match (method, path) {
        (Method::Post, "/ingest") => {
            let r: IngestReq = match serde_json::from_str(body) {
                Ok(r) => r,
                Err(e) => return jerr(&format!("bad body: {e}"), 400),
            };
            // Validate the payload BEFORE touching the store so bad input is
            // a 400 and a failed ingest of valid input is a 500.
            if r.cai {
                #[cfg(feature = "gcp")]
                if let Err(e) = psyrag_graph::gcp::parse_snapshot(&r.json) {
                    return jerr(&format!("bad CAI snapshot: {e}"), 400);
                }
                #[cfg(not(feature = "gcp"))]
                return jerr("built without gcp feature", 400);
            } else if let Err(e) = psyrag_graph::entity::parse_entities(&r.json) {
                return jerr(&format!("bad entities: {e}"), 400);
            }
            let ts = r.ts.unwrap_or_else(now_ms);
            let mut guard = db.engine.write().unwrap();
            let e = &mut *guard;
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
                e.pg.ingest_entities_from(&r.json, ts, r.reconcile, r.origin.as_deref())
            };
            match res {
                Ok(stale) => {
                    e.layer.sync(e.pg.graph());
                    jout(
                        &serde_json::json!({"stale_retired": stale, "edges": e.pg.graph().edge_count()}),
                        200,
                    )
                }
                Err(msg) => {
                    // Ops may be applied in memory but unacked on disk:
                    // memory/disk have diverged, so wedge read-only.
                    e.wedge(&msg);
                    jerr(
                        &format!("ingest failed mid-write; database wedged read-only: {msg}"),
                        500,
                    )
                }
            }
        }
        (Method::Post, "/touch") => {
            let r: TouchReq = match serde_json::from_str(body) {
                Ok(r) => r,
                Err(e) => return jerr(&format!("bad body: {e}"), 400),
            };
            let ts = r.ts.unwrap_or_else(now_ms);
            let mut guard = db.engine.write().unwrap();
            let e = &mut *guard;
            let mut touches = Vec::new();
            let mut missed = 0;
            for (s, d, k, rr) in &r.edges {
                match e.layer.edge_id(e.pg.graph(), s, d, k) {
                    Some(eid) => touches.push((eid, *rr)),
                    None => missed += 1,
                }
            }
            e.layer.touch(&touches, ts);
            jout(
                &serde_json::json!({"touched": touches.len(), "missed": missed}),
                200,
            )
        }
        (Method::Post, "/feedback") => {
            let mut r: FeedbackReq = match serde_json::from_str(body) {
                Ok(r) => r,
                Err(e) => return jerr(&format!("bad body: {e}"), 400),
            };
            // Poisoning limits: per-DB rate window, then server-side clamp
            // on credit magnitudes (defense in depth above the layer's
            // per-edge r_clip — one hostile client cannot firehose salience).
            if reg.opts.max_feedback_per_min > 0 {
                let mut w = db.feedback_window.lock().unwrap();
                if w.0.elapsed() >= Duration::from_secs(60) {
                    *w = (Instant::now(), 0);
                }
                w.1 += 1;
                if w.1 > reg.opts.max_feedback_per_min {
                    return jerr(
                        &format!(
                            "feedback rate limit ({}/min) exceeded",
                            reg.opts.max_feedback_per_min
                        ),
                        429,
                    );
                }
            }
            let cap = reg.opts.max_credit;
            if cap > 0.0 {
                if let Some(rw) = r.reward.as_mut() {
                    *rw = rw.clamp(-cap, cap);
                }
                if let Some(nodes) = r.nodes.as_mut() {
                    for (_, score) in nodes.iter_mut() {
                        *score = score.clamp(-cap, cap);
                    }
                }
            }
            let ts = r.ts.unwrap_or_else(now_ms);
            let mut guard = db.engine.write().unwrap();
            let e = &mut *guard;
            // build the Credit (nodes > reward > used)
            let hit = e.layer.cfg.feedback_hit;
            let credit = if let Some(nodes) = r.nodes.clone() {
                Credit::Nodes(nodes)
            } else if let Some(reward) = r.reward {
                Credit::Episodic {
                    reward,
                    spread: r.spread.unwrap_or(Spread::ByActivation),
                }
            } else if let Some(used) = r.used.clone() {
                Credit::Nodes(used.into_iter().map(|n| (n, hit)).collect())
            } else {
                return jerr("feedback needs one of: nodes, reward, used", 400);
            };
            // get the trace: deferred (trace_id) or stateless (recompute)
            let trace = if let Some(id) = r.trace_id {
                match e.traces.get(id) {
                    Some(t) => t,
                    None => return jerr("unknown trace_id (evicted or never issued)", 404),
                }
            } else if let Some(seeds) = r.seeds.clone() {
                let sref: Vec<&str> = seeds.iter().map(|s| s.as_str()).collect();
                let depth = r.depth.unwrap_or(e.layer.cfg.depth);
                let fan = r.fan.unwrap_or(e.layer.cfg.fan);
                let (_res, tr) =
                    e.layer
                        .retrieve_traced(e.pg.graph(), &sref, depth, fan, r.top_k, ts);
                tr
            } else {
                return jerr("feedback needs trace_id or seeds", 400);
            };
            let report = e.layer.apply_credit(e.pg.graph(), &trace, &credit, ts);
            if let Err(msg) = e.save_sidecar() {
                return jerr(&format!("feedback applied but not persisted: {msg}"), 500);
            }
            jout(&report, 200)
        }
        (Method::Post, "/sleep") => {
            let ts: i64 = serde_json::from_str::<serde_json::Value>(body)
                .ok()
                .and_then(|v| v.get("ts").and_then(|x| x.as_i64()))
                .unwrap_or_else(now_ms);
            let mut guard = db.engine.write().unwrap();
            let e = &mut *guard;
            let rep = {
                let g = e.pg.graph();
                e.layer.sleep(g, ts)
            };
            if let Err(msg) = e.save_sidecar() {
                return jerr(&format!("sleep ran but not persisted: {msg}"), 500);
            }
            jout(&rep, 200)
        }
        (Method::Post, "/checkpoint") => {
            #[derive(Deserialize)]
            struct CheckpointReq {
                #[serde(default = "d_true")]
                archive: bool,
            }
            let r: CheckpointReq = if body.trim().is_empty() {
                CheckpointReq { archive: true }
            } else {
                match serde_json::from_str(body) {
                    Ok(r) => r,
                    Err(e) => return jerr(&format!("bad body: {e}"), 400),
                }
            };
            let mut guard = db.engine.write().unwrap();
            let e = &mut *guard;
            let report = match e.pg.compact(r.archive) {
                Ok(rep) => rep,
                Err(msg) => {
                    // Compaction failures can leave the writer mid-swap;
                    // wedge rather than risk appending to the wrong file.
                    e.wedge(&msg);
                    return jerr(
                        &format!("checkpoint failed; database wedged read-only: {msg}"),
                        500,
                    );
                }
            };
            // Sidecar v2 keys are stable across the renumbering the next
            // replay performs, so saving against the (old-id) in-memory
            // graph is correct.
            if let Err(msg) = e.save_sidecar() {
                return jerr(
                    &format!("checkpoint done but sidecar not persisted: {msg}"),
                    500,
                );
            }
            // Outstanding traces reference pre-compaction dense ids.
            if let Err(msg) = e.traces.clear() {
                return jerr(
                    &format!("checkpoint done but trace log not cleared: {msg}"),
                    500,
                );
            }
            jout(
                &serde_json::json!({
                    "report": report,
                    "traces_cleared": true,
                    "note": "in-memory graph keeps full history until restart; the on-disk log is compacted",
                }),
                200,
            )
        }
        (Method::Post, "/quarantine") => {
            #[derive(Deserialize)]
            struct QuarantineReq {
                origin_prefix: String,
                /// 0.0 = fully quarantined; 1.0 (or removing the entry via
                /// config) = restored. Retrieval-time mask only.
                #[serde(default)]
                trust: f32,
            }
            let r: QuarantineReq = match serde_json::from_str(body) {
                Ok(r) => r,
                Err(e) => return jerr(&format!("bad body: {e}"), 400),
            };
            if r.origin_prefix.is_empty() {
                return jerr("origin_prefix must be non-empty", 400);
            }
            if !r.trust.is_finite() || r.trust < 0.0 {
                return jerr("trust must be a finite number >= 0", 400);
            }
            let mut guard = db.engine.write().unwrap();
            let e = &mut *guard;
            e.layer.set_trust(e.pg.graph(), &r.origin_prefix, r.trust);
            // Persist into the DB's config.json so trust survives restarts
            // (multi-DB mode; legacy mode applies in-memory only).
            let persisted = match &db.cfg_path {
                Some(cp) => {
                    let s = serde_json::to_string_pretty(&e.layer.cfg).unwrap_or_default();
                    match std::fs::write(cp, s) {
                        Ok(()) => true,
                        Err(er) => {
                            return jerr(
                                &format!("trust applied but config not persisted: {er}"),
                                500,
                            )
                        }
                    }
                }
                None => false,
            };
            jout(
                &serde_json::json!({
                    "origin_prefix": r.origin_prefix,
                    "trust": r.trust,
                    "persisted": persisted,
                }),
                200,
            )
        }
        (Method::Post, "/purge") => {
            #[derive(Deserialize)]
            struct PurgeReq {
                origin_prefix: String,
            }
            // Purging is irreversible content deletion; like DELETE /db it
            // is disabled unless the operator opted into authentication.
            if reg.opts.token.is_none() {
                return jerr(
                    "POST /purge is disabled unless the server runs with --token",
                    403,
                );
            }
            let r: PurgeReq = match serde_json::from_str(body) {
                Ok(r) => r,
                Err(e) => return jerr(&format!("bad body: {e}"), 400),
            };
            let mut guard = db.engine.write().unwrap();
            let e = &mut *guard;
            // EdgeIds renumber: carry learned state across by stable key.
            let snap = e.layer.snapshot_keys(e.pg.graph());
            let report = match e.pg.purge(&r.origin_prefix) {
                Ok(rep) => rep,
                Err(msg) => {
                    e.wedge(&msg);
                    return jerr(
                        &format!("purge failed; database wedged read-only: {msg}"),
                        500,
                    );
                }
            };
            e.layer.restore_keys(e.pg.graph(), &snap);
            if let Err(msg) = e.save_sidecar() {
                return jerr(&format!("purge done but sidecar not persisted: {msg}"), 500);
            }
            if let Err(msg) = e.traces.clear() {
                return jerr(&format!("purge done but trace log not cleared: {msg}"), 500);
            }
            jout(
                &serde_json::json!({"report": report, "traces_cleared": true}),
                200,
            )
        }
        (Method::Post, "/consolidate") => {
            let r: ConsolidateReq = if body.trim().is_empty() {
                ConsolidateReq::default()
            } else {
                match serde_json::from_str(body) {
                    Ok(r) => r,
                    Err(e) => return jerr(&format!("bad body: {e}"), 400),
                }
            };
            let ts = r.ts.unwrap_or_else(now_ms);
            let mut guard = db.engine.write().unwrap();
            let e = &mut *guard;
            let (stats, conflicts) = {
                let g = e.pg.graph();
                e.layer.consolidate(g, ts)
            };
            // Optionally journal supersession ops (a TRUTH change) to the WAL.
            let mut applied = 0;
            if r.apply_conflicts {
                for c in &conflicts {
                    for op in &c.superseded {
                        if let Err(msg) = e.pg.record_op(op.clone()) {
                            e.wedge(&msg);
                            return jerr(
                                &format!(
                                    "conflict op not journaled; database wedged read-only: {msg}"
                                ),
                                500,
                            );
                        }
                        applied += 1;
                    }
                }
                if let Err(msg) = e.pg.flush() {
                    e.wedge(&msg);
                    return jerr(
                        &format!("conflict ops not persisted; database wedged read-only: {msg}"),
                        500,
                    );
                }
                e.layer.sync(e.pg.graph());
            }
            if let Err(msg) = e.save_sidecar() {
                return jerr(&format!("consolidation ran but not persisted: {msg}"), 500);
            }
            jout(
                &serde_json::json!({"stats": stats, "conflicts": conflicts, "applied_ops": applied}),
                200,
            )
        }
        _ => jerr("not found", 404),
    }
}
