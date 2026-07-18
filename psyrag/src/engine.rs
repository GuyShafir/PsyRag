//! The in-memory engine: WAL-backed temporal graph + plasticity sidecar, plus a
//! DURABLE trace store for deferred feedback (credit that arrives after the
//! retrieval, applied against the trace as it was at retrieval time).

use psyrag_graph::PersistentGraph;
use psyrag_core::{PlasticityLayer, Trace};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Engine {
    pub pg: PersistentGraph,
    pub layer: PlasticityLayer,
    pub sidecar_path: String,
    pub traces: TraceStore,
    /// Durable idempotency-replay store: a retried Idempotency-Key must
    /// replay its original response even across a server restart, or the
    /// crash window reintroduces the double-apply the keys exist to prevent.
    pub idem: IdemStore,
    /// Set when a WAL write/flush failed AFTER ops were applied in memory:
    /// memory and disk have diverged and we cannot un-apply. The database
    /// stays readable but refuses further writes until restart (which
    /// replays only what the WAL acked). Holds the original error.
    pub wedged: Option<String>,
}

impl Engine {
    /// Persist the sidecar stamped with the WAL epoch it is as-of
    /// (wal_id + LSN). All sidecar saves go through here so the binding
    /// can never be forgotten.
    pub fn save_sidecar(&mut self) -> Result<(), String> {
        let lsn = self.pg.lsn();
        let id = self.pg.wal_id().map(|s| s.to_string());
        self.layer.set_wal_binding(id.as_deref(), lsn);
        self.layer.save(self.pg.graph(), &self.sidecar_path)
    }

    /// Record a WAL-write failure: wedge the database read-only.
    pub fn wedge(&mut self, err: &str) {
        if self.wedged.is_none() {
            self.wedged = Some(err.to_string());
        }
    }
}

/// Wall-clock millis, ratcheted monotonic: a backwards clock jump (NTP step,
/// manual reset) returns the last value seen instead of going back in time.
/// Decay math tolerates dt=0; it does not tolerate `t_last` in the future
/// (decay silently freezes until the clock catches up).
pub fn now_ms() -> i64 {
    use std::sync::atomic::{AtomicI64, Ordering};
    static LAST: AtomicI64 = AtomicI64::new(0);
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    LAST.fetch_max(wall, Ordering::Relaxed).max(wall)
}

#[derive(Serialize, Deserialize)]
struct TraceRecord {
    id: u64,
    trace: Trace,
}

#[derive(Serialize, Deserialize)]
struct IdemRecord {
    key: String,
    code: u16,
    /// The original response body (JSON, stored as a string).
    body: String,
    at_ms: i64,
}

/// Durable idempotency-replay store: NDJSON log of final responses keyed by
/// (endpoint, Idempotency-Key). Records are fsynced BEFORE the response is
/// acked — an acked write whose dedup record was lost would double-apply on
/// the client's post-crash retry, which is exactly the failure class this
/// prevents. Bounded by entry cap and time window; compacts like TraceStore.
pub struct IdemStore {
    cap: usize,
    window_ms: i64,
    map: HashMap<String, (u16, String, i64)>, // key -> (code, body, at_ms)
    order: VecDeque<String>,
    path: Option<String>,
    lines: usize,
}

impl IdemStore {
    pub fn in_memory(cap: usize, window_ms: i64) -> Self {
        IdemStore { cap, window_ms, map: HashMap::new(), order: VecDeque::new(), path: None, lines: 0 }
    }

    /// Durable store backed by an NDJSON file; replays records still inside
    /// the window (corrupt/torn lines are skipped — losing a dedup record
    /// only weakens dedup for that one key, it never corrupts data).
    pub fn open(cap: usize, window_ms: i64, path: &str) -> Self {
        let mut s = IdemStore {
            cap, window_ms, map: HashMap::new(), order: VecDeque::new(),
            path: Some(path.to_string()), lines: 0,
        };
        let now = now_ms();
        if let Ok(f) = std::fs::File::open(path) {
            for line in BufReader::new(f).lines().map_while(Result::ok) {
                if line.trim().is_empty() {
                    continue;
                }
                s.lines += 1;
                if let Ok(rec) = serde_json::from_str::<IdemRecord>(&line) {
                    if now - rec.at_ms > s.window_ms {
                        continue;
                    }
                    if !s.map.contains_key(&rec.key) {
                        s.order.push_back(rec.key.clone());
                    }
                    s.map.insert(rec.key, (rec.code, rec.body, rec.at_ms));
                }
            }
            while s.order.len() > s.cap {
                if let Some(old) = s.order.pop_front() {
                    s.map.remove(&old);
                }
            }
        }
        s
    }

    /// Look up a stored final response for a key (None once expired).
    pub fn get(&self, key: &str) -> Option<(u16, String)> {
        let (code, body, at) = self.map.get(key)?;
        if now_ms() - at > self.window_ms {
            return None;
        }
        Some((*code, body.clone()))
    }

    /// Store a final response, durably (fsynced) when file-backed. Must be
    /// called BEFORE the response is sent for the durability guarantee to
    /// hold.
    pub fn put(&mut self, key: &str, code: u16, body: &str) -> Result<(), String> {
        let at_ms = now_ms();
        if let Some(path) = &self.path {
            let rec = IdemRecord { key: key.to_string(), code, body: body.to_string(), at_ms };
            let line = serde_json::to_string(&rec).map_err(|e| e.to_string())?;
            let mut f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|e| format!("open idem log {path}: {e}"))?;
            writeln!(f, "{line}").map_err(|e| format!("append idem log: {e}"))?;
            f.sync_data().map_err(|e| format!("fsync idem log: {e}"))?;
            self.lines += 1;
        }
        if !self.map.contains_key(key) {
            self.order.push_back(key.to_string());
        }
        self.map.insert(key.to_string(), (code, body.to_string(), at_ms));
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.map.remove(&old);
            }
        }
        if self.path.is_some() && self.lines > self.cap.saturating_mul(4).max(64) {
            self.compact()?;
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<(), String> {
        let Some(path) = self.path.clone() else { return Ok(()) };
        let tmp = format!("{path}.tmp");
        let mut n = 0;
        {
            let mut f = std::fs::File::create(&tmp).map_err(|e| e.to_string())?;
            for key in self.order.iter() {
                if let Some((code, body, at_ms)) = self.map.get(key) {
                    let rec = IdemRecord { key: key.clone(), code: *code, body: body.clone(), at_ms: *at_ms };
                    writeln!(f, "{}", serde_json::to_string(&rec).map_err(|e| e.to_string())?)
                        .map_err(|e| e.to_string())?;
                    n += 1;
                }
            }
            f.sync_all().map_err(|e| e.to_string())?;
        }
        std::fs::rename(&tmp, &path).map_err(|e| e.to_string())?;
        self.lines = n;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

/// Capacity-bounded store of retrieval traces, keyed by a returned id. When a
/// path is given it is durable: each `put` appends an NDJSON record, and `open`
/// replays the file (keeping the last `cap`). Survives `psyrag serve` restarts, so
/// deferred credit still lands after a bounce. The log is compacted (rewritten
/// to the live set) once it grows past `4*cap` lines.
pub struct TraceStore {
    cap: usize,
    next: u64,
    map: HashMap<u64, Trace>,
    order: VecDeque<u64>,
    path: Option<String>,
    lines: usize,
}

impl TraceStore {
    pub fn in_memory(cap: usize) -> Self {
        TraceStore { cap, next: 1, map: HashMap::new(), order: VecDeque::new(), path: None, lines: 0 }
    }

    /// Durable store backed by an NDJSON file; replays existing records.
    pub fn open(cap: usize, path: &str) -> Self {
        let mut s = TraceStore {
            cap, next: 1, map: HashMap::new(), order: VecDeque::new(),
            path: Some(path.to_string()), lines: 0,
        };
        if let Ok(f) = std::fs::File::open(path) {
            for line in BufReader::new(f).lines().map_while(Result::ok) {
                if line.trim().is_empty() {
                    continue;
                }
                s.lines += 1;
                if let Ok(rec) = serde_json::from_str::<TraceRecord>(&line) {
                    s.next = s.next.max(rec.id + 1);
                    if !s.map.contains_key(&rec.id) {
                        s.order.push_back(rec.id);
                    }
                    s.map.insert(rec.id, rec.trace);
                }
            }
            // enforce cap on the replayed set (keep newest)
            while s.order.len() > s.cap {
                if let Some(old) = s.order.pop_front() {
                    s.map.remove(&old);
                }
            }
        }
        s
    }

    /// Store a trace, durably when backed by a file. Errors surface to the
    /// caller: a trace_id the server hands out must actually be redeemable
    /// after a restart, so a failed append is a failed request, not a shrug.
    pub fn put(&mut self, t: Trace) -> Result<u64, String> {
        let id = self.next;
        self.next += 1;
        if let Some(path) = &self.path {
            let line = serde_json::to_string(&TraceRecord { id, trace: t.clone() })
                .map_err(|e| e.to_string())?;
            let mut f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|e| format!("open trace log {path}: {e}"))?;
            writeln!(f, "{line}").map_err(|e| format!("append trace log: {e}"))?;
            f.sync_data().map_err(|e| format!("fsync trace log: {e}"))?;
            self.lines += 1;
        }
        self.map.insert(id, t);
        self.order.push_back(id);
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.map.remove(&old);
            }
        }
        // compact if the log has grown well past the live set
        if self.path.is_some() && self.lines > self.cap.saturating_mul(4).max(64) {
            self.compact()?;
        }
        Ok(id)
    }

    /// Rewrite the log to the live set: temp file + fsync + rename, so a
    /// crash mid-compaction leaves the old log intact.
    fn compact(&mut self) -> Result<(), String> {
        let Some(path) = self.path.clone() else { return Ok(()) };
        let tmp = format!("{path}.tmp");
        let mut n = 0;
        {
            let mut f = std::fs::File::create(&tmp).map_err(|e| e.to_string())?;
            for id in self.order.iter() {
                if let Some(t) = self.map.get(id) {
                    let line = serde_json::to_string(&TraceRecord { id: *id, trace: t.clone() })
                        .map_err(|e| e.to_string())?;
                    writeln!(f, "{line}").map_err(|e| e.to_string())?;
                    n += 1;
                }
            }
            f.sync_all().map_err(|e| e.to_string())?;
        }
        std::fs::rename(&tmp, &path).map_err(|e| e.to_string())?;
        if let Some(dir) = std::path::Path::new(&path).parent().filter(|p| !p.as_os_str().is_empty()) {
            if let Ok(d) = std::fs::File::open(dir) {
                let _ = d.sync_all();
            }
        }
        self.lines = n;
        Ok(())
    }

    /// Drop every stored trace and truncate the durable log. Called after WAL
    /// compaction: traces hold dense NodeId/EdgeIds, which renumber on the
    /// next replay. The id counter is NOT reset, so a stale client's old
    /// trace_id can never silently credit a new, unrelated trace.
    pub fn clear(&mut self) -> Result<(), String> {
        self.map.clear();
        self.order.clear();
        self.lines = 0;
        if let Some(path) = &self.path {
            if std::path::Path::new(path).exists() {
                std::fs::write(path, b"").map_err(|e| format!("truncate trace log: {e}"))?;
            }
        }
        Ok(())
    }

    pub fn get(&self, id: u64) -> Option<Trace> {
        self.map.get(&id).cloned()
    }

    /// Recent trace ids (newest last) for the management UI.
    pub fn ids(&self) -> Vec<u64> {
        self.order.iter().copied().collect()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}
