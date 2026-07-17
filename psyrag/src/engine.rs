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
