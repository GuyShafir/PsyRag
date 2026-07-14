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

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
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

    pub fn put(&mut self, t: Trace) -> u64 {
        let id = self.next;
        self.next += 1;
        if let Some(path) = &self.path {
            if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(path) {
                if let Ok(line) = serde_json::to_string(&TraceRecord { id, trace: t.clone() }) {
                    let _ = writeln!(f, "{line}");
                    self.lines += 1;
                }
            }
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
            self.compact();
        }
        id
    }

    fn compact(&mut self) {
        let Some(path) = self.path.clone() else { return };
        let tmp = format!("{path}.tmp");
        if let Ok(mut f) = std::fs::File::create(&tmp) {
            let mut n = 0;
            for id in self.order.iter() {
                if let Some(t) = self.map.get(id) {
                    if let Ok(line) = serde_json::to_string(&TraceRecord { id: *id, trace: t.clone() }) {
                        let _ = writeln!(f, "{line}");
                        n += 1;
                    }
                }
            }
            let _ = std::fs::rename(&tmp, &path);
            self.lines = n;
        }
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
