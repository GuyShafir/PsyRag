//! Persistence: a write-ahead log of `Op` records, one JSON object per line.
//!
//! Properties:
//! - The WAL *is* the database. Replaying it reproduces identical state,
//!   including full temporal history — consistent with the append-only model.
//! - Ops are name-addressed, so the log is portable across processes
//!   (NodeIds are process-local arena indices).
//! - Torn tails tolerated: a partial last line (crash mid-write) is dropped
//!   on replay with a warning count, never an error.
//! - Reconciliation is journaled by *effect*: retirements are written as
//!   explicit `RetireNode`/`RetireEdge` ops. The log never contains derived
//!   state that depends on replay order.
//!
//! `PersistentGraph` is domain-agnostic: it implements `OpSink`, so every
//! adapter (generic entities, GCP CAI, future K8s/AWS/...) journals through
//! the same path.

use crate::entity;
use crate::graph::{Op, TemporalGraph, Ts};
use crate::snapshot::OpSink;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

pub struct PersistentGraph {
    graph: TemporalGraph,
    wal: BufWriter<File>,
    /// Lines skipped during replay (torn tail / corruption).
    pub replay_skipped: usize,
}

impl OpSink for PersistentGraph {
    fn record(&mut self, op: Op) -> Result<(), String> {
        serde_json::to_writer(&mut self.wal, &op).map_err(|e| e.to_string())?;
        self.wal.write_all(b"\n").map_err(|e| e.to_string())?;
        self.graph.apply(&op);
        Ok(())
    }
    fn graph(&self) -> &TemporalGraph {
        &self.graph
    }
}

impl PersistentGraph {
    /// Open (or create) a graph backed by the WAL at `path`, replaying any
    /// existing log.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let path = path.as_ref();
        let mut graph = TemporalGraph::new();
        let mut replay_skipped = 0usize;

        if path.exists() {
            let f = File::open(path).map_err(|e| e.to_string())?;
            for line in BufReader::new(f).lines() {
                let line = line.map_err(|e| e.to_string())?;
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<Op>(&line) {
                    Ok(op) => graph.apply(&op),
                    Err(_) => replay_skipped += 1, // torn tail: tolerate
                }
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            graph,
            wal: BufWriter::new(file),
            replay_skipped,
        })
    }

    /// Read access to the underlying graph (all temporal queries live there).
    pub fn graph(&self) -> &TemporalGraph {
        &self.graph
    }

    /// Journal-then-apply a single op (incremental path). Call `flush` at
    /// batch boundaries.
    pub fn record_op(&mut self, op: Op) -> Result<(), String> {
        OpSink::record(self, op)
    }

    /// Flush the WAL to the OS. Called automatically at snapshot boundaries.
    pub fn flush(&mut self) -> Result<(), String> {
        self.wal.flush().map_err(|e| e.to_string())
    }

    /// Ingest generic entities observed at `ts`. With `reconcile`, the input
    /// is a full domain snapshot (zombie + edge reconciliation applied).
    pub fn ingest_entities(
        &mut self,
        json: &str,
        ts: Ts,
        reconcile: bool,
    ) -> Result<Vec<String>, String> {
        let stale = entity::ingest_entities(self, json, ts, reconcile)?;
        self.flush()?;
        Ok(stale)
    }

    /// Ingest a full GCP Cloud Asset Inventory snapshot taken at `ts`.
    /// Returns pruned zombie names.
    #[cfg(feature = "gcp")]
    pub fn ingest_cai_snapshot(&mut self, json: &str, ts: Ts) -> Result<Vec<String>, String> {
        use crate::gcp::{asset_ops, parse_snapshot};
        use crate::snapshot::ingest_snapshot_ops;
        let assets = parse_snapshot(json)?;
        let stale =
            ingest_snapshot_ops(self, assets.iter().map(|a| asset_ops(a, ts)), ts, true)?;
        self.flush()?;
        Ok(stale)
    }
}
