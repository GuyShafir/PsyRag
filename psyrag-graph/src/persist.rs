//! Persistence: a write-ahead log of `Op` records, one framed JSON object per
//! line.
//!
//! Record format (v1): `crc32hex8:{json}\n` — an 8-hex-digit IEEE CRC32 of the
//! JSON bytes, a colon, then the serialized op. New files begin with a
//! `#psyrag-wal:v1` header line. Legacy (v0) plain-NDJSON lines replay
//! transparently, and new appends to a legacy file are framed — a mixed file
//! is valid, so no in-place migration is ever needed.
//!
//! Properties:
//! - The WAL *is* the database. Replaying it reproduces identical state,
//!   including full temporal history — consistent with the append-only model.
//! - Ops are name-addressed, so the log is portable across processes
//!   (NodeIds are process-local arena indices).
//! - Durability: `flush()` pushes buffered records AND fsyncs (`sync_data`).
//!   This is the group-commit point; callers flush at batch boundaries, and
//!   an acknowledged flush means the data survived power loss.
//! - Corruption policy: a torn FINAL record (crash mid-append) is truncated
//!   away on open and counted in `replay_skipped` — never an error. Corruption
//!   anywhere else is a hard error: `EdgeId`s are dense in replay order, so
//!   silently skipping a mid-file record would misalign every later edge's
//!   plasticity sidecar state.
//! - Single-writer: an exclusive advisory lock (flock) is held on the WAL for
//!   the life of the handle. A second opener (e.g. the CLI while `psyrag
//!   serve` is running) fails fast with a clear error instead of corrupting
//!   the log.
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

const WAL_HEADER: &str = "#psyrag-wal:v1";

/// Generate a WAL identity for a new log: hex of FNV-1a over (nanos, pid).
/// Not cryptographic — it only needs to distinguish one WAL lineage from
/// another so a sidecar copied next to the wrong WAL is caught loudly.
fn new_wal_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id() as u128;
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in nanos.to_le_bytes().iter().chain(pid.to_le_bytes().iter()) {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{h:016x}")
}

/// Extract ` id=<hex>` from a `#psyrag-wal:...` header line.
fn parse_header_id(line: &str) -> Option<String> {
    line.split_whitespace()
        .find_map(|tok| tok.strip_prefix("id=").map(String::from))
}

/// Extract the format version from a `#psyrag-wal:vN ...` header line.
fn parse_header_version(line: &str) -> Option<u32> {
    line.strip_prefix("#psyrag-wal:v")
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|v| v.parse().ok())
}

/// The newest WAL format this binary understands. A header with a higher
/// version refuses to open: after a rollback, "refuse loudly" beats
/// half-reading a future format.
const WAL_VERSION_MAX: u32 = 1;

/// IEEE CRC32 (the zlib/PNG polynomial), table-based. Kept in-crate to hold
/// the zero-dependency line.
pub fn crc32(data: &[u8]) -> u32 {
    use std::sync::OnceLock;
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        let mut i = 0u32;
        while i < 256 {
            let mut c = i;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
                k += 1;
            }
            t[i as usize] = c;
            i += 1;
        }
        t
    });
    let mut c = 0xFFFF_FFFFu32;
    for &b in data {
        c = table[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    c ^ 0xFFFF_FFFF
}

/// Exclusive advisory lock on the WAL file, non-blocking. Uses flock(2)
/// directly (declared here rather than via the libc crate to keep the
/// dependency tree at serde/serde_json). Once MSRV reaches 1.89 this becomes
/// `File::try_lock()`.
#[cfg(unix)]
fn try_lock_exclusive(f: &File, path: &Path) -> Result<(), String> {
    use std::os::unix::io::AsRawFd;
    extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }
    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    if unsafe { flock(f.as_raw_fd(), LOCK_EX | LOCK_NB) } == 0 {
        return Ok(());
    }
    let e = std::io::Error::last_os_error();
    if e.kind() == std::io::ErrorKind::WouldBlock {
        Err(format!(
            "WAL {} is locked by another process (is `psyrag serve` running against it?); \
             stop it or use the HTTP API instead",
            path.display()
        ))
    } else {
        Err(format!("lock WAL {}: {e}", path.display()))
    }
}

/// Advisory locking is not implemented off unix; single-writer discipline is
/// the operator's responsibility there.
#[cfg(not(unix))]
fn try_lock_exclusive(_f: &File, _path: &Path) -> Result<(), String> {
    Ok(())
}

enum Parsed {
    Header,
    Record(Op),
}

/// Parse one WAL line (framed v1, legacy v0 plain JSON, or header/comment).
fn parse_line(s: &str) -> Result<Parsed, String> {
    if s.starts_with('#') {
        // Header / comment line. Unknown headers are tolerated for forward
        // compatibility; a torn header is caught by the trailing-newline rule.
        return Ok(Parsed::Header);
    }
    let b = s.as_bytes();
    if b.len() > 9 && b[8] == b':' && b[..8].iter().all(|c| c.is_ascii_hexdigit()) {
        let want = u32::from_str_radix(&s[..8], 16).map_err(|e| e.to_string())?;
        let json = &s[9..];
        let got = crc32(json.as_bytes());
        if got != want {
            return Err(format!("crc mismatch (recorded {want:08x}, computed {got:08x})"));
        }
        let op = serde_json::from_str(json)
            .map_err(|e| format!("valid crc but unparseable op (schema change?): {e}"))?;
        return Ok(Parsed::Record(op));
    }
    // Legacy v0: bare JSON op.
    serde_json::from_str(s)
        .map(Parsed::Record)
        .map_err(|e| format!("unparseable record: {e}"))
}

/// Structural verification of a WAL, read-only and lock-free (safe to run
/// against a live server; it may observe an in-flight torn tail, which is
/// reported, not failed).
#[derive(Debug, Clone, serde::Serialize)]
pub struct VerifyReport {
    pub wal_id: Option<String>,
    pub records: usize,
    pub framed: usize,
    pub legacy: usize,
    pub torn_tail: bool,
    /// First mid-file corrupt line (1-based), with its parse error.
    pub corrupt: Option<(usize, String)>,
}

pub fn verify_wal<P: AsRef<Path>>(path: P) -> Result<VerifyReport, String> {
    let f = File::open(path.as_ref()).map_err(|e| e.to_string())?;
    let mut rep = VerifyReport {
        wal_id: None,
        records: 0,
        framed: 0,
        legacy: 0,
        torn_tail: false,
        corrupt: None,
    };
    let mut pending_bad: Option<(usize, String)> = None;
    let mut line_no = 0usize;
    for line in BufReader::new(f).lines() {
        let line = line.map_err(|e| e.to_string())?;
        line_no += 1;
        if line.trim().is_empty() {
            continue;
        }
        if let Some(bad) = pending_bad.take() {
            rep.corrupt = Some(bad);
            return Ok(rep); // mid-file corruption: stop, report
        }
        match parse_line(line.trim_end_matches('\r')) {
            Ok(Parsed::Header) => {
                if rep.wal_id.is_none() {
                    rep.wal_id = parse_header_id(line.trim_end_matches('\r'));
                }
            }
            Ok(Parsed::Record(_)) => {
                rep.records += 1;
                if line.len() > 9 && line.as_bytes()[8] == b':' {
                    rep.framed += 1;
                } else {
                    rep.legacy += 1;
                }
            }
            Err(e) => pending_bad = Some((line_no, e)),
        }
    }
    rep.torn_tail = pending_bad.is_some();
    Ok(rep)
}

/// Replay a WAL into an in-memory graph WITHOUT taking the lock or repairing
/// the file (torn tail tolerated silently). For inspection/verification only.
pub fn replay_readonly<P: AsRef<Path>>(path: P) -> Result<TemporalGraph, String> {
    let f = File::open(path.as_ref()).map_err(|e| e.to_string())?;
    let mut g = TemporalGraph::new();
    for line in BufReader::new(f).lines() {
        let line = line.map_err(|e| e.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(Parsed::Record(op)) = parse_line(line.trim_end_matches('\r')) {
            g.apply(&op);
        }
    }
    Ok(g)
}

/// Take the WAL's exclusive lock without replaying it (e.g. to guarantee a
/// consistent file-copy backup). The lock lives as long as the returned
/// handle.
pub fn lock_wal_standalone<P: AsRef<Path>>(path: P) -> Result<File, String> {
    let path = path.as_ref();
    let f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("open {}: {e}", path.display()))?;
    try_lock_exclusive(&f, path)?;
    Ok(f)
}

pub struct PersistentGraph {
    graph: TemporalGraph,
    wal: BufWriter<File>,
    path: std::path::PathBuf,
    /// Identity of this WAL lineage (from the header; survives compaction).
    /// None for headerless legacy logs.
    wal_id: Option<String>,
    /// Log sequence number: ops replayed + ops appended. A sidecar stamped
    /// with (wal_id, lsn) is "as of" that point in this log.
    lsn: u64,
    /// Torn-tail records dropped during replay (0 or 1 with the v1 format).
    pub replay_skipped: usize,
}

/// What a `compact()` did.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CompactReport {
    pub ops_written: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    /// Where the pre-compaction log went (None with `archive = false`).
    pub archive: Option<String>,
}

impl OpSink for PersistentGraph {
    fn record(&mut self, op: Op) -> Result<(), String> {
        let json = serde_json::to_string(&op).map_err(|e| e.to_string())?;
        let line = format!("{:08x}:{}\n", crc32(json.as_bytes()), json);
        self.wal
            .write_all(line.as_bytes())
            .map_err(|e| e.to_string())?;
        self.graph.apply(&op);
        self.lsn += 1;
        Ok(())
    }
    fn graph(&self) -> &TemporalGraph {
        &self.graph
    }
}

impl PersistentGraph {
    /// Open (or create) a graph backed by the WAL at `path`: take the
    /// exclusive lock, replay the log (verifying record checksums), and
    /// recover from a torn tail by truncating it.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let path = path.as_ref();

        // Lock BEFORE replay so no other process can append between our
        // replay and our first write.
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("open WAL {}: {e}", path.display()))?;
        try_lock_exclusive(&file, path)?;

        let mut graph = TemporalGraph::new();
        let mut replay_skipped = 0usize;
        let mut lsn: u64 = 0;
        let mut wal_id: Option<String> = None;
        // Deferred verdict on the most recent bad line: only the FINAL line of
        // the file may be bad (torn tail). (offset, line_no, error)
        let mut pending_bad: Option<(u64, usize, String)> = None;
        // The last successfully parsed line lacked a trailing newline; a raw
        // append would concatenate onto it, so we terminate it first.
        let mut needs_newline = false;
        let mut is_empty = true;

        {
            let rf = File::open(path).map_err(|e| e.to_string())?;
            let mut rdr = BufReader::new(rf);
            let mut line = String::new();
            let mut offset: u64 = 0;
            let mut line_no = 0usize;
            loop {
                line.clear();
                let n = rdr.read_line(&mut line).map_err(|e| e.to_string())?;
                if n == 0 {
                    break;
                }
                let start = offset;
                offset += n as u64;
                line_no += 1;
                is_empty = false;
                let content = line.trim_end_matches(['\n', '\r']);
                if content.trim().is_empty() {
                    continue;
                }
                if let Some((_, bad_no, err)) = pending_bad.take() {
                    // A bad line followed by more data is mid-file corruption,
                    // not a torn tail. Refuse to replay past it: dense EdgeIds
                    // mean a silent skip would corrupt all downstream
                    // plasticity state.
                    return Err(format!(
                        "WAL {} corrupted at line {bad_no}: {err}. Replay stopped; restore from \
                         backup or repair the file (a torn final line would have been recovered \
                         automatically).",
                        path.display()
                    ));
                }
                needs_newline = false;
                match parse_line(content) {
                    Ok(Parsed::Header) => {
                        if let Some(v) = parse_header_version(content) {
                            if v > WAL_VERSION_MAX {
                                return Err(format!(
                                    "WAL {} is format v{v}, newer than this binary understands                                      (max v{WAL_VERSION_MAX}) — upgrade psyrag or restore the                                      pre-upgrade backup",
                                    path.display()
                                ));
                            }
                        }
                        if wal_id.is_none() {
                            wal_id = parse_header_id(content);
                        }
                        needs_newline = !line.ends_with('\n');
                    }
                    Ok(Parsed::Record(op)) => {
                        graph.apply(&op);
                        lsn += 1;
                        needs_newline = !line.ends_with('\n');
                    }
                    Err(e) => pending_bad = Some((start, line_no, e)),
                }
            }
        }

        if let Some((start, _, _)) = pending_bad {
            // Torn tail: drop the partial record so the next append starts on
            // a clean line.
            replay_skipped += 1;
            file.set_len(start).map_err(|e| e.to_string())?;
            needs_newline = false;
        }

        let mut wal = BufWriter::new(file);
        if needs_newline {
            // Complete final record missing its newline (crash between the
            // record write and the terminator): terminate it now.
            wal.write_all(b"\n").map_err(|e| e.to_string())?;
        }
        if is_empty {
            let id = new_wal_id();
            wal.write_all(format!("{WAL_HEADER} id={id}\n").as_bytes())
                .map_err(|e| e.to_string())?;
            wal_id = Some(id);
        }
        Ok(Self {
            graph,
            wal,
            path: path.to_path_buf(),
            wal_id,
            lsn,
            replay_skipped,
        })
    }

    /// **Checkpoint/compaction**: rewrite the WAL down to the ops that
    /// reconstruct the graph's *open* state (live node versions, live edges,
    /// with their original timestamps so `valid_from` is preserved), then
    /// atomically swap it in. Closed history is dropped from the working log;
    /// with `archive = true` the old log is kept beside it as
    /// `<wal>.archive-<ms>` (cold history / a natural backup unit).
    ///
    /// Safe against a live process: the new file is created and locked
    /// *before* any rename, so there is no instant at which another process
    /// could grab the WAL name. Safe against crashes: every step is
    /// write-new + rename; the old log is intact until the final rename.
    ///
    /// EdgeIds/NodeIds renumber on the next replay — persisted sidecars
    /// follow via their stable edge keys, but in-memory retrieval traces are
    /// invalidated (the caller clears its trace store).
    pub fn compact(&mut self, archive: bool) -> Result<CompactReport, String> {
        self.flush()?;
        let bytes_before = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);

        // 1. Synthesize the open-state ops: nodes first (real types), then
        //    placeholders, then open edges — replay order matters for typing.
        let g = &self.graph;
        let mut ops: Vec<Op> = Vec::new();
        for id in 0..g.node_count() {
            let n = g.node(id as crate::graph::NodeId);
            let Some(v) = n.versions.last().filter(|v| v.retired_at == crate::graph::T_MAX) else {
                continue; // fully retired: drop from the working log
            };
            let type_str = g.types.resolve(n.type_id).to_string();
            if n.placeholder {
                ops.push(Op::ObservePlaceholder {
                    name: n.name.clone(),
                    inferred_type: type_str,
                    ts: v.observed_at,
                });
            } else {
                ops.push(Op::ObserveNode {
                    name: n.name.clone(),
                    asset_type: type_str,
                    props: v.props_value(),
                    ts: v.observed_at,
                });
            }
        }
        for eid in 0..g.edge_count() {
            let e = g.edge(eid as crate::graph::EdgeId);
            if e.valid_to != crate::graph::T_MAX {
                continue; // closed edge: history, dropped
            }
            ops.push(Op::ObserveEdge {
                src: g.node_name(e.src).to_string(),
                dst: g.node_name(e.dst).to_string(),
                kind: g.kind_str(e.kind_id).to_string(),
                ts: e.valid_from,
            });
        }

        // 2. Write + lock the replacement before touching the live name.
        // (Names are appended to the full filename — with_extension would
        // clobber a ".wal" suffix.)
        let tmp_path = std::path::PathBuf::from(format!("{}.compact.tmp", self.path.display()));
        let tmp = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| format!("create {}: {e}", tmp_path.display()))?;
        try_lock_exclusive(&tmp, &tmp_path)?;
        let mut w = BufWriter::new(tmp);
        // Same lineage id across compaction (mint one for legacy logs so the
        // post-compaction sidecar binds to something).
        let id = self.wal_id.clone().unwrap_or_else(new_wal_id);
        w.write_all(format!("{WAL_HEADER} id={id}\n").as_bytes())
            .map_err(|e| e.to_string())?;
        for op in &ops {
            let json = serde_json::to_string(op).map_err(|e| e.to_string())?;
            let line = format!("{:08x}:{}\n", crc32(json.as_bytes()), json);
            w.write_all(line.as_bytes()).map_err(|e| e.to_string())?;
        }
        w.flush().map_err(|e| e.to_string())?;
        w.get_ref().sync_all().map_err(|e| e.to_string())?;

        // 3. Swap: old -> archive (or aside for deletion), tmp -> live name.
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let old_dest = std::path::PathBuf::from(if archive {
            format!("{}.archive-{stamp}", self.path.display())
        } else {
            format!("{}.compact.old", self.path.display())
        });
        std::fs::rename(&self.path, &old_dest).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp_path, &self.path).map_err(|e| e.to_string())?;
        if let Some(dir) = self.path.parent().filter(|p| !p.as_os_str().is_empty()) {
            if let Ok(d) = File::open(dir) {
                let _ = d.sync_all();
            }
        }
        let archive_path = if archive {
            Some(old_dest.to_string_lossy().into_owned())
        } else {
            std::fs::remove_file(&old_dest).map_err(|e| e.to_string())?;
            None
        };

        // 4. Adopt the tmp fd itself as our writer: it already holds the
        //    flock on what is now the live inode, so the lock is continuous —
        //    there is no instant where the WAL name is open to another
        //    process. (flock is per open-file-description: re-opening and
        //    re-locking from this same process would deadlock against `w`.)
        //    The fd's write position is the end of the file, so sequential
        //    appends continue correctly without O_APPEND.
        let f = w
            .into_inner()
            .map_err(|e| format!("adopt compacted WAL: {e}"))?;
        self.wal = BufWriter::new(f);
        self.wal_id = Some(id);
        // LSN restarts with the rewritten log so sidecars stamped after this
        // agree with what a fresh replay of the new file will count.
        self.lsn = ops.len() as u64;

        let bytes_after = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        Ok(CompactReport {
            ops_written: ops.len(),
            bytes_before,
            bytes_after,
            archive: archive_path,
        })
    }

    /// Read access to the underlying graph (all temporal queries live there).
    pub fn graph(&self) -> &TemporalGraph {
        &self.graph
    }

    /// This WAL lineage's identity (None for headerless legacy logs).
    pub fn wal_id(&self) -> Option<&str> {
        self.wal_id.as_deref()
    }
    /// Current log sequence number (ops replayed + appended).
    pub fn lsn(&self) -> u64 {
        self.lsn
    }

    /// Journal-then-apply a single op (incremental path). Call `flush` at
    /// batch boundaries.
    pub fn record_op(&mut self, op: Op) -> Result<(), String> {
        OpSink::record(self, op)
    }

    /// Flush buffered records and fsync — the durability point. An `Ok` here
    /// means the batch survives power loss. Called automatically at snapshot
    /// boundaries.
    pub fn flush(&mut self) -> Result<(), String> {
        self.wal.flush().map_err(|e| e.to_string())?;
        self.wal.get_ref().sync_data().map_err(|e| e.to_string())
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

impl Drop for PersistentGraph {
    /// Best-effort final flush + fsync so a clean process exit never loses
    /// buffered records. (Explicit `flush()` remains the checked path.)
    fn drop(&mut self) {
        let _ = self.wal.flush();
        let _ = self.wal.get_ref().sync_data();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_known_answer() {
        // The canonical CRC32 check value.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn framed_line_roundtrip() {
        let op = Op::ObserveNode {
            name: "a".into(),
            asset_type: "t/x".into(),
            props: serde_json::json!({"k": 1}),
            ts: 5,
        };
        let json = serde_json::to_string(&op).unwrap();
        let line = format!("{:08x}:{}", crc32(json.as_bytes()), json);
        match parse_line(&line) {
            Ok(Parsed::Record(Op::ObserveNode { name, ts, .. })) => {
                assert_eq!(name, "a");
                assert_eq!(ts, 5);
            }
            _ => panic!("expected framed record"),
        }
    }

    #[test]
    fn crc_mismatch_is_error() {
        let line = format!("deadbeef:{}", r#"{"op":"retire_node","name":"a","ts":1}"#);
        assert!(parse_line(&line).is_err());
    }

    #[test]
    fn legacy_line_still_parses() {
        let line = r#"{"op":"retire_node","name":"a","ts":1}"#;
        assert!(matches!(parse_line(line), Ok(Parsed::Record(_))));
    }
}
