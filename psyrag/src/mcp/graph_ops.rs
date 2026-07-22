use crate::engine::{now_ms, Engine};
use psyrag_graph::Op;
use serde_json::json;
use std::collections::VecDeque;
use std::path::Path;

const CO_EDITED: &str = "CO_EDITED";
const FILE_TYPE: &str = "file";
const ORIGIN: &str = "mcp";

/// Sliding window of the last N distinct touched paths. `record` returns the
/// (new, prev) pairs to journal as co-touch edges, newest-prev first, then
/// slides the window (moving a re-touched path to the front).
pub struct TouchWindow {
    span: usize,
    recent: VecDeque<String>,
}

impl TouchWindow {
    pub fn new(n: usize) -> Self { TouchWindow { span: n, recent: VecDeque::with_capacity(n) } }
    pub fn record(&mut self, path: &str) -> Vec<(String, String)> {
        let pairs: Vec<(String, String)> = self.recent.iter()
            .filter(|p| p.as_str() != path)
            .map(|p| (path.to_string(), p.clone()))
            .collect();
        self.recent.retain(|p| p != path);
        self.recent.push_front(path.to_string());
        while self.recent.len() > self.span { self.recent.pop_back(); }
        pairs
    }
}

/// Observe a touched file as a node and journal co-touch edges to recent
/// files. One WAL batch: record ops, flush, sync sidecar columns, save.
pub fn ingest_touch(engine: &mut Engine, window: &mut TouchWindow, path: &str) -> Result<(), String> {
    if engine.wedged.is_some() { return Ok(()); } // never write to a wedged db
    let ts = now_ms();
    let pairs = window.record(path);
    engine.pg.record_op(Op::ObserveNode {
        name: path.to_string(), asset_type: FILE_TYPE.into(),
        props: json!({}), ts, origin: Some(ORIGIN.into()),
    })?;
    for (src, dst) in &pairs {
        // ensure the dst node exists before the edge (idempotent observe)
        engine.pg.record_op(Op::ObserveNode {
            name: dst.clone(), asset_type: FILE_TYPE.into(),
            props: json!({}), ts, origin: Some(ORIGIN.into()),
        })?;
        engine.pg.record_op(Op::ObserveEdge {
            src: src.clone(), dst: dst.clone(), kind: CO_EDITED.into(),
            ts, origin: Some(ORIGIN.into()),
        })?;
    }
    engine.pg.flush()?;
    engine.layer.sync(engine.pg.graph());
    engine.save_sidecar()?;
    Ok(())
}

/// Seed CO_EDITED edges from git history so day one isn't an empty graph.
/// Parses `git log --name-only`, links files sharing a commit (window of 5
/// per commit to bound fan-out). Ok(0) when not a git repo. Idempotent:
/// re-observing the same edge just re-opens the existing one.
pub fn cold_start_from_git(engine: &mut Engine, repo_root: &Path) -> Result<usize, String> {
    // Already seeded: cold start only makes sense on an empty graph.
    if engine.pg.graph().node_count() > 0 {
        return Ok(0);
    }
    let out = std::process::Command::new("git")
        .args(["log", "--name-only", "--pretty=format:%x00", "-n", "500"])
        .current_dir(repo_root)
        .output();
    let out = match out {
        Ok(o) if o.status.success() => o.stdout,
        _ => return Ok(0),
    };
    let text = String::from_utf8_lossy(&out);
    let ts = now_ms();
    let mut added = 0usize;
    for commit in text.split('\u{0}') {
        let files: Vec<&str> = commit.lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .take(5)
            .collect();
        for (i, &a) in files.iter().enumerate() {
            engine.pg.record_op(Op::ObserveNode {
                name: a.into(), asset_type: FILE_TYPE.into(),
                props: json!({}), ts, origin: Some(ORIGIN.into()),
            })?;
            for &b in files.iter().skip(i + 1) {
                engine.pg.record_op(Op::ObserveEdge {
                    src: a.into(), dst: b.into(), kind: CO_EDITED.into(),
                    ts, origin: Some(ORIGIN.into()),
                })?;
                added += 1;
            }
        }
    }
    engine.pg.flush()?;
    engine.layer.sync(engine.pg.graph());
    engine.save_sidecar()?;
    Ok(added)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_pairs_recent_touches_within_span() {
        let mut w = TouchWindow::new(3);
        assert_eq!(w.record("a.rs"), Vec::<(String, String)>::new()); // first: no pairs
        assert_eq!(w.record("b.rs"), vec![("b.rs".to_string(), "a.rs".to_string())]);
        let pairs = w.record("c.rs"); // pairs with b and a (window 3)
        assert_eq!(pairs, vec![("c.rs".into(), "b.rs".into()), ("c.rs".into(), "a.rs".into())]);
    }

    #[test]
    fn window_drops_beyond_span_and_skips_self() {
        let mut w = TouchWindow::new(2);
        w.record("a.rs");
        w.record("b.rs");
        let pairs = w.record("a.rs"); // a re-touched: pairs only with b (a itself skipped)
        assert_eq!(pairs, vec![("a.rs".to_string(), "b.rs".to_string())]);
    }
}
