use crate::engine::now_ms;
use crate::engine::Engine;
use crate::mcp::graph_ops::{ingest_touch, TouchWindow};
use crate::mcp::recall::TraceRing;
use psyrag_core::Credit;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    /// The agent actually Read/Edited this path — observed downstream usage.
    Used { path: String },
    /// The harness is about to compact its context — run light consolidation.
    Compact,
}

/// Fire-and-forget: connect, write one line, drop. Any error (no listener,
/// stale socket) is swallowed — a hook must never block or fail the harness.
pub fn send(sock: &Path, ev: &Event) {
    if let Ok(mut s) = UnixStream::connect(sock) {
        let _ = s.set_write_timeout(Some(std::time::Duration::from_millis(200)));
        let mut line = match serde_json::to_string(ev) { Ok(l) => l, Err(_) => return };
        line.push('\n');
        let _ = s.write_all(line.as_bytes());
    }
}

/// Apply one received event to the engine under the caller's lock.
pub fn apply(engine: &mut Engine, window: &mut TouchWindow, ring: &TraceRing, ev: &Event)
    -> Result<(), String>
{
    match ev {
        Event::Used { path } => {
            // ingest_touch no-ops when the engine is wedged (read-only); skip
            // the credit loop and the follow-up save too so a wedged db does
            // no further pointless (guaranteed-failing) work.
            ingest_touch(engine, window, path)?;
            if engine.wedged.is_none() {
                // Credit any live trace that surfaced this path (explicit mode).
                let hit = 1.0f32;
                let ts = now_ms();
                let traces: Vec<_> = ring.iter().cloned().collect();
                for tr in &traces {
                    let surfaced_here = tr.surfaced().iter()
                        .any(|(id, _)| engine.pg.graph().node_name(*id) == path);
                    if surfaced_here {
                        let credit = Credit::Nodes(vec![(path.clone(), hit)]);
                        engine.layer.apply_credit(engine.pg.graph(), tr, &credit, ts);
                    }
                }
                // Required second save: ingest_touch already saved once above,
                // but apply_credit (just above) ran after that save, so its
                // updates need their own persist — without this, learning
                // from credit application would be silently dropped.
                engine.save_sidecar()?;
            }
            Ok(())
        }
        Event::Compact => {
            let ts = now_ms();
            let _ = engine.layer.consolidate(engine.pg.graph(), ts);
            engine.save_sidecar()?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_json_roundtrips() {
        let ev = Event::Used { path: "src/a.rs".into() };
        let line = serde_json::to_string(&ev).unwrap();
        assert!(line.contains("\"kind\":\"used\""));
        let back: Event = serde_json::from_str(&line).unwrap();
        matches!(back, Event::Used { .. }).then_some(()).unwrap();

        let c: Event = serde_json::from_str(r#"{"kind":"compact"}"#).unwrap();
        matches!(c, Event::Compact).then_some(()).unwrap();
    }

    #[test]
    fn send_is_silent_when_no_listener() {
        let missing = std::env::temp_dir().join("psyrag-nope.sock");
        // must not panic / must not block
        send(&missing, &Event::Compact);
    }
}
