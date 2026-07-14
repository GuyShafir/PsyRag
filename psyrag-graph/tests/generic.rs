//! Domain-agnostic proof: a home-automation inventory (nothing cloud about
//! it) through the generic entity format, including the edge-retargeting
//! case where both endpoints stay alive — the snapshot must retire the old
//! edge even though neither node died.

use psyrag_graph::entity::ingest_entities_mem;
use psyrag_graph::{Direction, PersistentGraph, TemporalGraph};

const T1: i64 = 1_000;
const T2: i64 = 2_000;

/// At t1 the motion sensor triggers the kitchen lights automation; at t2 it's
/// rewired to the alarm automation. Both automations exist at both times.
fn snapshot(target: &str) -> String {
    serde_json::json!([
        { "name": "hub/main", "type": "zigbee/Hub",
          "props": { "fw": "3.1" } },
        { "name": "automation/kitchen-lights", "type": "ha/Automation",
          "props": { "enabled": true } },
        { "name": "automation/alarm", "type": "ha/Automation",
          "props": { "enabled": true } },
        { "name": "sensor/kitchen-motion", "type": "zigbee/MotionSensor",
          "props": { "battery": 87 },
          "edges": [
            { "dst": "hub/main", "kind": "PAIRED_WITH" },
            { "dst": target, "kind": "TRIGGERS" }
          ] }
    ])
    .to_string()
}

#[test]
fn generic_domain_end_to_end() {
    let mut g = TemporalGraph::new();
    let z1 = ingest_entities_mem(&mut g, &snapshot("automation/kitchen-lights"), T1, true).unwrap();
    assert!(z1.is_empty());
    let z2 = ingest_entities_mem(&mut g, &snapshot("automation/alarm"), T2, true).unwrap();
    assert!(z2.is_empty()); // nothing died — this is pure rewiring

    // The retargeting fix: sensor->kitchen-lights must be closed at t2 even
    // though both endpoints are alive.
    let d = g.diff(T1 + 1, T2 + 1);
    assert!(d
        .edges_removed
        .iter()
        .any(|e| e.contains("TRIGGERS") && e.contains("kitchen-lights")));
    assert!(d
        .edges_added
        .iter()
        .any(|e| e.contains("TRIGGERS") && e.contains("alarm")));
    assert!(d.nodes_removed.is_empty());

    // Unrelated edges of the same source survive (they were re-asserted).
    let hits = g.blast_radius("sensor/kitchen-motion", T2 + 1, Direction::Down, 2);
    let names: Vec<&str> = hits.iter().map(|r| r.node.as_str()).collect();
    assert!(names.contains(&"hub/main"));
    assert!(names.contains(&"automation/alarm"));
    assert!(!names.contains(&"automation/kitchen-lights"));

    // Time travel still sees the old wiring.
    let then = g.blast_radius("sensor/kitchen-motion", T1 + 1, Direction::Down, 2);
    assert!(then.iter().any(|r| r.node == "automation/kitchen-lights"));
}

#[test]
fn partial_snapshot_is_not_evidence_of_absence() {
    let mut g = TemporalGraph::new();
    ingest_entities_mem(&mut g, &snapshot("automation/kitchen-lights"), T1, true).unwrap();
    // Incremental observation of ONE entity with reconcile=false: nothing
    // else may be retired, and un-reasserted edges of other nodes survive.
    let one = serde_json::json!([
        { "name": "hub/main", "type": "zigbee/Hub", "props": { "fw": "3.2" } }
    ])
    .to_string();
    let z = ingest_entities_mem(&mut g, &one, T2, false).unwrap();
    assert!(z.is_empty());
    let d = g.diff(T1 + 1, T2 + 1);
    assert_eq!(d.nodes_changed, vec!["hub/main".to_string()]);
    assert!(d.nodes_removed.is_empty());
    assert!(d.edges_removed.is_empty());
}

#[test]
fn generic_entities_persist_through_wal() {
    let path = std::env::temp_dir().join("psyrag_generic.wal");
    let _ = std::fs::remove_file(&path);
    {
        let mut pg = PersistentGraph::open(&path).unwrap();
        pg.ingest_entities(&snapshot("automation/kitchen-lights"), T1, true).unwrap();
        pg.ingest_entities(&snapshot("automation/alarm"), T2, true).unwrap();
    }
    let pg = PersistentGraph::open(&path).unwrap();
    assert_eq!(pg.replay_skipped, 0);
    // Replayed history includes the reconciled edge retirement.
    let d = pg.graph().diff(T1 + 1, T2 + 1);
    assert!(d.edges_removed.iter().any(|e| e.contains("kitchen-lights")));
    let _ = std::fs::remove_file(&path);
}
