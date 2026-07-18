//! Format fixture zoo: on-disk WAL/sidecar files from every released format
//! version, replayed forever. If a change breaks reading any historical
//! format, or fails to refuse a FUTURE format loudly (the downgrade story),
//! this is where it surfaces. Fixtures live in psyrag-graph/tests/fixtures/;
//! regenerate one ONLY when introducing a new version, never edit in place.

use psyrag_core::PlasticityLayer;
use psyrag_graph::persist::{replay_readonly, verify_wal};
use psyrag_graph::PersistentGraph;
use std::fs;
use std::path::PathBuf;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../psyrag-graph/tests/fixtures")
        .join(name)
}

/// Copy a fixture to a temp path first: open() takes the lock and may repair
/// (append a newline / truncate a torn tail); committed fixtures must never
/// be mutated by the test run.
fn temp_copy(name: &str, tag: &str) -> PathBuf {
    let dst = std::env::temp_dir().join(format!("psyrag_fixture_{tag}_{name}"));
    let _ = fs::remove_file(&dst);
    fs::copy(fixture(name), &dst).unwrap();
    dst
}

#[test]
fn v0_legacy_wal_replays() {
    let p = temp_copy("v0-legacy.wal", "v0");
    let pg = PersistentGraph::open(&p).unwrap();
    assert_eq!(pg.replay_skipped, 0);
    let g = pg.graph();
    assert!(g.id_of("x").is_some() && g.id_of("y").is_some() && g.id_of("z").is_some());
    assert_eq!(g.edge_count(), 2);
    let _ = fs::remove_file(&p);
}

#[test]
fn v1_framed_wal_replays_and_verifies() {
    let p = temp_copy("v1-framed.wal", "verify");
    let rep = verify_wal(&p).unwrap();
    assert!(rep.corrupt.is_none() && !rep.torn_tail);
    assert!(rep.framed > 0 && rep.legacy == 0);
    assert!(rep.wal_id.is_some(), "v1 fixtures carry a lineage id");
    let pg = PersistentGraph::open(&p).unwrap();
    assert_eq!(pg.replay_skipped, 0);
    assert_eq!(pg.wal_id().map(String::from), rep.wal_id);
    assert_eq!(pg.lsn(), rep.records as u64);
    assert_eq!(pg.graph().edge_count(), 2);
    let _ = fs::remove_file(&p);
}

#[test]
fn future_version_wal_refuses_loudly() {
    let p = temp_copy("future-version.wal", "future");
    let err = match PersistentGraph::open(&p) {
        Ok(_) => panic!("a future-format WAL must refuse to open"),
        Err(e) => e,
    };
    assert!(err.contains("newer"), "unexpected error: {err}");
    let _ = fs::remove_file(&p);
}

#[test]
fn sidecar_v1_positional_loads_against_v0_graph() {
    let g = replay_readonly(fixture("v0-legacy.wal")).unwrap();
    let mut layer = PlasticityLayer::new(psyrag_core::Config::default());
    layer
        .load_if_exists(&g, fixture("sidecar-v1.json").to_str().unwrap())
        .unwrap();
    layer.sync(&g);
    // positional: edge 0 (x->y) carries the fixture's 0.9 at its t_last
    assert!((layer.effective_weight(0, 1000) - 0.9).abs() < 1e-6);
}

#[test]
fn sidecar_v2_keyed_loads_against_v1_graph() {
    let g = replay_readonly(fixture("v1-framed.wal")).unwrap();
    let mut layer = PlasticityLayer::new(psyrag_core::Config::default());
    layer
        .load_if_exists(&g, fixture("sidecar-v2.json").to_str().unwrap())
        .unwrap();
    layer.sync(&g);
    // the fixture was produced by `feedback --used y` — the x->y edge is
    // reinforced above the w0 baseline of its sibling
    let xy = layer.edge_id(&g, "x", "y", "CALLS").unwrap();
    let yz = layer.edge_id(&g, "y", "z", "REFS").unwrap();
    let t = 2000;
    assert!(
        layer.effective_weight(xy, t) > layer.effective_weight(yz, t),
        "learned reinforcement present in the fixture"
    );
    assert!(
        layer.loaded_binding.is_some(),
        "v2 fixture carries WAL binding"
    );
}

#[test]
fn sidecar_future_version_refuses_loudly() {
    let g = replay_readonly(fixture("v0-legacy.wal")).unwrap();
    let mut layer = PlasticityLayer::new(psyrag_core::Config::default());
    let err = layer
        .load_if_exists(&g, fixture("sidecar-future.json").to_str().unwrap())
        .unwrap_err();
    assert!(err.contains("newer"), "unexpected error: {err}");
}

#[test]
fn replay_is_time_independent() {
    // Decay must derive from stored timestamps, never replay wall-clock:
    // replaying the same WAL+sidecar (at whatever wall time this test runs)
    // and querying at a FIXED ts reproduces the same result, byte for byte.
    let p = temp_copy("v1-framed.wal", "timeindep");
    let run = || {
        let pg = PersistentGraph::open(&p).unwrap();
        let mut layer = PlasticityLayer::new(psyrag_core::Config::default());
        layer.set_wal_binding(pg.wal_id(), pg.lsn());
        layer
            .load_if_exists(pg.graph(), fixture("sidecar-v2.json").to_str().unwrap())
            .unwrap();
        layer.sync(pg.graph());
        serde_json::to_string(&layer.retrieve(pg.graph(), &["x"], 2, 0.9, 10, 5000)).unwrap()
    };
    let a = run();
    std::thread::sleep(std::time::Duration::from_millis(50));
    let b = run();
    assert_eq!(
        a, b,
        "replay + fixed-ts retrieval is wall-clock independent"
    );
    let _ = fs::remove_file(&p);
}
