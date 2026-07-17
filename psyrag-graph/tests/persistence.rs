#![cfg(feature = "gcp")]
//! WAL roundtrip: build a graph through PersistentGraph, drop it, reopen from
//! the log, and verify the replayed state answers temporal queries
//! identically — including history that predates the reopen.

use psyrag_graph::{Direction, Op, PersistentGraph};
use std::fs;
use std::io::Write;

const T1: i64 = 1_000;
const T2: i64 = 2_000;

fn mini_snapshot(image: &str, subnet: &str) -> String {
    serde_json::json!([
        {
            "name": "//compute.googleapis.com/projects/p/global/networks/vpc",
            "assetType": "compute.googleapis.com/Network",
            "ancestors": ["projects/p"],
            "resource": { "data": { "name": "vpc" } }
        },
        {
            "name": format!("//compute.googleapis.com/projects/p/regions/r/subnetworks/{subnet}"),
            "assetType": "compute.googleapis.com/Subnetwork",
            "ancestors": ["projects/p"],
            "resource": { "data": {
                "network": "https://www.googleapis.com/compute/v1/projects/p/global/networks/vpc"
            } }
        },
        {
            "name": "//run.googleapis.com/projects/p/locations/r/services/api",
            "assetType": "run.googleapis.com/Service",
            "ancestors": ["projects/p"],
            "resource": { "data": {
                "image": image,
                "subnet": format!("//compute.googleapis.com/projects/p/regions/r/subnetworks/{subnet}")
            } }
        }
    ])
    .to_string()
}

const API: &str = "//run.googleapis.com/projects/p/locations/r/services/api";
const VPC: &str = "//compute.googleapis.com/projects/p/global/networks/vpc";

#[test]
fn wal_roundtrip_preserves_history() {
    let path = std::env::temp_dir().join("psyrag_roundtrip.wal");
    let _ = fs::remove_file(&path);

    // Session 1: two snapshots with drift (image bump, subnet swap).
    {
        let mut pg = PersistentGraph::open(&path).unwrap();
        let z1 = pg.ingest_cai_snapshot(&mini_snapshot("img:v1", "subnet-a"), T1).unwrap();
        assert!(z1.is_empty());
        let z2 = pg.ingest_cai_snapshot(&mini_snapshot("img:v2", "subnet-b"), T2).unwrap();
        assert_eq!(z2.len(), 1); // subnet-a pruned
    } // dropped: WAL is the only surviving state

    // Session 2: replay.
    let pg = PersistentGraph::open(&path).unwrap();
    assert_eq!(pg.replay_skipped, 0);
    let g = pg.graph();

    // History predating the reopen is intact.
    let d = g.diff(T1 + 1, T2 + 1);
    assert!(d.nodes_changed.contains(&API.to_string()));
    assert!(d.nodes_removed.iter().any(|n| n.contains("subnet-a")));
    assert!(d.nodes_added.iter().any(|n| n.contains("subnet-b")));

    // Temporal blast radius still works at both times.
    let at_t1 = g.blast_radius(API, T1 + 1, Direction::Down, 5);
    assert!(at_t1.iter().any(|r| r.node.contains("subnet-a")));
    let at_t2 = g.blast_radius(API, T2 + 1, Direction::Down, 5);
    assert!(at_t2.iter().any(|r| r.node.contains("subnet-b")));
    assert!(at_t2.iter().any(|r| r.node == VPC)); // 2 hops via subnet-b

    let _ = fs::remove_file(&path);
}

#[test]
fn torn_tail_is_tolerated() {
    let path = std::env::temp_dir().join("psyrag_torn.wal");
    let _ = fs::remove_file(&path);
    {
        let mut pg = PersistentGraph::open(&path).unwrap();
        pg.record_op(Op::ObserveNode {
            name: "a".into(),
            asset_type: "t/x".into(),
            props: serde_json::json!({"k": 1}),
            ts: T1,
        })
        .unwrap();
        pg.flush().unwrap();
    }
    // Simulate a crash mid-append.
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    f.write_all(b"{\"op\":\"observe_node\",\"name\":\"b\",\"asset_t").unwrap();
    drop(f);

    {
        let pg = PersistentGraph::open(&path).unwrap();
        assert_eq!(pg.replay_skipped, 1);
        assert!(pg.graph().id_of("a").is_some());
        assert!(pg.graph().id_of("b").is_none());
    }
    // The torn tail was truncated on recovery: a clean reopen sees no skips
    // and the recovered state is unchanged.
    let pg = PersistentGraph::open(&path).unwrap();
    assert_eq!(pg.replay_skipped, 0);
    assert!(pg.graph().id_of("a").is_some());

    let _ = fs::remove_file(&path);
}

#[test]
fn mid_file_corruption_is_a_hard_error() {
    let path = std::env::temp_dir().join("psyrag_midcorrupt.wal");
    let _ = fs::remove_file(&path);
    {
        let mut pg = PersistentGraph::open(&path).unwrap();
        for n in ["a", "b", "c"] {
            pg.record_op(Op::ObserveNode {
                name: n.into(),
                asset_type: "t/x".into(),
                props: serde_json::Value::Null,
                ts: T1,
            })
            .unwrap();
        }
        pg.flush().unwrap();
    }
    // Flip a byte in the middle of the file (inside record "b"'s json).
    let mut bytes = fs::read(&path).unwrap();
    let pos = bytes
        .windows(3)
        .position(|w| w == b"\"b\"")
        .expect("record b present");
    bytes[pos + 1] = b'X';
    // keep length identical so only the CRC breaks
    fs::write(&path, &bytes).unwrap();

    let err = match PersistentGraph::open(&path) {
        Ok(_) => panic!("mid-file corruption must not replay"),
        Err(e) => e,
    };
    assert!(err.contains("corrupted"), "unexpected error: {err}");

    let _ = fs::remove_file(&path);
}

#[test]
fn second_opener_is_refused_while_locked() {
    let path = std::env::temp_dir().join("psyrag_locked.wal");
    let _ = fs::remove_file(&path);
    let _holder = PersistentGraph::open(&path).unwrap();
    let err = match PersistentGraph::open(&path) {
        Ok(_) => panic!("second opener must be refused"),
        Err(e) => e,
    };
    assert!(err.contains("locked"), "unexpected error: {err}");
    drop(_holder);
    // Released on drop.
    assert!(PersistentGraph::open(&path).is_ok());
    let _ = fs::remove_file(&path);
}

#[test]
fn compaction_preserves_open_state_and_drops_history() {
    let path = std::env::temp_dir().join("psyrag_compact.wal");
    let _ = fs::remove_file(&path);
    let (report, live_nodes_before, live_edges_before) = {
        let mut pg = PersistentGraph::open(&path).unwrap();
        // Two snapshots with churn: subnet-a exists at T1, replaced at T2.
        pg.ingest_cai_snapshot(&mini_snapshot("img:v1", "subnet-a"), T1).unwrap();
        pg.ingest_cai_snapshot(&mini_snapshot("img:v2", "subnet-b"), T2).unwrap();
        let g = pg.graph();
        let live_nodes = g.alive_at(T2 + 1).len();
        let live_edges = (0..g.edge_count())
            .filter(|&e| g.edge(e as u32).alive_at(T2 + 1))
            .count();
        let report = pg.compact(true).unwrap();
        // The compacted log still replays through the SAME handle's appends:
        pg.record_op(Op::ObserveNode {
            name: "post-compact".into(),
            asset_type: "t/x".into(),
            props: serde_json::Value::Null,
            ts: T2 + 10,
        })
        .unwrap();
        pg.flush().unwrap();
        (report, live_nodes, live_edges)
    };
    assert!(report.bytes_after < report.bytes_before, "log shrank: {report:?}");
    let archive = report.archive.clone().expect("archive kept");
    assert!(std::path::Path::new(&archive).exists());

    // Replay the compacted log: open state identical, history gone.
    let pg = PersistentGraph::open(&path).unwrap();
    assert_eq!(pg.replay_skipped, 0);
    let g = pg.graph();
    assert_eq!(g.alive_at(T2 + 1).len(), live_nodes_before);
    let live_edges_after = (0..g.edge_count())
        .filter(|&e| g.edge(e as u32).alive_at(T2 + 1))
        .count();
    assert_eq!(live_edges_after, live_edges_before);
    // valid_from preserved through compaction (stable-key prerequisite)
    assert!(g.blast_radius(API, T2 + 1, Direction::Down, 5).iter().any(|r| r.node.contains("subnet-b")));
    // dropped: subnet-a (retired before compaction) is no longer known at all
    assert!(g.id_of(&format!("//compute.googleapis.com/projects/p/regions/r/subnetworks/subnet-a")).is_none());
    // the post-compaction append made it through the adopted fd
    assert!(g.id_of("post-compact").is_some());

    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(&archive);
}

#[test]
fn legacy_v0_wal_replays_and_mixes_with_framed_appends() {
    let path = std::env::temp_dir().join("psyrag_legacy.wal");
    let _ = fs::remove_file(&path);
    // Hand-write a v0 (plain NDJSON, headerless) log.
    fs::write(
        &path,
        concat!(
            "{\"op\":\"observe_node\",\"name\":\"old\",\"asset_type\":\"t/x\",\"props\":null,\"ts\":1000}\n",
            "{\"op\":\"observe_edge\",\"src\":\"old\",\"dst\":\"tgt\",\"kind\":\"CALLS\",\"ts\":1000}\n",
        ),
    )
    .unwrap();
    {
        let mut pg = PersistentGraph::open(&path).unwrap();
        assert_eq!(pg.replay_skipped, 0);
        assert!(pg.graph().id_of("old").is_some());
        assert!(pg.graph().id_of("tgt").is_some());
        // Append through the current (framed) format.
        pg.record_op(Op::ObserveNode {
            name: "new".into(),
            asset_type: "t/x".into(),
            props: serde_json::Value::Null,
            ts: T2,
        })
        .unwrap();
        pg.flush().unwrap();
    }
    // Mixed legacy + framed file replays fully.
    let pg = PersistentGraph::open(&path).unwrap();
    assert_eq!(pg.replay_skipped, 0);
    assert!(pg.graph().id_of("old").is_some());
    assert!(pg.graph().id_of("new").is_some());
    let _ = fs::remove_file(&path);
}
