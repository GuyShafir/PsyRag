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
    let path = std::env::temp_dir().join("driftgraph_roundtrip.wal");
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
    let path = std::env::temp_dir().join("driftgraph_torn.wal");
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

    let pg = PersistentGraph::open(&path).unwrap();
    assert_eq!(pg.replay_skipped, 1);
    assert!(pg.graph().id_of("a").is_some());
    assert!(pg.graph().id_of("b").is_none());

    let _ = fs::remove_file(&path);
}
