//! Memory + disk reality check. Run: cargo run --release --example membench
//!
//! Simulates a large org: 500k assets with realistic ~1KB props (mix of
//! per-node-unique and fleet-shared config), 1M edges, then a second
//! snapshot with 2% drift. Reports RSS, WAL size, replay time.

use psyrag_graph::{Op, OpSink, PersistentGraph};
use serde_json::json;
use std::time::Instant;

fn rss_mb() -> f64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap();
    let kb: f64 = s
        .lines()
        .find(|l| l.starts_with("VmRSS"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0);
    kb / 1024.0
}

/// Realistic-ish resource payload: ~1KB serialized. `variant` controls how
/// much is fleet-shared vs unique: real clouds have thousands of nodes with
/// near-identical config differing only in name/IP.
fn props(node: usize, variant: usize) -> serde_json::Value {
    let env = ["prod", "staging", "dev"][variant % 3];
    let mt = [
        "e2-standard-2",
        "e2-standard-4",
        "e2-standard-8",
        "e2-standard-16",
    ][variant % 4];
    json!({
        "name": format!("resource-{node}"),
        "selfLink": format!("https://www.googleapis.com/compute/v1/projects/p/zones/z/instances/resource-{node}"),
        "machineType": mt,
        "status": "RUNNING",
        "networkInterfaces": [{
            "network": "https://www.googleapis.com/compute/v1/projects/p/global/networks/vpc-main",
            "networkIP": format!("10.0.{}.{}", node / 256 % 256, node % 256),
            "stackType": "IPV4_ONLY",
            "subnetwork": format!("https://www.googleapis.com/compute/v1/projects/p/regions/r/subnetworks/subnet-{}", variant % 20),
        }],
        "disks": [{
            "boot": true, "autoDelete": true, "mode": "READ_WRITE",
            "type": "PERSISTENT", "diskSizeGb": "100",
            "interface": "SCSI",
            "licenses": ["https://www.googleapis.com/compute/v1/projects/debian-cloud/global/licenses/debian-12"],
        }],
        "scheduling": {"onHostMaintenance": "MIGRATE", "automaticRestart": true, "preemptible": false},
        "labels": {"env": env, "team": format!("team-{}", variant % 12), "managed-by": "terraform"},
        "serviceAccounts": [{"email": format!("sa-{}@p.iam.gserviceaccount.com", variant % 8),
                             "scopes": ["https://www.googleapis.com/auth/cloud-platform"]}],
        "shieldedInstanceConfig": {"enableSecureBoot": true, "enableVtpm": true, "enableIntegrityMonitoring": true},
        "cpuPlatform": "Intel Cascade Lake",
        "fingerprint": format!("fp-{:016x}", node * 2654435761usize),
    })
}

fn main() {
    const N: usize = 500_000;
    let wal_path = "/tmp/membench.wal";
    let _ = std::fs::remove_file(wal_path);

    let base = rss_mb();
    let mut pg = PersistentGraph::open(wal_path).unwrap();

    // Snapshot 1
    let t = Instant::now();
    for i in 0..N {
        OpSink::record(
            &mut pg,
            Op::ObserveNode {
                origin: None,
                name: format!("//compute.googleapis.com/projects/p/zones/z/instances/i{i}"),
                asset_type: "compute.googleapis.com/Instance".into(),
                props: props(i, i % 100), // 100 config variants across the fleet
                ts: 1_000,
            },
        )
        .unwrap();
        // 2 edges per node: containment + subnet reference
        OpSink::record(
            &mut pg,
            Op::ObserveEdge {
                origin: None,
                src: format!("//crm/projects/p"),
                dst: format!("//compute.googleapis.com/projects/p/zones/z/instances/i{i}"),
                kind: "CONTAINS".into(),
                ts: 1_000,
            },
        )
        .unwrap();
        OpSink::record(
            &mut pg,
            Op::ObserveEdge {
                origin: None,
                src: format!("//compute.googleapis.com/projects/p/zones/z/instances/i{i}"),
                dst: format!("//compute/subnet-{}", i % 20),
                kind: "REFERENCES".into(),
                ts: 1_000,
            },
        )
        .unwrap();
    }
    pg.flush().unwrap();
    let ingest1 = t.elapsed();

    // Snapshot 2: 2% drift (re-observe with changed props)
    let t = Instant::now();
    for i in (0..N).step_by(50) {
        let mut p = props(i, i % 100);
        p["status"] = json!("TERMINATED");
        OpSink::record(
            &mut pg,
            Op::ObserveNode {
                origin: None,
                name: format!("//compute.googleapis.com/projects/p/zones/z/instances/i{i}"),
                asset_type: "compute.googleapis.com/Instance".into(),
                props: p,
                ts: 2_000,
            },
        )
        .unwrap();
    }
    pg.flush().unwrap();
    let drift = t.elapsed();

    let mem = rss_mb() - base;
    let wal_size = std::fs::metadata(wal_path).unwrap().len() as f64 / 1e6;

    // Query timings
    let t = Instant::now();
    let d = pg.graph().diff(1_001, 2_001);
    let diff_ms = t.elapsed().as_millis();

    let t = Instant::now();
    let br = pg
        .graph()
        .blast_radius("//compute/subnet-0", 2_001, psyrag_graph::Direction::Up, 3);
    let br_us = t.elapsed().as_micros();

    println!(
        "nodes={} edges={} versions~{}",
        pg.graph().node_count(),
        pg.graph().edge_count(),
        N + N / 50
    );
    println!(
        "RSS delta: {:.0} MB ({:.1} KB/asset)",
        mem,
        mem * 1024.0 / N as f64
    );
    println!("WAL on disk: {:.0} MB", wal_size);
    println!(
        "ingest snapshot1: {:.1}s | drift pass: {:.2}s",
        ingest1.as_secs_f64(),
        drift.as_secs_f64()
    );
    println!(
        "diff: {}ms | blast radius ({} hits): {}us",
        diff_ms,
        br.len(),
        br_us
    );
    drop(pg);

    // Replay cost (the compaction question)
    let t = Instant::now();
    let pg2 = PersistentGraph::open(wal_path).unwrap();
    println!(
        "WAL replay ({} nodes): {:.1}s",
        pg2.graph().node_count(),
        t.elapsed().as_secs_f64()
    );
    let _ = std::fs::remove_file(wal_path);
}
