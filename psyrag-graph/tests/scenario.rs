#![cfg(feature = "gcp")]
//! End-to-end scenario: a small GCP project observed at t1, mutated, and
//! re-observed at t2. Exercises ingestion, containment/reference extraction,
//! placeholder upgrade, as-of queries, diff, blast radius (up/down), cycle
//! safety, and zombie pruning.

use psyrag_graph::gcp::ingest_snapshot;
use psyrag_graph::{Direction, TemporalGraph};

const T1: i64 = 1_000;
const T2: i64 = 2_000;

fn snapshot_t1() -> String {
    serde_json::json!([
        {
            "name": "//cloudresourcemanager.googleapis.com/projects/lumili-prod",
            "assetType": "cloudresourcemanager.googleapis.com/Project",
            "ancestors": ["projects/lumili-prod", "organizations/999"],
            "resource": { "data": { "projectId": "lumili-prod" } }
        },
        {
            "name": "//compute.googleapis.com/projects/lumili-prod/global/networks/vpc-main",
            "assetType": "compute.googleapis.com/Network",
            "ancestors": ["projects/lumili-prod", "organizations/999"],
            "resource": { "data": { "name": "vpc-main" } }
        },
        {
            "name": "//compute.googleapis.com/projects/lumili-prod/regions/me-west1/subnetworks/subnet-a",
            "assetType": "compute.googleapis.com/Subnetwork",
            "ancestors": ["projects/lumili-prod", "organizations/999"],
            "resource": { "data": {
                "name": "subnet-a",
                "network": "https://www.googleapis.com/compute/v1/projects/lumili-prod/global/networks/vpc-main"
            } }
        },
        {
            "name": "//run.googleapis.com/projects/lumili-prod/locations/me-west1/services/api",
            "assetType": "run.googleapis.com/Service",
            "ancestors": ["projects/lumili-prod", "organizations/999"],
            "resource": { "data": {
                "name": "api",
                "template": {
                    "vpcAccess": {
                        "networkInterfaces": [{
                            "subnetwork": "//compute.googleapis.com/projects/lumili-prod/regions/me-west1/subnetworks/subnet-a"
                        }]
                    }
                },
                "image": "me-west1-docker.pkg.dev/lumili-prod/app/api:v1"
            } }
        },
        {
            "name": "//sqladmin.googleapis.com/projects/lumili-prod/instances/db-main",
            "assetType": "sqladmin.googleapis.com/Instance",
            "ancestors": ["projects/lumili-prod", "organizations/999"],
            "resource": { "data": {
                "name": "db-main",
                "settings": { "ipConfiguration": {
                    "privateNetwork": "//compute.googleapis.com/projects/lumili-prod/global/networks/vpc-main"
                } }
            } }
        }
    ])
    .to_string()
}

/// t2: Cloud Run image bumped to v2, subnet-a DELETED (zombie test),
/// a new subnet-b appears.
fn snapshot_t2() -> String {
    serde_json::json!([
        {
            "name": "//cloudresourcemanager.googleapis.com/projects/lumili-prod",
            "assetType": "cloudresourcemanager.googleapis.com/Project",
            "ancestors": ["projects/lumili-prod", "organizations/999"],
            "resource": { "data": { "projectId": "lumili-prod" } }
        },
        {
            "name": "//compute.googleapis.com/projects/lumili-prod/global/networks/vpc-main",
            "assetType": "compute.googleapis.com/Network",
            "ancestors": ["projects/lumili-prod", "organizations/999"],
            "resource": { "data": { "name": "vpc-main" } }
        },
        {
            "name": "//compute.googleapis.com/projects/lumili-prod/regions/me-west1/subnetworks/subnet-b",
            "assetType": "compute.googleapis.com/Subnetwork",
            "ancestors": ["projects/lumili-prod", "organizations/999"],
            "resource": { "data": {
                "name": "subnet-b",
                "network": "https://www.googleapis.com/compute/v1/projects/lumili-prod/global/networks/vpc-main"
            } }
        },
        {
            "name": "//run.googleapis.com/projects/lumili-prod/locations/me-west1/services/api",
            "assetType": "run.googleapis.com/Service",
            "ancestors": ["projects/lumili-prod", "organizations/999"],
            "resource": { "data": {
                "name": "api",
                "template": {
                    "vpcAccess": {
                        "networkInterfaces": [{
                            "subnetwork": "//compute.googleapis.com/projects/lumili-prod/regions/me-west1/subnetworks/subnet-b"
                        }]
                    }
                },
                "image": "me-west1-docker.pkg.dev/lumili-prod/app/api:v2"
            } }
        },
        {
            "name": "//sqladmin.googleapis.com/projects/lumili-prod/instances/db-main",
            "assetType": "sqladmin.googleapis.com/Instance",
            "ancestors": ["projects/lumili-prod", "organizations/999"],
            "resource": { "data": {
                "name": "db-main",
                "settings": { "ipConfiguration": {
                    "privateNetwork": "//compute.googleapis.com/projects/lumili-prod/global/networks/vpc-main"
                } }
            } }
        }
    ])
    .to_string()
}

const SUBNET_A: &str =
    "//compute.googleapis.com/projects/lumili-prod/regions/me-west1/subnetworks/subnet-a";
const VPC: &str = "//compute.googleapis.com/projects/lumili-prod/global/networks/vpc-main";
const API: &str = "//run.googleapis.com/projects/lumili-prod/locations/me-west1/services/api";
const DB: &str = "//sqladmin.googleapis.com/projects/lumili-prod/instances/db-main";

fn build() -> TemporalGraph {
    let mut g = TemporalGraph::new();
    let pruned_t1 = ingest_snapshot(&mut g, &snapshot_t1(), T1).unwrap();
    assert!(pruned_t1.is_empty());
    let pruned_t2 = ingest_snapshot(&mut g, &snapshot_t2(), T2).unwrap();
    // zombie pruning: subnet-a was deleted between snapshots
    assert_eq!(pruned_t2, vec![SUBNET_A.to_string()]);
    g
}

#[test]
fn placeholder_upgraded_by_real_record() {
    let g = build();
    // subnet-a referenced vpc-main before... actually vpc-main is ingested in
    // the same snapshot; either order must converge to the real type.
    let id = g.id_of(VPC).unwrap();
    let node = g.node(id);
    assert!(!node.placeholder);
    assert_eq!(
        g.types.resolve(node.type_id),
        "compute.googleapis.com/Network"
    );
}

#[test]
fn as_of_and_zombie_pruning() {
    let g = build();
    let id = g.id_of(SUBNET_A).unwrap();
    assert!(g.node(id).alive_at(T1 + 1));
    assert!(!g.node(id).alive_at(T2 + 1)); // pruned, not a zombie
}

#[test]
fn diff_answers_what_changed() {
    let g = build();
    let d = g.diff(T1 + 1, T2 + 1);
    assert!(d.nodes_removed.contains(&SUBNET_A.to_string()));
    assert!(d.nodes_added.iter().any(|n| n.contains("subnet-b")));
    assert!(d.nodes_changed.contains(&API.to_string())); // image v1 -> v2
                                                         // rewired reference: api -> subnet-a removed, api -> subnet-b added
    assert!(d.edges_removed.iter().any(|e| e.contains("subnet-a")));
    assert!(d.edges_added.iter().any(|e| e.contains("subnet-b")));
    // idempotence: nothing changed between two points after t2
    assert!(g.diff(T2 + 1, T2 + 2).is_empty());
}

#[test]
fn blast_radius_upstream_from_db() {
    // "if vpc-main breaks, what is affected?" — everything that references it,
    // transitively, is upstream traversal (Up = who points at me).
    let g = build();
    let hit = g.blast_radius(VPC, T1 + 1, Direction::Up, 5);
    let names: Vec<&str> = hit.iter().map(|r| r.node.as_str()).collect();
    assert!(names.contains(&SUBNET_A)); // subnet -> REFERENCES -> vpc
    assert!(names.contains(&DB)); // db -> REFERENCES -> vpc
    assert!(names.contains(&API)); // api -> subnet -> vpc (depth 2)
    let api_hit = hit.iter().find(|r| r.node == API).unwrap();
    assert_eq!(api_hit.depth, 2);
    // path explainability: full chain rendered for prompt injection
    assert!(api_hit.path.contains("REFERENCES"));
    assert!(api_hit.path.contains(SUBNET_A));
}

#[test]
fn blast_radius_is_temporal() {
    let g = build();
    // At t2, api reaches vpc via subnet-b, not subnet-a.
    let hit = g.blast_radius(API, T2 + 1, Direction::Down, 5);
    let names: Vec<&str> = hit.iter().map(|r| r.node.as_str()).collect();
    assert!(names.iter().any(|n| n.contains("subnet-b")));
    assert!(!names.contains(&SUBNET_A));
}

#[test]
fn cycles_do_not_hang() {
    // Cloud graphs have cycles (peering, IAM). Synthesize one and make sure
    // BFS terminates and visits each node once.
    let mut g = TemporalGraph::new();
    let a = g.observe_node("a", "t/x", serde_json::json!({}), 1);
    let b = g.observe_node("b", "t/x", serde_json::json!({}), 1);
    g.observe_edge(a, b, "PEERS_WITH", 1);
    g.observe_edge(b, a, "PEERS_WITH", 1);
    let hit = g.blast_radius("a", 2, Direction::Both, 10);
    assert_eq!(hit.len(), 1);
    assert_eq!(hit[0].node, "b");
}
