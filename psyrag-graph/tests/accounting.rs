//! Memory-estimate accounting: approx_bytes must grow with content and
//! reset on purge's in-place rebuild (quota decisions depend on it).

use psyrag_graph::{PersistentGraph, TemporalGraph};

#[test]
fn approx_bytes_tracks_growth() {
    let mut g = TemporalGraph::new();
    let b0 = g.approx_bytes();
    psyrag_graph::entity::ingest_entities_mem(
        &mut g,
        r#"[{"name":"a","type":"t","props":{"blob":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"},
             "edges":[{"dst":"b","kind":"REL"}]}]"#,
        0,
        false,
    )
    .unwrap();
    let b1 = g.approx_bytes();
    assert!(b1 > b0 + 100, "grew with content: {b0} -> {b1}");
    // re-observing identical props is a no-op and must not inflate the estimate
    psyrag_graph::entity::ingest_entities_mem(
        &mut g,
        r#"[{"name":"a","type":"t","props":{"blob":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"},
             "edges":[{"dst":"b","kind":"REL"}]}]"#,
        1,
        false,
    )
    .unwrap();
    assert_eq!(
        g.approx_bytes(),
        b1,
        "idempotent re-observation adds nothing"
    );
}

#[test]
fn approx_bytes_shrinks_after_purge() {
    let path = std::env::temp_dir().join("psyrag_acct_purge.wal");
    let _ = std::fs::remove_file(&path);
    let mut pg = PersistentGraph::open(&path).unwrap();
    pg.ingest_entities_from(
        r#"[{"name":"big","type":"t","origin":"user:x",
             "props":{"blob":"yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"},
             "edges":[{"dst":"z","kind":"REL"}]},
            {"name":"keep","type":"t","origin":"user:y"}]"#,
        0,
        false,
        None,
    )
    .unwrap();
    let before = pg.graph().approx_bytes();
    pg.purge("user:x").unwrap();
    let after = pg.graph().approx_bytes();
    assert!(
        after < before,
        "purge rebuild shrinks the estimate: {before} -> {after}"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn token_index_matches_by_token_prefix_deterministically() {
    let mut g = TemporalGraph::new();
    psyrag_graph::entity::ingest_entities_mem(
        &mut g,
        r#"[{"name":"svc/metering-api","type":"t"},
            {"name":"svc/billing","type":"t"},
            {"name":"db/metrics-store","type":"t"}]"#,
        0,
        false,
    )
    .unwrap();
    let names = |ids: Vec<u32>| -> Vec<String> {
        ids.into_iter()
            .map(|i| g.node_name(i).to_string())
            .collect()
    };
    // exact token
    assert_eq!(
        names(g.match_tokens(&["billing".into()], 10)),
        ["svc/billing"]
    );
    // token prefix spans multiple nodes, ascending NodeId order
    assert_eq!(
        names(g.match_tokens(&["met".into()], 10)),
        ["svc/metering-api", "db/metrics-store"]
    );
    // case-insensitive, multi-token union, limit respected
    assert_eq!(g.match_tokens(&["SVC".into(), "db".into()], 2).len(), 2);
    // mid-token substring does NOT match in token mode (documented change)
    assert!(g.match_tokens(&["eter".into()], 10).is_empty());
}
