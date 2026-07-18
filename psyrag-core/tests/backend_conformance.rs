//! GraphBackend conformance suite. A managed backend (Spanner, AlloyDB, ...)
//! is correct exactly when these pass against it too — swap the constructor.

use psyrag_core::backend::{EdgeState, GraphBackend, InMemoryBackend};
use psyrag_core::{stable_edge_key, Config, PlasticityLayer};
use psyrag_graph::entity::{entity_ops, parse_entities};

fn backend_under_test() -> InMemoryBackend {
    InMemoryBackend::new()
}

fn seed(b: &mut impl GraphBackend) {
    let ents = parse_entities(
        r#"[{"name":"gw","type":"t","origin":"sys","edges":[
              {"dst":"a","kind":"CALLS"},{"dst":"b","kind":"CALLS"}]},
            {"name":"a","type":"t","edges":[{"dst":"deep","kind":"CALLS"}]},
            {"name":"island","type":"t","edges":[{"dst":"far","kind":"REL"}]}]"#,
    )
    .unwrap();
    for e in &ents {
        let (ops, _) = entity_ops(e, 1000, None);
        b.append_ops(&ops).unwrap();
    }
    b.checkpoint().unwrap();
}

#[test]
fn neighborhood_is_one_call_and_horizon_bounded() {
    let mut b = backend_under_test();
    seed(&mut b);
    let sub = b.neighborhood(&["gw"], 1, 2000).unwrap();
    // depth 1: gw + direct children, no `deep`, and never the island
    assert!(sub.id_of("gw").is_some() && sub.id_of("a").is_some() && sub.id_of("b").is_some());
    assert!(sub.id_of("deep").is_none(), "outside horizon is absent");
    assert!(sub.id_of("island").is_none() && sub.id_of("far").is_none());
    // provenance survives materialization
    let gw = sub.id_of("gw").unwrap();
    assert_eq!(sub.node_origin_at(gw, 2000), "sys");
    // depth 2 reaches deep
    let sub2 = b.neighborhood(&["gw"], 2, 2000).unwrap();
    assert!(sub2.id_of("deep").is_some());
}

#[test]
fn subgraph_is_directly_traversable_by_the_plasticity_layer() {
    let mut b = backend_under_test();
    seed(&mut b);
    // Access mode B from the architecture doc: one neighborhood() round
    // trip, then all retrieval/feedback happens in-process on the subgraph.
    let sub = b.neighborhood(&["gw"], 2, 2000).unwrap();
    let mut layer = PlasticityLayer::new(Config::default());
    layer.sync(&sub);
    let r = layer.retrieve(&sub, &["gw"], 2, 0.9, 10, 2000);
    assert!(r.mass > 0.0);
    assert!(r.top.iter().any(|n| n.node == "deep"));
}

#[test]
fn stable_keys_agree_between_store_and_subgraph() {
    // The weight-traffic contract: keys computed on a materialized subgraph
    // must equal keys computed on the backing store, or sleep-time flushes
    // would write to the wrong rows.
    let mut b = backend_under_test();
    seed(&mut b);
    let sub = b.neighborhood(&["gw"], 1, 2000).unwrap();
    let sub_gw = sub.id_of("gw").unwrap();
    let full_gw = b.graph().id_of("gw").unwrap();
    let sub_key = stable_edge_key(&sub, sub.out_edge_ids(sub_gw)[0]);
    let full_keys: Vec<u64> = b
        .graph()
        .out_edge_ids(full_gw)
        .iter()
        .map(|&e| stable_edge_key(b.graph(), e))
        .collect();
    assert!(
        full_keys.contains(&sub_key),
        "subgraph key {sub_key} not found in store keys"
    );
}

#[test]
fn weights_roundtrip_by_stable_key() {
    let mut b = backend_under_test();
    seed(&mut b);
    let g = b.graph();
    let gw = g.id_of("gw").unwrap();
    let k = stable_edge_key(g, g.out_edge_ids(gw)[0]);
    b.flush_weights(&[(
        k,
        EdgeState {
            w: 0.83,
            t_last: 5000,
        },
    )])
    .unwrap();
    b.checkpoint().unwrap();
    let loaded = b.load_weights(&[k, 0xDEAD]).unwrap();
    assert_eq!(
        loaded.get(&k),
        Some(&EdgeState {
            w: 0.83,
            t_last: 5000
        })
    );
    assert!(!loaded.contains_key(&0xDEAD), "absent keys stay absent");
}
