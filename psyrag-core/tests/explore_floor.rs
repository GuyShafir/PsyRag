//! Issue #29: negative credit could drive a live edge to exactly 0, where
//! consolidation prunes it as dead and it can never be re-learned. With
//! `explore_floor` on, depression is bounded at the floor (stored) and every
//! live edge keeps a minimum retrieval salience, so a regime shift stays
//! recoverable. Dead edges are never resurrected by the floor.

use psyrag_core::{Config, Credit, PlasticityLayer};
use psyrag_graph::TemporalGraph;

fn hub_graph() -> TemporalGraph {
    let mut g = TemporalGraph::new();
    let t0 = 1_000;
    g.observe_node("hub", "t", serde_json::json!({}), t0);
    for n in ["a", "b"] {
        g.observe_node(n, "t", serde_json::json!({}), t0);
        let (s, d) = (g.id_of("hub").unwrap(), g.id_of(n).unwrap());
        g.observe_edge(s, d, "R", t0);
    }
    g
}

fn cfg(explore_floor: f32) -> Config {
    Config {
        alpha: 0.5,
        lambda_base: 1e-9,
        theta: 0.01,
        explore_floor,
        ..Config::default()
    }
}

/// Depress `a` hard, consolidate, and report whether `a` still surfaces.
fn depress_then_surfaces(explore_floor: f32) -> (bool, usize) {
    let g = hub_graph();
    let mut l = PlasticityLayer::new(cfg(explore_floor));
    l.sync(&g);
    let mut ts = 2_000;
    for _ in 0..10 {
        let (_, tr) = l.retrieve_traced(&g, &["hub"], 1, 0.9, 8, ts);
        l.apply_credit(&g, &tr, &Credit::Nodes(vec![("a".into(), -1.0)]), ts);
        ts += 1_000;
    }
    let (st, _) = l.consolidate(&g, ts);
    let (res, _) = l.retrieve_traced(&g, &["hub"], 1, 0.9, 8, ts + 1_000);
    (res.top.iter().any(|n| n.node == "a"), st.live_edges)
}

#[test]
fn without_floor_depression_is_an_absorbing_state() {
    let (surfaces, live) = depress_then_surfaces(0.0);
    assert!(
        !surfaces,
        "with floor off, a depressed edge vanishes from retrieval"
    );
    assert_eq!(live, 1, "and consolidation prunes it as dead");
}

#[test]
fn with_floor_a_depressed_edge_stays_live_and_discoverable() {
    let (surfaces, live) = depress_then_surfaces(0.05);
    assert!(surfaces, "a live edge never disappears from retrieval");
    assert_eq!(live, 2, "bounded depression never reaches the prune floor");
}

#[test]
fn with_floor_a_depressed_edge_can_be_relearned() {
    let g = hub_graph();
    let mut l = PlasticityLayer::new(cfg(0.05));
    l.sync(&g);
    let mut ts = 2_000;
    for _ in 0..10 {
        let (_, tr) = l.retrieve_traced(&g, &["hub"], 1, 0.9, 8, ts);
        l.apply_credit(&g, &tr, &Credit::Nodes(vec![("a".into(), -1.0)]), ts);
        ts += 1_000;
    }
    l.consolidate(&g, ts);
    // regime shift: a is useful now
    for _ in 0..6 {
        let (_, tr) = l.retrieve_traced(&g, &["hub"], 1, 0.9, 8, ts);
        l.apply_credit(&g, &tr, &Credit::Nodes(vec![("a".into(), 1.0)]), ts);
        ts += 1_000;
    }
    let (res, _) = l.retrieve_traced(&g, &["hub"], 1, 0.9, 8, ts);
    let rank = |n: &str| res.top.iter().position(|x| x.node == n).unwrap();
    assert!(rank("a") < rank("b"), "a re-learned past b: {:?}", res.top);
}

#[test]
fn floor_never_resurrects_dead_edges() {
    let g = hub_graph();
    // fast decay so pure disuse prunes `a` and `b` at consolidation
    let mut l = PlasticityLayer::new(Config {
        lambda_base: 1.0,
        theta: 0.01,
        explore_floor: 0.5,
        ..Config::default()
    });
    l.sync(&g);
    let (st, _) = l.consolidate(&g, 1_000 + 3_600_000);
    assert_eq!(st.live_edges, 0, "disuse still prunes");
    let (res, _) = l.retrieve_traced(&g, &["hub"], 1, 0.9, 8, 1_000 + 3_700_000);
    assert!(
        res.top.iter().all(|n| n.node == "hub"),
        "dead edges get no floor: {:?}",
        res.top
    );
}
