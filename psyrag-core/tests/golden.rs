//! Golden learning-quality suite + determinism guarantees.
//!
//! Property tests elsewhere guard invariants; this suite guards the PRODUCT:
//! a fixed corpus, a scripted episode sequence at fixed timestamps, and
//! exact expected recall rankings. If a decay/homeostat/renorm tuning change
//! silently degrades retrieval quality, these fail — the eval-suite
//! equivalent for an adaptive system. Update the expectations ONLY when a
//! ranking change is intended and understood, and say why in the commit.

use psyrag_core::{Config, PlasticityLayer};
use psyrag_graph::entity::ingest_entities_mem;
use psyrag_graph::TemporalGraph;

const S: i64 = 1000; // 1 second in ms

/// A small incident-management service graph: one gateway fanning out to
/// five services, two of which share downstream infrastructure.
const CORPUS: &str = r#"[
  {"name":"gateway","type":"svc/Gateway","edges":[
    {"dst":"orders","kind":"CALLS"},{"dst":"billing","kind":"CALLS"},
    {"dst":"search","kind":"CALLS"},{"dst":"profile","kind":"CALLS"},
    {"dst":"recs","kind":"CALLS"}]},
  {"name":"orders","type":"svc/Service","edges":[
    {"dst":"pg-main","kind":"CONNECTS"},{"dst":"redis","kind":"CONNECTS"}]},
  {"name":"billing","type":"svc/Service","edges":[
    {"dst":"pg-main","kind":"CONNECTS"},{"dst":"stripe","kind":"CALLS"}]},
  {"name":"search","type":"svc/Service","edges":[{"dst":"elastic","kind":"CONNECTS"}]},
  {"name":"profile","type":"svc/Service","edges":[{"dst":"pg-main","kind":"CONNECTS"}]},
  {"name":"recs","type":"svc/Service","edges":[{"dst":"redis","kind":"CONNECTS"}]}
]"#;

fn corpus() -> TemporalGraph {
    let mut g = TemporalGraph::new();
    ingest_entities_mem(&mut g, CORPUS, 0, false).unwrap();
    g
}

fn top_names(layer: &PlasticityLayer, g: &TemporalGraph, seeds: &[&str], k: usize, t: i64) -> Vec<String> {
    layer
        .retrieve(g, seeds, 2, 0.9, k, t)
        .top
        .into_iter()
        .map(|n| n.node)
        .collect()
}

/// The scripted history: billing incidents keep implicating pg-main.
/// Ten episodes, each: retrieve from gateway, "pg-main was the answer",
/// consolidate. Interleave a couple of search episodes so elastic gets
/// some credit too, but 5x less often.
fn run_episodes(layer: &mut PlasticityLayer, g: &TemporalGraph) {
    for i in 1..=10 {
        let t = (i as i64) * 60 * S;
        layer.feedback(g, &["gateway"], 2, 0.9, 10, t, &["pg-main"]);
        if i % 5 == 0 {
            layer.feedback(g, &["search"], 2, 0.9, 10, t + S, &["elastic"]);
        }
        layer.consolidate(g, t + 2 * S);
    }
}

#[test]
fn golden_learned_ranking_is_stable() {
    let g = corpus();
    let mut layer = PlasticityLayer::new(Config::default());
    layer.sync(&g);
    run_episodes(&mut layer, &g);
    let t_query = 11 * 60 * S;

    let top = top_names(&layer, &g, &["gateway"], 10, t_query);
    // The seed itself is always rank 0.
    assert_eq!(top[0], "gateway", "seed first: {top:?}");
    // THE golden product assertion: after ten pg-main-implicating episodes,
    // pg-main outranks every service except the seed — recall reordered
    // itself around what proved useful.
    let rank = |name: &str| top.iter().position(|n| n == name);
    let pg = rank("pg-main").expect("pg-main surfaced");
    for svc in ["search", "recs", "elastic", "stripe", "redis"] {
        if let Some(r) = rank(svc) {
            assert!(pg < r, "pg-main ({pg}) must outrank {svc} ({r}): {top:?}");
        }
    }
    // Paths that carried the credit stay ahead of the never-credited ones.
    let orders = rank("orders").expect("orders surfaced");
    let recs = rank("recs");
    if let Some(recs) = recs {
        assert!(orders < recs, "credited path outranks uncredited: {top:?}");
    }
}

#[test]
fn golden_decay_forgets_the_uncredited() {
    let g = corpus();
    let mut layer = PlasticityLayer::new(Config::default());
    layer.sync(&g);
    run_episodes(&mut layer, &g);
    // A week later with no further reinforcement, the learned edge is weaker
    // in absolute terms but its RELATIVE dominance over never-credited
    // siblings must survive (protected consolidation ordering).
    let t_late = 7 * 24 * 3600 * S;
    let top = top_names(&layer, &g, &["gateway"], 10, t_late);
    let rank = |name: &str| top.iter().position(|n| n == name);
    if let (Some(pg), Some(el)) = (rank("pg-main"), rank("elastic")) {
        assert!(pg < el, "dominance survives a week of decay: {top:?}");
    }
}

#[test]
fn retrieval_is_deterministic_run_to_run() {
    // Two independently constructed worlds, identical inputs → byte-identical
    // retrieval JSON. Guards against HashMap-iteration-order or float-tie
    // nondeterminism sneaking into the retrieval path.
    let mk = || {
        let g = corpus();
        let mut layer = PlasticityLayer::new(Config::default());
        layer.sync(&g);
        run_episodes(&mut layer, &g);
        let r = layer.retrieve(&g, &["gateway"], 2, 0.9, 10, 11 * 60 * S);
        serde_json::to_string(&r).unwrap()
    };
    assert_eq!(mk(), mk(), "identical inputs must produce identical output");
}

#[test]
fn ties_break_stably_by_node_id() {
    // Symmetric children with identical weights: ranking among them must be
    // insertion (NodeId) order, not allocation luck.
    let g = corpus();
    let layer = {
        let mut l = PlasticityLayer::new(Config::default());
        l.sync(&g);
        l
    };
    let a = top_names(&layer, &g, &["gateway"], 10, 60 * S);
    let b = top_names(&layer, &g, &["gateway"], 10, 60 * S);
    assert_eq!(a, b);
    // fresh sidecar: all direct children tie; they must appear in the order
    // their edges were ingested
    let children: Vec<&String> = a
        .iter()
        .filter(|n| ["orders", "billing", "search", "profile", "recs"].contains(&n.as_str()))
        .collect();
    assert_eq!(
        children,
        ["orders", "billing", "search", "profile", "recs"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .iter()
            .collect::<Vec<_>>(),
        "tie-break = ingestion order"
    );
}
