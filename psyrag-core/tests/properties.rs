//! Property tests: invariants that must hold under randomized (seeded,
//! reproducible) workloads, not just the golden scenarios.

use psyrag_core::{Config, PlasticityLayer};
use psyrag_graph::entity::ingest_entities_mem;
use psyrag_graph::TemporalGraph;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn f32_01(&mut self) -> f32 {
        (self.next() % 10_000) as f32 / 10_000.0
    }
}

/// Random-ish graph: `n` sources each fanning to a handful of targets.
fn random_graph(rng: &mut Rng, n: usize) -> TemporalGraph {
    let mut ents = Vec::new();
    for i in 0..n {
        let fan = 1 + rng.below(5);
        let edges: Vec<String> = (0..fan)
            .map(|_| {
                format!(
                    r#"{{"dst":"n{}","kind":"K{}"}}"#,
                    rng.below(n * 2),
                    rng.below(3)
                )
            })
            .collect();
        ents.push(format!(
            r#"{{"name":"n{i}","type":"t","edges":[{}]}}"#,
            edges.join(",")
        ));
    }
    let json = format!("[{}]", ents.join(","));
    let mut g = TemporalGraph::new();
    ingest_entities_mem(&mut g, &json, 0, false).unwrap();
    g
}

#[test]
fn renorm_holds_l1_budget_per_source() {
    let mut rng = Rng(42);
    let g = random_graph(&mut rng, 40);
    let mut cfg = Config::default();
    cfg.lambda_base = 0.01;
    let norm_target = cfg.norm_target;
    let mut p = PlasticityLayer::new(cfg);
    p.sync(&g);
    // random reinforcement storm
    for step in 1..200i64 {
        let touches: Vec<(u32, f32)> = (0..5)
            .map(|_| (rng.below(g.edge_count()) as u32, rng.f32_01() * 2.0 - 0.5))
            .collect();
        p.touch(&touches, step * 1000);
    }
    let (_stats, _conflicts) = p.consolidate(&g, 200_000);
    // Invariant: every source with live out-mass is renormalized to the L1
    // budget (within float tolerance).
    let mut checked = 0;
    for u in 0..g.node_count() {
        let sum: f32 = g
            .out_edge_ids(u as u32)
            .iter()
            .map(|&eid| p.effective_weight(eid, 200_000))
            .sum();
        if sum > 0.0 {
            assert!(
                (sum - norm_target).abs() < 1e-3,
                "source {} L1 {} != target {}",
                g.node_name(u as u32),
                sum,
                norm_target
            );
            checked += 1;
        }
    }
    assert!(checked > 10, "renorm exercised on {checked} sources");
}

#[test]
fn homeostat_scale_stays_bounded_under_adversarial_mass() {
    let cfg = Config::default();
    let (lo, hi) = (cfg.scale_min, cfg.scale_max);
    let mut rng = Rng(7);
    let g = random_graph(&mut rng, 10);
    let mut p = PlasticityLayer::new(cfg);
    p.sync(&g);
    // feed adversarial mass sequences straight into the controller
    for i in 0..5000 {
        let mass = match i % 7 {
            0 => 0.0,
            1 => 1e9,
            2 => 1e-9,
            _ => rng.f32_01() * 10.0,
        };
        let s = p.observe(mass);
        assert!(
            s.is_finite() && s >= lo && s <= hi,
            "scale {s} escaped [{lo},{hi}] at step {i}"
        );
    }
}

#[test]
fn activations_stay_finite_under_random_load() {
    let mut rng = Rng(1337);
    let g = random_graph(&mut rng, 60);
    let mut cfg = Config::default();
    cfg.depth = 4;
    let mut p = PlasticityLayer::new(cfg);
    p.sync(&g);
    for step in 1..100i64 {
        let t = step * 1000;
        let seed = format!("n{}", rng.below(60));
        let r = p.retrieve_and_adapt(&g, &[seed.as_str()], 10, t);
        assert!(r.mass.is_finite() && r.mass >= 0.0, "mass {}", r.mass);
        for na in &r.top {
            assert!(na.activation.is_finite() && na.activation >= 0.0);
        }
        // interleave feedback with mixed-sign credit
        if step % 3 == 0 {
            let used = format!("n{}", rng.below(60));
            p.feedback(&g, &[seed.as_str()], 2, 0.9, 10, t, &[used.as_str()]);
        }
        if step % 10 == 0 {
            p.consolidate(&g, t);
        }
        if step % 25 == 0 {
            p.sleep(&g, t);
        }
    }
}

#[test]
fn authority_config_changes_apply_retroactively_on_load() {
    // Save a sidecar under default authority, reload under a config that
    // makes CALLS decay-resistant: existing edges must pick up the new rate.
    let json = r#"[{"name":"x","type":"t","edges":[
        {"dst":"y","kind":"CALLS"},{"dst":"z","kind":"NOTE"}]}]"#;
    let mut g = TemporalGraph::new();
    ingest_entities_mem(&mut g, json, 0, false).unwrap();
    let path = std::env::temp_dir().join("psyrag_auth_retro.json");
    let path = path.to_str().unwrap();
    {
        let mut old = PlasticityLayer::new(Config::default()); // authority 0
        old.sync(&g);
        old.save(&g, path).unwrap();
    }
    let mut cfg = Config::default();
    cfg.lambda_base = 0.2;
    cfg.authority_by_kind.insert("CALLS".into(), 9.0); // 10x slower decay
    let mut p = PlasticityLayer::new(cfg);
    p.load_if_exists(&g, path).unwrap();
    p.sync(&g);
    let calls = p.edge_id(&g, "x", "y", "CALLS").unwrap();
    let note = p.edge_id(&g, "x", "z", "NOTE").unwrap();
    let t = 30_000; // 30s
    let w_calls = p.effective_weight(calls, t);
    let w_note = p.effective_weight(note, t);
    assert!(
        w_calls > w_note * 5.0,
        "new authority config governs OLD edges: calls={w_calls} note={w_note}"
    );
    let _ = std::fs::remove_file(path);
}
