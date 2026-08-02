//! Adaptive-recall benchmark: does learning from use actually improve recall?
//!
//! Reproducible, deterministic, zero-dependency. Full design notes in
//! docs/bench.md; one-line summary of the setup:
//!
//! - T topics, each with M candidate docs linked topic -> doc. For every
//!   topic, a hidden subset (USEFUL of M) is what actually helps; which docs
//!   are useful is INVISIBLE to text — exactly the signal lexical retrieval
//!   cannot see and usage feedback can.
//! - Each episode: retrieve per topic, score recall@3 / MRR against the
//!   hidden useful set, then feed back "these were used" for the useful docs
//!   that surfaced (what a RAG citation or an agent's file-open gives you).
//! - Halfway through, the useful set SHIFTS (requirements changed): the
//!   adaptive system must dip and re-learn; static systems can't move at all.
//!
//! Three systems on the identical corpus:
//!   adaptive  — PsyRag with the feedback loop on (the product)
//!   static    — same graph, same spreading activation, feedback off (ablation)
//!   bm25      — classic lexical ranking (Okapi BM25, k1=1.2 b=0.75)
//!
//! Usage:
//!   cargo run --release -p psyrag --example adaptive_bench            # full run + CSV + SVG
//!   cargo run --release -p psyrag --example adaptive_bench -- --check # small run, asserts the claim
//!
//! `--check` exits non-zero unless adaptive's final recall@3 beats static's
//! by a clear margin — CI runs this so the README chart can never go stale
//! against the code.

use psyrag_core::{Config, Credit, PlasticityLayer};
use psyrag_graph::TemporalGraph;

// ---------------------------------------------------------------------------
// Deterministic RNG (xorshift64*) — no rand crate, same run every time.
// ---------------------------------------------------------------------------
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.max(1))
    }
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
    /// k distinct indices in 0..n.
    fn pick(&mut self, n: usize, k: usize) -> Vec<usize> {
        let mut all: Vec<usize> = (0..n).collect();
        for i in 0..k {
            let j = i + self.below(n - i);
            all.swap(i, j);
        }
        all.truncate(k);
        all
    }
}

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------
struct Corpus {
    topics: usize,
    docs_per_topic: usize,
    /// useful[t] = current useful doc indices for topic t.
    useful: Vec<Vec<usize>>,
    /// after the shift, each topic's useful set changes to this.
    useful_after: Vec<Vec<usize>>,
    /// token bags per (topic, doc) for the BM25 baseline.
    bags: Vec<Vec<Vec<String>>>,
}

fn doc_name(t: usize, m: usize) -> String {
    format!("docs/{t}/{m}")
}
fn topic_name(t: usize) -> String {
    format!("topic-{t}")
}

fn build_corpus(rng: &mut Rng, topics: usize, docs_per_topic: usize, useful_k: usize) -> Corpus {
    let vocab: Vec<String> = (0..30).map(|i| format!("w{i}")).collect();
    let mut useful = Vec::new();
    let mut useful_after = Vec::new();
    let mut bags = Vec::new();
    for t in 0..topics {
        let u1 = rng.pick(docs_per_topic, useful_k);
        // The post-shift useful set is drawn from the docs NOT currently
        // useful, so the shift is a full regime change for this topic.
        let rest: Vec<usize> = (0..docs_per_topic).filter(|i| !u1.contains(i)).collect();
        let picks = rng.pick(rest.len(), useful_k);
        let u2: Vec<usize> = picks.into_iter().map(|i| rest[i]).collect();
        useful.push(u1);
        useful_after.push(u2);
        let mut topic_bags = Vec::new();
        for m in 0..docs_per_topic {
            // Every candidate contains the topic token exactly once; noise
            // tokens differ. Usefulness is uncorrelated with any token, BY
            // CONSTRUCTION: the signal exists only in usage.
            let mut bag = vec![topic_name(t), format!("doc{m}")];
            for _ in 0..(3 + rng.below(4)) {
                bag.push(vocab[rng.below(vocab.len())].clone());
            }
            topic_bags.push(bag);
        }
        bags.push(topic_bags);
    }
    Corpus {
        topics,
        docs_per_topic,
        useful,
        useful_after,
        bags,
    }
}

fn build_graph(c: &Corpus, t0: i64) -> TemporalGraph {
    let mut g = TemporalGraph::new();
    for t in 0..c.topics {
        g.observe_node(&topic_name(t), "topic", serde_json::json!({}), t0);
        for m in 0..c.docs_per_topic {
            g.observe_node(&doc_name(t, m), "doc", serde_json::json!({}), t0);
            let src = g.id_of(&topic_name(t)).unwrap();
            let dst = g.id_of(&doc_name(t, m)).unwrap();
            g.observe_edge(src, dst, "REFERENCES", t0);
        }
    }
    g
}

// ---------------------------------------------------------------------------
// BM25 baseline (Okapi, k1=1.2, b=0.75) over the per-topic candidate docs.
// ---------------------------------------------------------------------------
fn bm25_rank(c: &Corpus, t: usize) -> Vec<usize> {
    let query = topic_name(t);
    let docs = &c.bags[t];
    let n = docs.len() as f64;
    let avg_len: f64 = docs.iter().map(|d| d.len() as f64).sum::<f64>() / n;
    let containing = docs.iter().filter(|d| d.contains(&query)).count() as f64;
    let idf = (((n - containing + 0.5) / (containing + 0.5)) + 1.0).ln();
    let (k1, b) = (1.2f64, 0.75f64);
    let mut scored: Vec<(usize, f64)> = docs
        .iter()
        .enumerate()
        .map(|(m, d)| {
            let tf = d.iter().filter(|w| **w == query).count() as f64;
            let dl = d.len() as f64;
            let s = idf * (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * dl / avg_len));
            (m, s)
        })
        .collect();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    scored.into_iter().map(|(m, _)| m).collect()
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------
struct Score {
    recall_at3: f64,
    mrr: f64,
}

fn score(ranked: &[usize], useful: &[usize]) -> Score {
    let hits3 = ranked.iter().take(3).filter(|m| useful.contains(m)).count();
    let recall_at3 = hits3 as f64 / useful.len() as f64;
    let mrr = ranked
        .iter()
        .position(|m| useful.contains(m))
        .map(|p| 1.0 / (p as f64 + 1.0))
        .unwrap_or(0.0);
    Score { recall_at3, mrr }
}

/// Rank a topic's docs with a plasticity layer (spreading activation from the
/// topic hub). Returns doc indices best-first, plus the trace for feedback.
fn psyrag_rank(
    layer: &mut PlasticityLayer,
    g: &TemporalGraph,
    t: usize,
    k: usize,
    ts: i64,
) -> (Vec<usize>, psyrag_core::Trace, f32) {
    let seed = topic_name(t);
    let (res, trace) = layer.retrieve_traced(g, &[seed.as_str()], 1, 0.9, k + 1, ts);
    let prefix = format!("docs/{t}/");
    let ranked: Vec<usize> = res
        .top
        .iter()
        .filter_map(|n| n.node.strip_prefix(&prefix).and_then(|m| m.parse().ok()))
        .collect();
    (ranked, trace, res.mass)
}

// ---------------------------------------------------------------------------
// SVG chart (hand-rolled; committed at bench/results.svg for the README)
// ---------------------------------------------------------------------------
fn svg_chart(series: &[(&str, &str, &[f64])], shift_at: usize, episodes: usize) -> String {
    let (w, h) = (760.0, 340.0);
    let (ml, mr, mt, mb) = (52.0, 16.0, 42.0, 44.0);
    let (pw, ph) = (w - ml - mr, h - mt - mb);
    let x = |e: f64| ml + pw * e / (episodes.saturating_sub(1)) as f64;
    let y = |v: f64| mt + ph * (1.0 - v);
    let mut s = format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" font-family="ui-monospace,Menlo,monospace" font-size="12">
<rect width="{w}" height="{h}" fill="white"/>
<text x="{ml}" y="20" font-size="14" font-weight="bold" fill="#111">recall@3 of the truly-useful docs, per episode</text>
<text x="{ml}" y="34" fill="#666">feedback: "these results were actually used" · useful set shifts at episode {shift_at}</text>
"##
    );
    for gy in [0.0f64, 0.25, 0.5, 0.75, 1.0] {
        s += &format!(
            r##"<line x1="{ml}" y1="{0}" x2="{1}" y2="{0}" stroke="#e5e5e5"/><text x="{2}" y="{3}" fill="#888" text-anchor="end">{gy}</text>
"##,
            y(gy),
            w - mr,
            ml - 6.0,
            y(gy) + 4.0
        );
    }
    // x labels
    for e in (0..episodes).step_by(5.max(episodes / 8)) {
        s += &format!(
            r##"<text x="{0}" y="{1}" fill="#888" text-anchor="middle">{e}</text>
"##,
            x(e as f64),
            h - mb + 16.0
        );
    }
    s += &format!(
        r##"<text x="{0}" y="{1}" fill="#666" text-anchor="middle">episode</text>
"##,
        ml + pw / 2.0,
        h - 8.0
    );
    // shift marker
    s += &format!(
        r##"<line x1="{0}" y1="{mt}" x2="{0}" y2="{1}" stroke="#c33" stroke-dasharray="4,4"/><text x="{2}" y="{3}" fill="#c33">useful set shifts</text>
"##,
        x(shift_at as f64),
        mt + ph,
        x(shift_at as f64) + 6.0,
        mt + 14.0
    );
    let mut placed_label_ys: Vec<f64> = Vec::new();
    for (name, color, vals) in series {
        let pts: Vec<String> = vals
            .iter()
            .enumerate()
            .map(|(e, v)| format!("{:.1},{:.1}", x(e as f64), y(*v)))
            .collect();
        s += &format!(
            r##"<polyline points="{}" fill="none" stroke="{color}" stroke-width="2.5"/>
"##,
            pts.join(" ")
        );
        let last = vals.last().copied().unwrap_or(0.0);
        // Nudge labels apart when series end close together.
        let mut ly = y(last) - 8.0;
        while placed_label_ys.iter().any(|p| (p - ly).abs() < 14.0) {
            ly += 14.0;
        }
        placed_label_ys.push(ly);
        s += &format!(
            r##"<text x="{0}" y="{1}" fill="{color}" font-weight="bold">{name}</text>
"##,
            x((episodes - 1) as f64) - 62.0,
            ly
        );
    }
    s += "</svg>\n";
    s
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let check = args.iter().any(|a| a == "--check");
    let arg_usize = |name: &str, default: usize| -> usize {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let topics = arg_usize("--topics", 20);
    let episodes = arg_usize("--episodes", 60);
    let docs_per_topic = 8;
    let useful_k = 3;
    let top_k = 8;
    let shift_at = episodes / 2;

    let mut rng = Rng::new(42);
    let corpus = build_corpus(&mut rng, topics, docs_per_topic, useful_k);
    let t0: i64 = 1_700_000_000_000; // fixed epoch — decay math sees identical dt every run
    let episode_gap_ms: i64 = 300_000; // 5 minutes between episodes

    // Session-memory decay profile: gentle decay so learning, not forgetting,
    // dominates a 40-episode window; renormalization keeps competition honest.
    let cfg = Config {
        alpha: 0.10,
        lambda_base: 1e-6,
        // Prune floor: low enough that consolidation squeezes but does not
        // EXECUTE currently-unused edges — a regime shift must stay
        // re-learnable. (Aggressive theta tombstones the post-shift useful
        // edges during phase 1 and recovery caps out.)
        theta: 0.005,
        ..Config::default()
    };

    let g = build_graph(&corpus, t0);
    let mut adaptive = PlasticityLayer::new(cfg.clone());
    adaptive.sync(&g);
    let mut static_l = PlasticityLayer::new(cfg);
    static_l.sync(&g);

    let mut rows: Vec<(usize, f64, f64, f64, f64, f64, f64)> = Vec::new();
    let (mut a3, mut s3, mut b3) = (vec![], vec![], vec![]);

    for e in 0..episodes {
        let ts = t0 + ((e + 1) as i64) * episode_gap_ms;
        let phase_useful = |t: usize| -> &Vec<usize> {
            if e < shift_at {
                &corpus.useful[t]
            } else {
                &corpus.useful_after[t]
            }
        };
        let (mut ar, mut am, mut sr, mut sm, mut br, mut bm) = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
        for t in 0..topics {
            let useful = phase_useful(t);
            // adaptive: retrieve, score, then feed back which docs were used
            let (ranked, trace, mass) = psyrag_rank(&mut adaptive, &g, t, top_k, ts);
            let sc = score(&ranked, useful);
            ar += sc.recall_at3;
            am += sc.mrr;
            adaptive.observe(mass);
            // Realistic, attention-limited usage signal — NOT an oracle:
            // 20% of sessions give no feedback at all (the user was busy);
            // top-3 results are always examined, ranks 4+ only 25% of the
            // time; an examined doc is "used" only if it truly helped.
            // Positive-only credit, i.e. citations / file-opens. (We also
            // tried contrastive negative credit for examined-but-useless
            // docs: combined with per-source renormalization it can crush
            // post-shift candidates below the activation floor and make the
            // new regime unrecoverable — a real dynamics finding this bench
            // surfaced; tracked as a GitHub issue.)
            let session_gives_feedback = rng.below(100) < 80;
            let used: Vec<(String, f32)> = ranked
                .iter()
                .enumerate()
                .filter(|(rank, m)| {
                    session_gives_feedback
                        && useful.contains(m)
                        && (*rank < 3 || rng.below(100) < 25)
                })
                .map(|(_, m)| (doc_name(t, *m), 1.0))
                .collect();
            if !used.is_empty() {
                adaptive.apply_credit(&g, &trace, &Credit::Nodes(used), ts);
            }
            // static ablation: same engine, feedback never applied
            let (ranked_s, _tr, _m) = psyrag_rank(&mut static_l, &g, t, top_k, ts);
            let sc = score(&ranked_s, useful);
            sr += sc.recall_at3;
            sm += sc.mrr;
            // lexical baseline
            let sc = score(&bm25_rank(&corpus, t), useful);
            br += sc.recall_at3;
            bm += sc.mrr;
        }
        let n = topics as f64;
        // nightly consolidation (prune + renormalize) every 8 episodes
        if e % 8 == 7 {
            adaptive.consolidate(&g, ts);
            static_l.consolidate(&g, ts);
        }
        rows.push((e, ar / n, am / n, sr / n, sm / n, br / n, bm / n));
        a3.push(ar / n);
        s3.push(sr / n);
        b3.push(br / n);
    }

    // ---- outputs ----
    let mut csv = String::from(
        "episode,adaptive_recall@3,adaptive_mrr,static_recall@3,static_mrr,bm25_recall@3,bm25_mrr\n",
    );
    for (e, a, am, s, sm, b, bm) in &rows {
        csv += &format!("{e},{a:.4},{am:.4},{s:.4},{sm:.4},{b:.4},{bm:.4}\n");
    }
    let final_window = |v: &[f64]| -> f64 {
        let k = 5.min(v.len());
        v.iter().rev().take(k).sum::<f64>() / k as f64
    };
    let (fa, fs, fb) = (final_window(&a3), final_window(&s3), final_window(&b3));
    let pre_shift = final_window(&a3[..shift_at.min(a3.len())]);
    println!("adaptive-recall benchmark — {topics} topics x {docs_per_topic} docs, {episodes} episodes, shift at {shift_at}");
    println!("  final recall@3 (last-5 mean):  adaptive {fa:.3}   static {fs:.3}   bm25 {fb:.3}");
    println!("  adaptive pre-shift plateau: {pre_shift:.3} — re-learned after the shift: {fa:.3}");

    if check {
        assert!(
            fa > fs + 0.25 && fa > fb + 0.25,
            "adaptive recall ({fa:.3}) must clearly beat static ({fs:.3}) and bm25 ({fb:.3})"
        );
        assert!(
            fa > 0.8 * pre_shift,
            "post-shift recovery ({fa:.3}) fell below 80% of the pre-shift plateau ({pre_shift:.3})"
        );
        println!("  --check: PASS");
        return;
    }

    std::fs::create_dir_all("bench").expect("create bench/");
    std::fs::write("bench/results.csv", &csv).expect("write csv");
    let chart = svg_chart(
        &[
            ("adaptive", "#1a9c5c", &a3),
            ("static", "#8a8f98", &s3),
            ("bm25", "#3b7dd8", &b3),
        ],
        shift_at,
        episodes,
    );
    std::fs::write("bench/results.svg", chart).expect("write svg");
    println!("  wrote bench/results.csv and bench/results.svg");
}
