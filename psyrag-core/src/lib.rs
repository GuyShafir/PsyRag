//! # psyrag-graph-plasticity
//!
//! An adaptive-retrieval **salience** layer over a `psyrag_graph::TemporalGraph`.
//! psyrag-graph records *what is true and when* (append-only, temporal). This
//! layer records *what is worth surfacing*, via Hebbian plasticity:
//!
//!   w_ij(t) = w_ij(t_prev) * e^{-lambda*dt} + alpha*R
//!
//! Design (see docs/architecture.md), and the boundaries the psyrag-graph code makes explicit:
//!
//! * **Sidecar, keyed by `EdgeId`.** psyrag-graph is append-only, so `EdgeId` is a
//!   stable dense index; plasticity state lives in parallel columns indexed by it.
//!   The graph's `Edge` struct is never modified. `sync()` grows the sidecar to
//!   match `graph.edge_count()` after ingestion.
//! * **Lazy decay.** State is `(w_last, t_last)`. True weight at any t is one
//!   `exp` from the stored value. Reads never mutate; only `touch` writes.
//! * **Authority -> decay resistance.** `lambda = lambda_base / (1 + beta*auth)`,
//!   authority resolved per edge-kind / source-type from config.
//! * **Homeostasis.** Integral controller on ONE scalar (`lambda_scale`), target
//!   mean activated mass. Above setpoint -> forget faster.
//! * **Salience vs. truth.** Decay + prune are a *retrieval mask* (sidecar `dead`
//!   bit): a stale edge is still a true fact and stays in the graph. Genuine
//!   contradiction (same src+kind, different dst) is a *truth* change and is
//!   emitted as psyrag-graph supersession ops for the caller to journal — never
//!   silently applied here.
//! * **Time base is psyrag-graph's `Ts` (i64 millis).** dt seconds = dt_ms/1000.
//!
//! What psyrag-graph already provides and this layer therefore does NOT reinvent:
//! validity intervals / supersession, typed edge kinds, adjacency, temporal
//! `alive_at` filtering, the WAL. Weighted spreading activation reads the graph's
//! adjacency (`out_edge_ids`) and the sidecar's weights.

use psyrag_graph::graph::{EdgeId, NodeId, Ts, T_MAX};
use psyrag_graph::{Op, TemporalGraph};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

const MS_PER_SEC: f32 = 1000.0;

// ===========================================================================
// Config — every knob, serde-loadable (TOML/JSON), with sane defaults.
// ===========================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Reinforcement gain (alpha): weight added per unit of clipped R on touch.
    pub alpha: f32,
    /// Base decay rate lambda (per second) before authority resistance.
    pub lambda_base: f32,
    /// Authority sensitivity (beta) in lambda_base/(1+beta*authority).
    pub beta: f32,
    /// Initial weight for a newly-observed edge.
    pub w0: f32,
    /// Per-touch clip on R so one interaction can't spike a hub.
    pub r_clip: f32,

    /// Homeostat setpoint: target mean activated mass per retrieval.
    pub setpoint: f32,
    /// Homeostat integral gain k_i.
    pub k_i: f32,
    /// EWMA smoothing on observed mass (0..1, higher = smoother).
    pub ewma_beta: f32,
    /// Bounds on the global lambda_scale knob.
    pub scale_min: f32,
    pub scale_max: f32,
    /// Anti-windup bounds on the integral term.
    pub integral_min: f32,
    pub integral_max: f32,

    /// Retrieval: default spreading depth and fan factor.
    pub depth: u32,
    pub fan: f32,
    /// Minimum per-hop activation delta to keep propagating.
    pub activation_epsilon: f32,

    /// Consolidation: prune (tombstone) edges whose live weight < theta.
    pub theta: f32,
    /// L1 per-source renormalization target (competitive budget).
    pub norm_target: f32,

    /// Per-edge-kind authority overrides (kind string -> authority).
    /// Authority raises decay resistance. Unlisted kinds get `authority_default`.
    pub authority_by_kind: HashMap<String, f32>,
    pub authority_default: f32,

    /// Edge kinds that are *functional* (single-valued): a source may have at
    /// most one open edge of this kind. Only these are checked for ground-truth
    /// contradictions during consolidation. Multi-valued predicates (CONNECTS,
    /// CONTAINS, REFERENCES, ...) are never flagged — a service connecting to
    /// both a db and a cache is not a conflict. Functionality is domain
    /// knowledge you declare, not structure inferred from co-occurrence.
    /// Default empty => no conflict detection.
    pub functional_kinds: std::collections::HashSet<String>,

    /// Feedback / credit-assignment loop:
    /// `feedback_gamma` — fraction of a used node's credit propagated one hop
    ///   upstream, so whole *paths* to useful nodes gain salience (0 = last edge
    ///   only, ->1 = full path). `feedback_hit` — credit magnitude for a node the
    ///   consumer actually used. `feedback_miss_penalty` — negative credit for a
    ///   node that was surfaced (returned in top-k) but NOT used; 0 disables
    ///   anti-Hebbian depression (the safe default — depression can suppress
    ///   useful-but-rare edges).
    pub feedback_gamma: f32,
    pub feedback_hit: f32,
    pub feedback_miss_penalty: f32,

    /// "Sleep" (offline heavy consolidation, per the synaptic-homeostasis
    /// hypothesis): `sleep_downscale` multiplies every live weight (<1 restores
    /// dynamic range while preserving relative structure); `sleep_theta` is the
    /// aggressive prune floor applied after downscaling (> daytime `theta`);
    /// `protect_top_frac` exempts the top fraction of edges by weight from that
    /// prune — they are "consolidated to long-term memory" and protected from
    /// catastrophic forgetting even if unused recently.
    pub sleep_downscale: f32,
    pub sleep_theta: f32,
    pub protect_top_frac: f32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            alpha: 0.05,
            lambda_base: 0.05,
            beta: 1.0,
            w0: 0.5,
            r_clip: 1.0,
            setpoint: 0.5,
            k_i: 0.02,
            ewma_beta: 0.8,
            scale_min: 0.25,
            scale_max: 8.0,
            integral_min: -500.0,
            integral_max: 500.0,
            depth: 2,
            fan: 0.9,
            activation_epsilon: 1e-6,
            theta: 0.01,
            norm_target: 1.0,
            authority_by_kind: HashMap::new(),
            authority_default: 0.0,
            functional_kinds: std::collections::HashSet::new(),
            feedback_gamma: 0.5,
            feedback_hit: 1.0,
            feedback_miss_penalty: 0.0,
            sleep_downscale: 0.6,
            sleep_theta: 0.05,
            protect_top_frac: 0.2,
        }
    }
}

// ===========================================================================
// Homeostat — integral controller on lambda_scale.
// ===========================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Homeostat {
    pub setpoint: f32,
    k_i: f32,
    integral: f32,
    pub ewma_mass: f32,
    ewma_beta: f32,
    scale_min: f32,
    scale_max: f32,
    i_min: f32,
    i_max: f32,
    primed: bool,
}

impl Homeostat {
    fn from_cfg(c: &Config) -> Self {
        Homeostat {
            setpoint: c.setpoint,
            k_i: c.k_i,
            integral: 0.0,
            ewma_mass: 0.0,
            ewma_beta: c.ewma_beta,
            scale_min: c.scale_min,
            scale_max: c.scale_max,
            i_min: c.integral_min,
            i_max: c.integral_max,
            primed: false,
        }
    }
    /// Feed one retrieval's activated mass; return the new lambda_scale.
    pub fn observe(&mut self, mass: f32) -> f32 {
        if !self.primed {
            self.ewma_mass = mass;
            self.primed = true;
        } else {
            self.ewma_mass = self.ewma_beta * self.ewma_mass + (1.0 - self.ewma_beta) * mass;
        }
        let err = self.ewma_mass - self.setpoint;
        self.integral = (self.integral + err).clamp(self.i_min, self.i_max);
        (1.0 + self.k_i * self.integral).clamp(self.scale_min, self.scale_max)
    }
    pub fn integral(&self) -> f32 {
        self.integral
    }
}

// ===========================================================================
// Reports
// ===========================================================================

#[derive(Debug, Clone, Serialize)]
pub struct RetrieveResult {
    pub mass: f32,
    pub lambda_scale: f32,
    pub top: Vec<NodeAct>,
}
#[derive(Debug, Clone, Serialize)]
pub struct NodeAct {
    pub node: String,
    pub node_type: String,
    pub activation: f32,
}

/// One edge that carried activation during a retrieval (hop order).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fired {
    pub eid: EdgeId,
    pub u: NodeId,
    pub v: NodeId,
    pub delta: f32,
}

/// Eligibility record from a traced retrieval. Feed it, plus which nodes the
/// consumer actually used, to `apply_feedback`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    fired: Vec<Fired>,
    incoming: Vec<f32>, // total edge-derived activation into each node
    surfaced: Vec<(NodeId, f32)>, // returned top-k: (node id, final activation)
    t: Ts,
}

impl Trace {
    pub fn surfaced(&self) -> &[(NodeId, f32)] {
        &self.surfaced
    }
    pub fn edges_fired(&self) -> usize {
        self.fired.len()
    }
    /// The `t_now` the trace was taken at.
    pub fn t(&self) -> Ts {
        self.t
    }
    /// Fired edges as (edge_id, src_node, dst_node, delivered_activation), hop
    /// order. For trace visualization / management UIs.
    pub fn fired(&self) -> Vec<(EdgeId, NodeId, NodeId, f32)> {
        self.fired.iter().map(|f| (f.eid, f.u, f.v, f.delta)).collect()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FeedbackReport {
    pub edges_reinforced: usize,
    pub total_positive_r: f32,
    pub total_negative_r: f32,
    pub hits: usize,
    pub misses: usize,
}

/// A usage signal, in whatever shape the consumer can produce. Every mode
/// reduces to a per-node credit vector fed to the same credit-assignment core.
#[derive(Debug, Clone)]
pub enum Credit {
    /// Explicit / graded: named nodes with a credit each (positive = useful,
    /// negative = actively unhelpful). Covers RAG citations, click-through,
    /// thumbs up/down, and contrastive "A beat B" (positive A, negative B).
    Nodes(Vec<(String, f32)>),
    /// Episodic: one scalar reward for the whole retrieval, distributed across
    /// the surfaced nodes. Covers agent task success, incident resolution, A/B
    /// outcome, bandit reward — anything where you only know if the *episode*
    /// worked, not which node carried it.
    Episodic { reward: f32, spread: Spread },
}

/// How an episodic reward is distributed over surfaced nodes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Spread {
    /// Proportional to each node's final activation (the retrieval's own
    /// confidence). Trusts the ranking.
    ByActivation,
    /// Equal share to every surfaced node. Agnostic to the ranking.
    Uniform,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ConsolidationStats {
    pub live_edges: usize,
    pub pruned: usize,
    pub renormalized_sources: usize,
    pub conflicts_found: usize,
}

/// Result of a `sleep` cycle (offline heavy consolidation).
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SleepReport {
    pub downscaled: usize,
    pub pruned: usize,
    pub protected: usize,
    pub renormalized_sources: usize,
    pub live_after: usize,
    pub mean_weight_before: f32,
    pub mean_weight_after: f32,
}

/// A genuine ground-truth contradiction detected during consolidation: the
/// source has multiple *open* edges of the same kind. The layer does NOT
/// resolve this itself (that's a truth change); it surfaces the losers as
/// `RetireEdge` ops for the caller to journal through the WAL if desired.
#[derive(Debug, Clone, Serialize)]
pub struct Conflict {
    pub src: String,
    pub kind: String,
    /// Winner (highest current weight) kept; the rest are proposed for retire.
    pub winner_dst: String,
    pub superseded: Vec<Op>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Stats {
    pub edges_total: usize,
    pub edges_live: usize,
    pub edges_dead: usize,
    pub nodes: usize,
    pub lambda_scale: f32,
    pub setpoint: f32,
    pub ewma_mass: f32,
    pub integral: f32,
    /// Weight distribution summary over live edges.
    pub weight_min: f32,
    pub weight_max: f32,
    pub weight_mean: f32,
}

// ===========================================================================
// PlasticityLayer — the sidecar
// ===========================================================================

pub struct PlasticityLayer {
    pub cfg: Config,
    pub homeo: Homeostat,

    // sidecar columns, indexed by psyrag-graph EdgeId
    w: Vec<f32>,
    t_last: Vec<Ts>,
    neg_lambda: Vec<f32>,
    dead: Vec<bool>,

    lambda_scale: AtomicU32,
}

impl PlasticityLayer {
    pub fn new(cfg: Config) -> Self {
        let homeo = Homeostat::from_cfg(&cfg);
        PlasticityLayer {
            cfg,
            homeo,
            w: Vec::new(),
            t_last: Vec::new(),
            neg_lambda: Vec::new(),
            dead: Vec::new(),
            lambda_scale: AtomicU32::new(1.0f32.to_bits()),
        }
    }

    #[inline]
    pub fn lambda_scale(&self) -> f32 {
        f32::from_bits(self.lambda_scale.load(Ordering::Relaxed))
    }
    #[inline]
    pub fn set_lambda_scale(&self, s: f32) {
        self.lambda_scale.store(s.to_bits(), Ordering::Relaxed);
    }

    /// Grow the sidecar to cover every edge in the graph. New edges are seeded
    /// with `w0`, `t_last = edge.valid_from`, and a per-edge lambda derived from
    /// the edge kind's authority. Call after each ingest batch. O(new edges).
    pub fn sync(&mut self, g: &TemporalGraph) {
        let total = g.edge_count();
        for eid in self.w.len()..total {
            let e = g.edge(eid as EdgeId);
            let kind = g.kind_str(e.kind_id);
            let auth = *self
                .cfg
                .authority_by_kind
                .get(kind)
                .unwrap_or(&self.cfg.authority_default);
            let lam = self.cfg.lambda_base / (1.0 + self.cfg.beta * auth);
            self.w.push(self.cfg.w0);
            self.t_last.push(e.valid_from);
            self.neg_lambda.push(-lam);
            self.dead.push(false);
        }
    }

    #[inline]
    fn eff(&self, eid: EdgeId, t_now: Ts, scale: f32) -> f32 {
        let i = eid as usize;
        if self.dead[i] {
            return 0.0;
        }
        let dt = (t_now - self.t_last[i]).max(0) as f32 / MS_PER_SEC;
        self.w[i] * (self.neg_lambda[i] * scale * dt).exp()
    }

    /// True current retrieval weight of an edge (pure read).
    pub fn effective_weight(&self, eid: EdgeId, t_now: Ts) -> f32 {
        self.eff(eid, t_now, self.lambda_scale())
    }

    /// Reinforce edges: decay-to-now then add alpha*clip(R). Batch write.
    pub fn touch(&mut self, touches: &[(EdgeId, f32)], t_now: Ts) {
        let scale = self.lambda_scale();
        for &(eid, r) in touches {
            let i = eid as usize;
            if i >= self.w.len() || self.dead[i] {
                continue;
            }
            let decayed = self.eff(eid, t_now, scale);
            let r = r.clamp(-self.cfg.r_clip, self.cfg.r_clip);
            self.w[i] = (decayed + self.cfg.alpha * r).max(0.0);
            self.t_last[i] = t_now;
        }
    }

    /// Weighted spreading activation over edges *alive at t_now* (psyrag-graph's
    /// temporal filter) using sidecar weights. Saturating conductance keeps
    /// weight magnitude in the loop (so decay controls mass) while bounding
    /// outflow. Returns mass + top-k activated nodes. Read-only.
    pub fn retrieve(
        &self,
        g: &TemporalGraph,
        seeds: &[&str],
        depth: u32,
        fan: f32,
        top_k: usize,
        t_now: Ts,
    ) -> RetrieveResult {
        self.spread(g, seeds, depth, fan, top_k, t_now).0
    }

    /// Same as `retrieve`, but also returns a `Trace`: the fired edges (in hop
    /// order) and each node's total incoming activation. Hold the trace, let the
    /// consumer act, then feed which nodes were useful to `apply_feedback` — that
    /// is the learning signal. The trace is cheap (bounded by fired edges).
    pub fn retrieve_traced(
        &self,
        g: &TemporalGraph,
        seeds: &[&str],
        depth: u32,
        fan: f32,
        top_k: usize,
        t_now: Ts,
    ) -> (RetrieveResult, Trace) {
        self.spread(g, seeds, depth, fan, top_k, t_now)
    }

    fn spread(
        &self,
        g: &TemporalGraph,
        seeds: &[&str],
        depth: u32,
        fan: f32,
        top_k: usize,
        t_now: Ts,
    ) -> (RetrieveResult, Trace) {
        let scale = self.lambda_scale();
        let n = g.node_count();
        let mut act = vec![0f32; n];
        let mut incoming = vec![0f32; n]; // edge-derived activation into each node
        let mut fired: Vec<Fired> = Vec::new();
        let mut frontier: Vec<NodeId> = Vec::new();
        for s in seeds {
            if let Some(id) = g.id_of(s) {
                if g.node(id).alive_at(t_now) {
                    act[id as usize] = 1.0;
                    frontier.push(id);
                }
            }
        }
        let mut mass = 0f32;
        let eps = self.cfg.activation_epsilon;
        for _ in 0..depth {
            let mut next = Vec::new();
            for &u in &frontier {
                let a = act[u as usize];
                if a <= 0.0 {
                    continue;
                }
                let mut out_mass = 0f32;
                for &eid in g.out_edge_ids(u) {
                    let e = g.edge(eid);
                    if e.alive_at(t_now) {
                        out_mass += self.eff(eid, t_now, scale).max(0.0);
                    }
                }
                if out_mass <= 0.0 {
                    continue;
                }
                let conductance = out_mass / (1.0 + out_mass);
                let outflow = a * fan * conductance;
                for &eid in g.out_edge_ids(u) {
                    let e = g.edge(eid);
                    if !e.alive_at(t_now) {
                        continue;
                    }
                    let we = self.eff(eid, t_now, scale).max(0.0);
                    let v = e.dst as usize;
                    let delta = outflow * (we / out_mass);
                    if delta > eps {
                        if act[v] == 0.0 {
                            next.push(e.dst);
                        }
                        act[v] += delta;
                        incoming[v] += delta;
                        mass += delta;
                        fired.push(Fired { eid, u, v: e.dst, delta });
                    }
                }
            }
            frontier = next;
            if frontier.is_empty() {
                break;
            }
        }
        let mut pairs: Vec<(NodeId, f32)> = act
            .iter()
            .enumerate()
            .filter(|(_, &a)| a > 0.0)
            .map(|(i, &a)| (i as NodeId, a))
            .collect();
        pairs.sort_by(|x, y| y.1.partial_cmp(&x.1).unwrap());
        pairs.truncate(top_k);
        let surfaced: Vec<(NodeId, f32)> = pairs.iter().map(|(id, a)| (*id, *a)).collect();
        let top = pairs
            .into_iter()
            .map(|(id, a)| NodeAct {
                node: g.node_name(id).to_string(),
                node_type: g.node_type(id).to_string(),
                activation: a,
            })
            .collect();
        (
            RetrieveResult {
                mass,
                lambda_scale: scale,
                top,
            },
            Trace {
                fired,
                incoming,
                surfaced,
                t: t_now,
            },
        )
    }

    /// Close the homeostatic loop for one retrieval: observe mass, update scale.
    pub fn observe(&mut self, mass: f32) -> f32 {
        let s = self.homeo.observe(mass);
        self.set_lambda_scale(s);
        s
    }

    /// Convenience: retrieve + observe in one call.
    pub fn retrieve_and_adapt(
        &mut self,
        g: &TemporalGraph,
        seeds: &[&str],
        top_k: usize,
        t_now: Ts,
    ) -> RetrieveResult {
        let (depth, fan) = (self.cfg.depth, self.cfg.fan);
        let mut r = self.retrieve(g, seeds, depth, fan, top_k, t_now);
        r.lambda_scale = self.observe(r.mass);
        r
    }

    /// The learning step. Given a retrieval `trace` and a per-node credit map
    /// (positive = the consumer used it, negative = surfaced-but-useless),
    /// assign credit to the edges that carried the activation and reinforce them.
    ///
    /// Credit flows *backwards along the fired paths* (reverse hop order): a used
    /// node's credit is split among its incoming fired edges in proportion to how
    /// much activation each delivered, and a `gamma` fraction propagates one hop
    /// further upstream — so an entire path to a useful node gains salience, with
    /// geometric decay. This is an eligibility-trace credit assignment; it
    /// reinforces only edges that *both* fired *and* led somewhere useful, so
    /// unused structure is left to decay. Returns what changed.
    pub fn apply_feedback(
        &mut self,
        trace: &Trace,
        credit: &HashMap<NodeId, f32>,
        t_now: Ts,
    ) -> FeedbackReport {
        // mutable credit vector over nodes, seeded from the caller's map
        let mut cred = vec![0f32; trace.incoming.len()];
        for (&node, &c) in credit {
            let i = node as usize;
            if i < cred.len() {
                cred[i] = c;
            }
        }
        self.assign_and_touch(trace, cred, t_now)
    }

    /// The universal credit-assignment core: given a per-node credit vector,
    /// walk the fired edges in reverse hop order, split each used node's credit
    /// among its incoming fired edges by delivered activation, propagate a
    /// `feedback_gamma` fraction one hop upstream, and reinforce. Every feedback
    /// mode (explicit, graded, episodic, deferred) reduces to producing the
    /// credit vector and calling this.
    fn assign_and_touch(&mut self, trace: &Trace, mut cred: Vec<f32>, t_now: Ts) -> FeedbackReport {
        let gamma = self.cfg.feedback_gamma;
        let mut r_by_edge: HashMap<EdgeId, f32> = HashMap::new();
        for f in trace.fired.iter().rev() {
            let vi = f.v as usize;
            let cv = cred[vi];
            if cv == 0.0 {
                continue;
            }
            let inc = trace.incoming[vi];
            if inc <= 0.0 {
                continue;
            }
            let share = f.delta / inc;
            let r = cv * share;
            *r_by_edge.entry(f.eid).or_insert(0.0) += r;
            let ui = f.u as usize;
            if ui < cred.len() {
                cred[ui] += gamma * r;
            }
        }
        let mut pos = 0f32;
        let mut neg = 0f32;
        let touches: Vec<(EdgeId, f32)> = r_by_edge
            .into_iter()
            .map(|(eid, r)| {
                if r >= 0.0 {
                    pos += r;
                } else {
                    neg += r;
                }
                (eid, r)
            })
            .collect();
        let reinforced = touches.len();
        self.touch(&touches, t_now);
        let hits = cred.iter().filter(|&&c| c > 0.0).count();
        let misses = cred.iter().filter(|&&c| c < 0.0).count();
        FeedbackReport {
            edges_reinforced: reinforced,
            total_positive_r: pos,
            total_negative_r: neg,
            hits,
            misses,
        }
    }

    /// Apply a `Credit` (any feedback mode) against a trace. Resolves node names
    /// and builds the credit vector per mode, then runs the universal core.
    pub fn apply_credit(
        &mut self,
        g: &TemporalGraph,
        trace: &Trace,
        credit: &Credit,
        t_now: Ts,
    ) -> FeedbackReport {
        let mut cred = vec![0f32; trace.incoming.len()];
        match credit {
            Credit::Nodes(items) => {
                for (name, score) in items {
                    if let Some(id) = g.id_of(name) {
                        let i = id as usize;
                        if i < cred.len() {
                            cred[i] += score;
                        }
                    }
                }
            }
            Credit::Episodic { reward, spread } => {
                match spread {
                    Spread::Uniform => {
                        let n = trace.surfaced.len().max(1) as f32;
                        for &(id, _) in &trace.surfaced {
                            cred[id as usize] += reward / n;
                        }
                    }
                    Spread::ByActivation => {
                        let total: f32 = trace.surfaced.iter().map(|(_, a)| *a).sum();
                        if total > 0.0 {
                            for &(id, a) in &trace.surfaced {
                                cred[id as usize] += reward * (a / total);
                            }
                        }
                    }
                }
            }
        }
        // optional anti-Hebbian depression of surfaced-but-uncredited nodes
        if self.cfg.feedback_miss_penalty > 0.0 {
            for &(id, _) in &trace.surfaced {
                let i = id as usize;
                if cred[i] == 0.0 {
                    cred[i] = -self.cfg.feedback_miss_penalty;
                }
            }
        }
        self.assign_and_touch(trace, cred, t_now)
    }

    /// Name-based explicit feedback (stateless): retrieve (traced), credit the
    /// `used` nodes with `feedback_hit`, apply. Deterministic given `t_now`.
    pub fn feedback(
        &mut self,
        g: &TemporalGraph,
        seeds: &[&str],
        depth: u32,
        fan: f32,
        top_k: usize,
        t_now: Ts,
        used: &[&str],
    ) -> FeedbackReport {
        let (_res, trace) = self.retrieve_traced(g, seeds, depth, fan, top_k, t_now);
        let hit = self.cfg.feedback_hit;
        let items: Vec<(String, f32)> = used.iter().map(|n| (n.to_string(), hit)).collect();
        self.apply_credit(g, &trace, &Credit::Nodes(items), t_now)
    }

    /// Episodic feedback (stateless): one scalar `reward` for the whole
    /// retrieval, spread over surfaced nodes. This is the RL/outcome path — wire
    /// it to "the task the retrieval fed actually succeeded".
    pub fn feedback_reward(
        &mut self,
        g: &TemporalGraph,
        seeds: &[&str],
        depth: u32,
        fan: f32,
        top_k: usize,
        t_now: Ts,
        reward: f32,
        spread: Spread,
    ) -> FeedbackReport {
        let (_res, trace) = self.retrieve_traced(g, seeds, depth, fan, top_k, t_now);
        self.apply_credit(g, &trace, &Credit::Episodic { reward, spread }, t_now)
    }

    /// Consolidation over the sidecar (salience only) plus ground-truth conflict
    /// *detection* (not resolution). Steps:
    ///   1. materialize decay to now for live edges,
    ///   2. prune (tombstone) live weight < theta  [sidecar mask; the fact stays],
    ///   3. L1-renormalize each source's live out-edges to norm_target,
    ///   4. detect same-(src,kind) open-edge contradictions in the GRAPH and
    ///      return the losers as RetireEdge ops for the caller to journal.
    /// Returns (stats, conflicts). The graph is read-only here.
    pub fn consolidate(
        &mut self,
        g: &TemporalGraph,
        t_now: Ts,
    ) -> (ConsolidationStats, Vec<Conflict>) {
        let scale = self.lambda_scale();
        let theta = self.cfg.theta;
        let norm_target = self.cfg.norm_target;

        // 1 + 2: materialize + prune
        let mut pruned = 0usize;
        for i in 0..self.w.len() {
            if self.dead[i] {
                continue;
            }
            let w_eff = self.eff(i as EdgeId, t_now, scale);
            if w_eff < theta {
                self.dead[i] = true;
                self.w[i] = 0.0;
                pruned += 1;
            } else {
                self.w[i] = w_eff;
                self.t_last[i] = t_now;
            }
        }

        // 3: per-source L1 renorm over live out-edges
        let mut renormalized = 0usize;
        for u in 0..g.node_count() {
            let out = g.out_edge_ids(u as NodeId);
            let mut sum = 0f32;
            for &eid in out {
                let i = eid as usize;
                if i < self.w.len() && !self.dead[i] {
                    sum += self.w[i];
                }
            }
            if sum > 0.0 {
                let k = norm_target / sum;
                for &eid in out {
                    let i = eid as usize;
                    if i < self.w.len() && !self.dead[i] {
                        self.w[i] *= k;
                    }
                }
                renormalized += 1;
            }
        }

        // 4: ground-truth conflict detection (open edges, same src+kind, diff dst)
        let mut conflicts = Vec::new();
        for u in 0..g.node_count() {
            // group open out-edges by kind_id — only for FUNCTIONAL kinds, where
            // >1 open edge is a genuine contradiction. Multi-valued predicates
            // are skipped entirely.
            if self.cfg.functional_kinds.is_empty() {
                continue;
            }
            let mut by_kind: HashMap<u32, Vec<EdgeId>> = HashMap::new();
            for &eid in g.out_edge_ids(u as NodeId) {
                let e = g.edge(eid);
                if e.valid_to == T_MAX && self.cfg.functional_kinds.contains(g.kind_str(e.kind_id)) {
                    by_kind.entry(e.kind_id).or_default().push(eid);
                }
            }
            for (kind_id, eids) in by_kind {
                if eids.len() < 2 {
                    continue;
                }
                // winner = highest current weight
                let mut best = eids[0];
                let mut best_w = self.eff(best, t_now, scale);
                for &eid in &eids[1..] {
                    let we = self.eff(eid, t_now, scale);
                    if we > best_w {
                        best_w = we;
                        best = eid;
                    }
                }
                let kind = g.kind_str(kind_id).to_string();
                let src = g.node_name(u as NodeId).to_string();
                let winner_dst = g.node_name(g.edge(best).dst).to_string();
                let superseded = eids
                    .iter()
                    .filter(|&&eid| eid != best)
                    .map(|&eid| Op::RetireEdge {
                        src: src.clone(),
                        dst: g.node_name(g.edge(eid).dst).to_string(),
                        kind: kind.clone(),
                        ts: t_now,
                    })
                    .collect();
                conflicts.push(Conflict {
                    src,
                    kind,
                    winner_dst,
                    superseded,
                });
            }
        }

        let live = self.dead.iter().filter(|d| !**d).count();
        (
            ConsolidationStats {
                live_edges: live,
                pruned,
                renormalized_sources: renormalized,
                conflicts_found: conflicts.len(),
            },
            conflicts,
        )
    }

    /// **Sleep** — offline heavy consolidation (synaptic homeostasis hypothesis).
    /// Wake-time reinforcement only ever *adds* weight, so without an offline
    /// downscale the graph drifts up, saturates, and loses discriminability. Sleep:
    ///   1. materialize lazy decay to `t_now`,
    ///   2. compute a protection threshold at the `(1 - protect_top_frac)` weight
    ///      quantile (the top fraction is "consolidated to long-term memory"),
    ///   3. multiplicatively **downscale** every live weight by `sleep_downscale`
    ///      (preserves relative structure, restores dynamic range),
    ///   4. **prune** edges that fall below `sleep_theta` *unless* they were above
    ///      the protection threshold (anti-catastrophic-forgetting: durable
    ///      memories survive even when unused today),
    ///   5. per-source L1 **renormalize** (competition).
    /// In a tiered deployment this is also where the working graph's learned
    /// deltas are flushed to long-term store (the GCP backend) — see DESIGN.
    pub fn sleep(&mut self, g: &TemporalGraph, t_now: Ts) -> SleepReport {
        let scale = self.lambda_scale();
        // 1. materialize decay, collect live weights
        let mut live_idx = Vec::new();
        let mut before_sum = 0f32;
        for i in 0..self.w.len() {
            if self.dead[i] {
                continue;
            }
            let w = self.eff(i as EdgeId, t_now, scale);
            self.w[i] = w;
            self.t_last[i] = t_now;
            before_sum += w;
            live_idx.push(i);
        }
        let live_before = live_idx.len().max(1);
        // 2. protection threshold = (1 - frac) quantile of live weights
        let mut sorted: Vec<f32> = live_idx.iter().map(|&i| self.w[i]).collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let frac = self.cfg.protect_top_frac.clamp(0.0, 1.0);
        let q = ((1.0 - frac) * (sorted.len().saturating_sub(1)) as f32).round() as usize;
        let protect_thr = if sorted.is_empty() { f32::MAX } else { sorted[q.min(sorted.len() - 1)] };
        // 3 + 4. downscale then prune (protecting the consolidated top)
        let ds = self.cfg.sleep_downscale;
        let (mut downscaled, mut pruned, mut protected) = (0usize, 0usize, 0usize);
        for &i in &live_idx {
            let orig = self.w[i];
            let protected_edge = orig >= protect_thr;
            self.w[i] = orig * ds;
            downscaled += 1;
            if self.w[i] < self.cfg.sleep_theta && !protected_edge {
                self.dead[i] = true;
                self.w[i] = 0.0;
                pruned += 1;
            } else if protected_edge {
                protected += 1;
            }
        }
        // 5. per-source L1 renorm over survivors
        let mut renorm = 0usize;
        let mut after_sum = 0f32;
        for u in 0..g.node_count() {
            let out = g.out_edge_ids(u as NodeId);
            let mut sum = 0f32;
            for &eid in out {
                let i = eid as usize;
                if i < self.w.len() && !self.dead[i] {
                    sum += self.w[i];
                }
            }
            if sum > 0.0 {
                let k = self.cfg.norm_target / sum;
                for &eid in out {
                    let i = eid as usize;
                    if i < self.w.len() && !self.dead[i] {
                        self.w[i] *= k;
                    }
                }
                renorm += 1;
            }
        }
        let mut live_after = 0usize;
        for i in 0..self.w.len() {
            if !self.dead[i] {
                live_after += 1;
                after_sum += self.w[i];
            }
        }
        SleepReport {
            downscaled,
            pruned,
            protected,
            renormalized_sources: renorm,
            live_after,
            mean_weight_before: before_sum / live_before as f32,
            mean_weight_after: after_sum / live_after.max(1) as f32,
        }
    }

    pub fn stats(&self, g: &TemporalGraph) -> Stats {
        let mut mn = f32::MAX;
        let mut mx = f32::MIN;
        let mut sum = 0f32;
        let mut live = 0usize;
        for i in 0..self.w.len() {
            if self.dead[i] {
                continue;
            }
            live += 1;
            mn = mn.min(self.w[i]);
            mx = mx.max(self.w[i]);
            sum += self.w[i];
        }
        Stats {
            edges_total: self.w.len(),
            edges_live: live,
            edges_dead: self.w.len() - live,
            nodes: g.node_count(),
            lambda_scale: self.lambda_scale(),
            setpoint: self.homeo.setpoint,
            ewma_mass: self.homeo.ewma_mass,
            integral: self.homeo.integral(),
            weight_min: if live > 0 { mn } else { 0.0 },
            weight_max: if live > 0 { mx } else { 0.0 },
            weight_mean: if live > 0 { sum / live as f32 } else { 0.0 },
        }
    }

    /// Serialize the sidecar (weights, last-touch times, per-edge lambda, dead
    /// mask, homeostat, scale) to JSON. EdgeIds are stable across WAL replay, so
    /// this composes with the on-disk WAL. Call `sync` after `load` to cover any
    /// edges appended since the snapshot.
    pub fn save(&self, path: &str) -> Result<(), String> {
        let p = Persisted {
            w: &self.w,
            t_last: &self.t_last,
            neg_lambda: &self.neg_lambda,
            dead: &self.dead,
            lambda_scale: self.lambda_scale(),
            homeo: &self.homeo,
        };
        let s = serde_json::to_string(&p).map_err(|e| e.to_string())?;
        std::fs::write(path, s).map_err(|e| e.to_string())
    }

    /// Load a sidecar snapshot if the file exists (no-op if absent).
    pub fn load_if_exists(&mut self, path: &str) -> Result<(), String> {
        if !std::path::Path::new(path).exists() {
            return Ok(());
        }
        let s = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let p: PersistedOwned = serde_json::from_str(&s).map_err(|e| e.to_string())?;
        self.w = p.w;
        self.t_last = p.t_last;
        self.neg_lambda = p.neg_lambda;
        self.dead = p.dead;
        self.set_lambda_scale(p.lambda_scale);
        self.homeo = p.homeo;
        Ok(())
    }

    /// Resolve an edge id from endpoint names + kind (for touch by name).
    pub fn edge_id(&self, g: &TemporalGraph, src: &str, dst: &str, kind: &str) -> Option<EdgeId> {
        let s = g.id_of(src)?;
        let d = g.id_of(dst)?;
        for &eid in g.out_edge_ids(s) {
            let e = g.edge(eid);
            if e.dst == d && g.kind_str(e.kind_id) == kind && e.valid_to == T_MAX {
                return Some(eid);
            }
        }
        None
    }
}

// Borrowed view for zero-copy save; owned mirror for load.
#[derive(Serialize)]
struct Persisted<'a> {
    w: &'a [f32],
    t_last: &'a [Ts],
    neg_lambda: &'a [f32],
    dead: &'a [bool],
    lambda_scale: f32,
    homeo: &'a Homeostat,
}
#[derive(Deserialize)]
struct PersistedOwned {
    w: Vec<f32>,
    t_last: Vec<Ts>,
    neg_lambda: Vec<f32>,
    dead: Vec<bool>,
    lambda_scale: f32,
    homeo: Homeostat,
}

#[cfg(test)]
mod tests {
    use super::*;
    use psyrag_graph::graph::TemporalGraph;

    const S: Ts = 1000; // one second in ms

    fn seed_graph() -> TemporalGraph {
        // node 0 (hub) -> a,b,c,d strong ; -> e weak ; a -> f
        let json = r#"[
          {"name":"hub","type":"svc/Hub","edges":[
            {"dst":"a","kind":"CALLS"},{"dst":"b","kind":"CALLS"},
            {"dst":"c","kind":"CALLS"},{"dst":"d","kind":"CALLS"},
            {"dst":"e","kind":"CALLS"}]},
          {"name":"a","type":"svc/S","edges":[{"dst":"f","kind":"CALLS"}]}
        ]"#;
        let mut g = TemporalGraph::new();
        psyrag_graph::entity::ingest_entities_mem(&mut g, json, 0, false).unwrap();
        g
    }

    #[test]
    fn sync_covers_all_edges() {
        let g = seed_graph();
        let mut p = PlasticityLayer::new(Config::default());
        p.sync(&g);
        assert_eq!(p.w.len(), g.edge_count());
        assert!(g.edge_count() >= 6);
    }

    #[test]
    fn lazy_decay_matches_closed_form() {
        let g = seed_graph();
        let mut cfg = Config::default();
        cfg.lambda_base = 0.1;
        cfg.beta = 0.0;
        let mut p = PlasticityLayer::new(cfg);
        p.sync(&g);
        let eid = p.edge_id(&g, "hub", "a", "CALLS").unwrap();
        // starts at w0=0.5, valid_from=0; read at 20s
        let w = p.effective_weight(eid, 20 * S);
        assert!((w - 0.5 * (-0.1f32 * 20.0).exp()).abs() < 1e-5, "{w}");
    }

    #[test]
    fn authority_by_kind_slows_decay() {
        let g = {
            let json = r#"[{"name":"x","type":"t","edges":[
                {"dst":"y","kind":"WEAK"},{"dst":"z","kind":"STRONG"}]}]"#;
            let mut g = TemporalGraph::new();
            psyrag_graph::entity::ingest_entities_mem(&mut g, json, 0, false).unwrap();
            g
        };
        let mut cfg = Config::default();
        cfg.lambda_base = 0.2;
        cfg.beta = 1.0;
        cfg.authority_by_kind.insert("STRONG".into(), 9.0); // lambda/10
        let mut p = PlasticityLayer::new(cfg);
        p.sync(&g);
        let weak = p.edge_id(&g, "x", "y", "WEAK").unwrap();
        let strong = p.edge_id(&g, "x", "z", "STRONG").unwrap();
        let ww = p.effective_weight(weak, 30 * S);
        let ws = p.effective_weight(strong, 30 * S);
        assert!(ws > ww && ww < 0.01 && ws > 0.25, "weak={ww} strong={ws}");
    }

    #[test]
    fn retrieve_weights_and_alive_filter() {
        let g = seed_graph();
        let mut p = PlasticityLayer::new(Config::default());
        p.sync(&g);
        let r = p.retrieve(&g, &["hub"], 2, 0.9, 10, 1 * S);
        assert!(r.mass > 0.0);
        assert_eq!(r.top[0].node, "hub"); // seed strongest
        // f is reachable at depth 2 via a
        assert!(r.top.iter().any(|na| na.node == "f"));
    }

    #[test]
    fn homeostat_adapts_scale() {
        let g = seed_graph();
        let mut cfg = Config::default();
        cfg.setpoint = 0.3;
        let mut p = PlasticityLayer::new(cfg);
        p.sync(&g);
        let before = p.lambda_scale();
        for i in 0..50 {
            p.retrieve_and_adapt(&g, &["hub"], 5, (i as Ts) * S);
        }
        // scale moved away from 1.0 to chase the setpoint
        assert!((p.lambda_scale() - before).abs() > 1e-4);
    }

    #[test]
    fn feedback_path_credit_reaches_seed_edge() {
        // hub -> a -> f ; use f. Both hub->a and a->f should gain, a->f more.
        let g = seed_graph();
        let mut cfg = Config::default();
        cfg.lambda_base = 0.0; // isolate reinforcement from decay
        cfg.feedback_gamma = 0.5;
        let mut p = PlasticityLayer::new(cfg);
        p.sync(&g);
        let hub_a = p.edge_id(&g, "hub", "a", "CALLS").unwrap();
        let a_f = p.edge_id(&g, "a", "f", "CALLS").unwrap();
        let w_ha0 = p.effective_weight(hub_a, 1 * S);
        let w_af0 = p.effective_weight(a_f, 1 * S);
        p.feedback(&g, &["hub"], 2, 0.9, 10, 1 * S, &["f"]);
        let w_ha1 = p.effective_weight(hub_a, 1 * S);
        let w_af1 = p.effective_weight(a_f, 1 * S);
        assert!(w_af1 > w_af0, "a->f reinforced");
        assert!(w_ha1 > w_ha0, "hub->a reinforced via path credit");
        let d_af = w_af1 - w_af0;
        let d_ha = w_ha1 - w_ha0;
        assert!(d_af > d_ha, "closer-to-used edge gets more credit: {d_af} vs {d_ha}");
    }

    #[test]
    fn feedback_is_selective() {
        // use only 'a'; sibling 'b' (also a direct hub child) must NOT gain.
        let g = seed_graph();
        let mut cfg = Config::default();
        cfg.lambda_base = 0.0;
        let mut p = PlasticityLayer::new(cfg);
        p.sync(&g);
        let hub_b = p.edge_id(&g, "hub", "b", "CALLS").unwrap();
        let w_b0 = p.effective_weight(hub_b, 1 * S);
        p.feedback(&g, &["hub"], 2, 0.9, 10, 1 * S, &["a"]);
        let w_b1 = p.effective_weight(hub_b, 1 * S);
        assert!((w_b1 - w_b0).abs() < 1e-6, "unused sibling untouched: {w_b0}->{w_b1}");
    }

    #[test]
    fn feedback_converges_used_over_unused() {
        // Repeated "a is useful" makes hub->a dominate hub->b under per-source
        // renorm competition.
        let g = seed_graph();
        let mut cfg = Config::default();
        cfg.lambda_base = 0.02;
        cfg.alpha = 0.2;
        let mut p = PlasticityLayer::new(cfg);
        p.sync(&g);
        let hub_a = p.edge_id(&g, "hub", "a", "CALLS").unwrap();
        let hub_b = p.edge_id(&g, "hub", "b", "CALLS").unwrap();
        for i in 1..40 {
            let t = (i as Ts) * S;
            p.feedback(&g, &["hub"], 1, 0.9, 10, t, &["a"]);
            p.consolidate(&g, t); // renorm competition each cycle
        }
        let wa = p.effective_weight(hub_a, 40 * S);
        let wb = p.effective_weight(hub_b, 40 * S);
        assert!(wa > 2.0 * wb, "used edge should dominate: a={wa} b={wb}");
    }

    #[test]
    fn miss_penalty_depresses_unused_surfaced() {
        let g = seed_graph();
        let mut cfg = Config::default();
        cfg.lambda_base = 0.0;
        cfg.feedback_miss_penalty = 0.5;
        let mut p = PlasticityLayer::new(cfg);
        p.sync(&g);
        let hub_b = p.edge_id(&g, "hub", "b", "CALLS").unwrap();
        let w0 = p.effective_weight(hub_b, 1 * S);
        // use 'a'; 'b' is surfaced but unused -> penalized
        p.feedback(&g, &["hub"], 1, 0.9, 10, 1 * S, &["a"]);
        let w1 = p.effective_weight(hub_b, 1 * S);
        assert!(w1 < w0, "surfaced-but-unused edge depressed: {w0}->{w1}");
    }

    #[test]
    fn episodic_reward_spreads_over_surfaced() {
        // one scalar reward, ByActivation: symmetric children gain ~equally.
        let g = seed_graph();
        let mut cfg = Config::default();
        cfg.lambda_base = 0.0;
        let mut p = PlasticityLayer::new(cfg);
        p.sync(&g);
        let ha = p.edge_id(&g, "hub", "a", "CALLS").unwrap();
        let hb = p.edge_id(&g, "hub", "b", "CALLS").unwrap();
        let a0 = p.effective_weight(ha, 1 * S);
        let b0 = p.effective_weight(hb, 1 * S);
        let rep = p.feedback_reward(&g, &["hub"], 1, 0.9, 10, 1 * S, 1.0, Spread::ByActivation);
        let a1 = p.effective_weight(ha, 1 * S);
        let b1 = p.effective_weight(hb, 1 * S);
        assert!(rep.edges_reinforced > 0 && rep.total_positive_r > 0.0);
        assert!(a1 > a0 && b1 > b0, "episodic lifts all surfaced children");
        assert!((( a1 - a0) - (b1 - b0)).abs() < 1e-6, "symmetric children gain equally");
    }

    #[test]
    fn contrastive_credit_signs() {
        // "a beat b": positive a, negative b, in one Credit::Nodes call.
        let g = seed_graph();
        let mut cfg = Config::default();
        cfg.lambda_base = 0.0;
        let mut p = PlasticityLayer::new(cfg);
        p.sync(&g);
        let ha = p.edge_id(&g, "hub", "a", "CALLS").unwrap();
        let hb = p.edge_id(&g, "hub", "b", "CALLS").unwrap();
        let (a0, b0) = (p.effective_weight(ha, 1 * S), p.effective_weight(hb, 1 * S));
        let (_r, trace) = p.retrieve_traced(&g, &["hub"], 1, 0.9, 10, 1 * S);
        p.apply_credit(&g, &trace, &Credit::Nodes(vec![("a".into(), 1.0), ("b".into(), -1.0)]), 1 * S);
        let (a1, b1) = (p.effective_weight(ha, 1 * S), p.effective_weight(hb, 1 * S));
        assert!(a1 > a0, "preferred up");
        assert!(b1 < b0, "dispreferred down");
    }

    #[test]
    fn sleep_downscales_prunes_and_protects() {
        let g = seed_graph(); // hub -> a,b,c,d,e ; a -> f
        let mut cfg = Config::default();
        cfg.lambda_base = 0.0;
        cfg.sleep_downscale = 0.5;
        cfg.sleep_theta = 0.2;
        cfg.protect_top_frac = 0.25; // protect the top ~quarter
        let mut p = PlasticityLayer::new(cfg);
        p.sync(&g);
        // make hub->a strong (consolidated), hub->e weak (should be pruned)
        let ha = p.edge_id(&g, "hub", "a", "CALLS").unwrap();
        let he = p.edge_id(&g, "hub", "e", "CALLS").unwrap();
        p.w[ha as usize] = 1.0;
        p.w[he as usize] = 0.3;
        let rep = p.sleep(&g, 100 * S);
        // downscaling lowered the mean weight
        assert!(rep.mean_weight_after < rep.mean_weight_before || rep.pruned > 0);
        // the strong edge survived (protected); the weak one was pruned
        assert!(p.effective_weight(ha, 100 * S) > 0.0, "consolidated edge survives");
        assert!(p.effective_weight(he, 100 * S) == 0.0, "weak edge pruned in sleep");
        assert!(rep.protected >= 1 && rep.pruned >= 1);
    }

    #[test]
    fn consolidation_prunes_renorms_and_detects_conflict() {
        // build a graph with a genuine contradiction: p has two open SERVES edges
        let json = r#"[{"name":"p","type":"t","edges":[
            {"dst":"q","kind":"SERVES"},{"dst":"r","kind":"SERVES"},
            {"dst":"weak","kind":"NOTE"}]}]"#;
        let mut g = TemporalGraph::new();
        psyrag_graph::entity::ingest_entities_mem(&mut g, json, 0, false).unwrap();
        let mut cfg = Config::default();
        cfg.w0 = 0.5;
        cfg.theta = 0.05;
        cfg.lambda_base = 0.0; // no decay so weights stay put for the test
        cfg.functional_kinds.insert("SERVES".into()); // SERVES is single-valued here
        let mut p = PlasticityLayer::new(cfg);
        p.sync(&g);
        // make one NOTE edge weak so it gets pruned
        let note = p.edge_id(&g, "p", "weak", "NOTE").unwrap();
        p.w[note as usize] = 0.01;
        // make q the conflict winner
        let pq = p.edge_id(&g, "p", "q", "SERVES").unwrap();
        p.w[pq as usize] = 0.9;
        let (st, conflicts) = p.consolidate(&g, 10 * S);
        assert_eq!(st.pruned, 1, "weak NOTE pruned");
        assert_eq!(st.conflicts_found, 1, "two open SERVES = conflict");
        assert_eq!(conflicts[0].winner_dst, "q");
        assert_eq!(conflicts[0].superseded.len(), 1); // p->r proposed for retire
    }
}
