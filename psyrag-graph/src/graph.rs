//! Core temporal typed property graph.
//!
//! Design invariants:
//! - Append-only. Nothing is ever overwritten or deleted; state changes close
//!   the previous version/edge interval and open a new one.
//! - Transaction-time temporal model (observed_at / retired_at). A fact is
//!   "alive at t" iff observed_at <= t < retired_at.
//! - Node identity is the provider's full resource name (stable, globally
//!   unique in GCP). NodeId is a dense u32 index into arenas.
//! - Cloud inventory is a typed property graph, NOT a DAG. IAM, peering, PSC
//!   and cross-project references create cycles; traversals must be
//!   cycle-safe (visited sets), never recursion-on-hierarchy.

use serde::Serialize;
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};

/// Unix milliseconds. Kept as a plain i64 to stay dependency-free.
pub type Ts = i64;

pub const T_MAX: Ts = i64::MAX;

// ---------------------------------------------------------------------------
// Interning
// ---------------------------------------------------------------------------

/// Interner for asset types and edge kinds. Types are open-ended strings
/// (schema-as-data): new provider resource types cost nothing.
#[derive(Default)]
pub struct Interner {
    map: HashMap<String, u32>,
    vec: Vec<String>,
}

impl Interner {
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.map.get(s) {
            return id;
        }
        let id = self.vec.len() as u32;
        self.vec.push(s.to_string());
        self.map.insert(s.to_string(), id);
        id
    }
    pub fn resolve(&self, id: u32) -> &str {
        &self.vec[id as usize]
    }
    pub fn get(&self, s: &str) -> Option<u32> {
        self.map.get(s).copied()
    }
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

pub type NodeId = u32;
pub type EdgeId = u32;

#[derive(Debug, Clone, Serialize)]
pub struct NodeVersion {
    pub observed_at: Ts,
    pub retired_at: Ts, // T_MAX = open
    /// Interned provenance label (0 = none). Per-version: each observation
    /// records where it came from.
    pub origin_id: u32,
    pub props_hash: u64,
    /// Canonical serialized JSON. Stored as bytes, not a Value tree: a
    /// parsed Value costs ~7x the serialized size in heap (measured), and
    /// traversal/diff never touch props. Parse on access via props_value().
    props_json: Box<str>,
}

impl NodeVersion {
    /// Parse the stored properties. ~µs-scale; only query paths that
    /// actually read props pay it.
    pub fn props_value(&self) -> Value {
        serde_json::from_str(&self.props_json).unwrap_or(Value::Null)
    }
    /// The canonical serialized form, zero-copy.
    pub fn props_json(&self) -> &str {
        &self.props_json
    }
}

pub struct Node {
    pub name: String,
    pub type_id: u32,
    pub versions: Vec<NodeVersion>, // ordered by observed_at
    /// True for nodes created only because something referenced them
    /// (target not yet ingested). Upgraded in place on first real observation.
    pub placeholder: bool,
}

impl Node {
    /// Version alive at t, if any.
    pub fn version_at(&self, t: Ts) -> Option<&NodeVersion> {
        // versions are few per node; linear scan from the back is fastest in practice
        self.versions
            .iter()
            .rev()
            .find(|v| v.observed_at <= t && t < v.retired_at)
    }
    pub fn alive_at(&self, t: Ts) -> bool {
        self.version_at(t).is_some()
    }
    fn open_version_mut(&mut self) -> Option<&mut NodeVersion> {
        self.versions.last_mut().filter(|v| v.retired_at == T_MAX)
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Edge {
    pub src: NodeId,
    pub dst: NodeId,
    pub kind_id: u32,
    /// Interned provenance label (0 = none), set at first observation of
    /// this open interval.
    pub origin_id: u32,
    pub valid_from: Ts,
    pub valid_to: Ts, // T_MAX = open
}

impl Edge {
    pub fn alive_at(&self, t: Ts) -> bool {
        self.valid_from <= t && t < self.valid_to
    }
}

// ---------------------------------------------------------------------------
// Ops (event-sourced mutation, WAL record format)
// ---------------------------------------------------------------------------

/// Every mutation as a serializable, name-addressed record. The WAL is a
/// NDJSON stream of these; replaying them reproduces identical graph state
/// including full history.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    ObserveNode {
        name: String,
        asset_type: String,
        props: Value,
        ts: Ts,
        /// Provenance label (source/session/principal — opaque to the graph;
        /// prefix conventions like "user:alice/session:42" enable
        /// purge-by-subject). Absent on pre-provenance logs.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin: Option<String>,
    },
    ObservePlaceholder {
        name: String,
        inferred_type: String,
        ts: Ts,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin: Option<String>,
    },
    RetireNode {
        name: String,
        ts: Ts,
    },
    ObserveEdge {
        src: String,
        dst: String,
        kind: String,
        ts: Ts,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin: Option<String>,
    },
    RetireEdge {
        src: String,
        dst: String,
        kind: String,
        ts: Ts,
    },
}

// ---------------------------------------------------------------------------
// Graph
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct TemporalGraph {
    pub types: Interner,
    pub kinds: Interner,
    /// Provenance labels ("who/where did this fact come from"), interned.
    /// Id 0 is reserved for "" = no recorded origin.
    pub origins: Interner,
    nodes: Vec<Node>,
    name_to_id: HashMap<String, NodeId>,
    edges: Vec<Edge>,
    /// (src, dst, kind) -> index of the currently-open edge, if any.
    open_edges: HashMap<(NodeId, NodeId, u32), EdgeId>,
    out_adj: Vec<Vec<EdgeId>>,
    in_adj: Vec<Vec<EdgeId>>,
    /// Running estimate of this graph's heap footprint, maintained on every
    /// observation (append-only, so it only grows; rebuilt structures start
    /// fresh). An ESTIMATE for quota/budget decisions, not an exact RSS.
    approx_bytes: usize,
    /// Inverted token index over node names: lowercase alphanumeric runs of
    /// the name -> NodeIds containing them. Powers O(log N + hits) seed
    /// matching instead of a full-name scan. BTreeMap so token-PREFIX
    /// queries are range scans.
    name_index: BTreeMap<String, Vec<NodeId>>,
}

/// Lowercase alphanumeric runs of a name, minimum 2 chars: the indexable
/// tokens. "svc/metering-api" -> ["svc", "metering", "api"].
fn name_tokens(name: &str) -> Vec<String> {
    name.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| t.len() >= 2)
        .map(str::to_string)
        .collect()
}

fn hash_str(s: &str) -> u64 {
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Follow edges src -> dst (downstream: what does this affect?)
    Down,
    /// Follow edges dst -> src (upstream: what does this depend on?)
    Up,
    Both,
}

#[derive(Debug, Clone, Serialize)]
pub struct Reach {
    pub node: String,
    pub node_type: String,
    pub depth: u32,
    /// Full traversal path from the origin, rendered as
    /// "origin -[KIND]-> hop -[KIND]-> node". Built for direct prompt
    /// injection so the LLM sees *why* the node is in the blast radius.
    pub path: String,
}

#[derive(Debug, Default, Serialize)]
pub struct GraphDiff {
    pub nodes_added: Vec<String>,
    pub nodes_removed: Vec<String>,
    pub nodes_changed: Vec<String>,
    pub edges_added: Vec<String>,
    pub edges_removed: Vec<String>,
}

impl GraphDiff {
    pub fn is_empty(&self) -> bool {
        self.nodes_added.is_empty()
            && self.nodes_removed.is_empty()
            && self.nodes_changed.is_empty()
            && self.edges_added.is_empty()
            && self.edges_removed.is_empty()
    }
}

/// Rough per-record overheads for the memory estimate: struct sizes plus
/// map/adjacency entries, rounded generously.
const NODE_OVERHEAD: usize = 160; // Node + name_to_id entry + adjacency vecs
const VERSION_OVERHEAD: usize = 72; // NodeVersion + Box<str> header
const EDGE_OVERHEAD: usize = 104; // Edge + open_edges entry + 2 adjacency slots

impl TemporalGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Estimated heap bytes held by graph structures (names, props, edges,
    /// adjacency). Sidecar columns are ~33 bytes/edge on top of this.
    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }
    pub fn id_of(&self, name: &str) -> Option<NodeId> {
        self.name_to_id.get(name).copied()
    }
    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id as usize]
    }

    // -- Additive read-only accessors (v0.3.1+plasticity) --------------------
    // Zero new state, zero behavior change. These expose the adjacency and
    // edge records that already exist so an external, EdgeId-keyed layer
    // (e.g. psyrag-core) can do weighted traversal without owning a
    // second copy of the graph. EdgeId is stable and dense (append-only), so
    // a parallel sidecar indexed by EdgeId stays correct for the process life.

    /// Out-edge ids of a node (may include retired edges; filter by alive_at).
    pub fn out_edge_ids(&self, id: NodeId) -> &[EdgeId] {
        &self.out_adj[id as usize]
    }
    /// In-edge ids of a node.
    pub fn in_edge_ids(&self, id: NodeId) -> &[EdgeId] {
        &self.in_adj[id as usize]
    }
    /// Edge record by id. Ids are stable/dense under the append-only model.
    pub fn edge(&self, eid: EdgeId) -> &Edge {
        &self.edges[eid as usize]
    }
    /// Node name by id.
    pub fn node_name(&self, id: NodeId) -> &str {
        &self.nodes[id as usize].name
    }
    /// Node type string by id.
    pub fn node_type(&self, id: NodeId) -> &str {
        self.types.resolve(self.nodes[id as usize].type_id)
    }
    /// Resolve an interned edge-kind id to its string.
    pub fn kind_str(&self, kind_id: u32) -> &str {
        self.kinds.resolve(kind_id)
    }
    /// Provenance label of an edge ("" = none recorded).
    pub fn edge_origin(&self, eid: EdgeId) -> &str {
        let id = self.edges[eid as usize].origin_id;
        if id == 0 && self.origins.get("").is_none() {
            return "";
        }
        self.origins.resolve(id)
    }
    /// Provenance label of a node's version alive at `t` ("" = none).
    pub fn node_origin_at(&self, id: NodeId, t: Ts) -> &str {
        match self.nodes[id as usize].version_at(t) {
            Some(v) if v.origin_id != 0 => self.origins.resolve(v.origin_id),
            _ => "",
        }
    }

    fn ensure_node(&mut self, name: &str, type_id: u32, placeholder: bool) -> NodeId {
        if let Some(&id) = self.name_to_id.get(name) {
            let n = &mut self.nodes[id as usize];
            if n.placeholder && !placeholder {
                n.placeholder = false;
                n.type_id = type_id; // real type wins over inferred
            }
            return id;
        }
        let id = self.nodes.len() as NodeId;
        self.approx_bytes += NODE_OVERHEAD + name.len() * 2; // node + index key
        for tok in name_tokens(name) {
            self.approx_bytes += tok.len() + 28; // token entry + id slot
            let ids = self.name_index.entry(tok).or_default();
            if ids.last() != Some(&id) {
                ids.push(id);
            }
        }
        self.nodes.push(Node {
            name: name.to_string(),
            type_id,
            versions: Vec::new(),
            placeholder,
        });
        self.name_to_id.insert(name.to_string(), id);
        self.out_adj.push(Vec::new());
        self.in_adj.push(Vec::new());
        id
    }

    fn intern_origin(&mut self, origin: Option<&str>) -> u32 {
        // Reserve interner slot 0 for "" (no origin) so origin_id 0 is
        // always the none marker.
        if self.origins.get("").is_none() {
            self.origins.intern("");
        }
        match origin {
            Some(o) if !o.is_empty() => self.origins.intern(o),
            _ => 0,
        }
    }

    /// Record an observation of a node's state at `ts`. No-op if the live
    /// properties are unchanged (hash-compared); otherwise closes the current
    /// version and opens a new one.
    pub fn observe_node(&mut self, name: &str, asset_type: &str, props: Value, ts: Ts) -> NodeId {
        self.observe_node_from(name, asset_type, props, ts, None)
    }

    /// `observe_node` carrying a provenance label.
    pub fn observe_node_from(
        &mut self,
        name: &str,
        asset_type: &str,
        props: Value,
        ts: Ts,
        origin: Option<&str>,
    ) -> NodeId {
        let origin_id = self.intern_origin(origin);
        let type_id = self.types.intern(asset_type);
        let id = self.ensure_node(name, type_id, false);
        // serde_json's default Map is BTreeMap-backed => to_string() is
        // canonical for semantically equal objects.
        let s = props.to_string();
        let h = hash_str(&s);
        let node = &mut self.nodes[id as usize];
        if let Some(open) = node.open_version_mut() {
            if open.props_hash == h {
                return id; // unchanged
            }
            open.retired_at = ts;
        }
        self.approx_bytes += VERSION_OVERHEAD + s.len();
        node.versions.push(NodeVersion {
            observed_at: ts,
            retired_at: T_MAX,
            origin_id,
            props_hash: h,
            props_json: s.into_boxed_str(),
        });
        id
    }

    /// Placeholder node: something referenced `name` but we haven't ingested
    /// it yet. Alive from ts so traversals can cross partial ingestion.
    pub fn observe_placeholder(&mut self, name: &str, inferred_type: &str, ts: Ts) -> NodeId {
        self.observe_placeholder_from(name, inferred_type, ts, None)
    }

    pub fn observe_placeholder_from(
        &mut self,
        name: &str,
        inferred_type: &str,
        ts: Ts,
        origin: Option<&str>,
    ) -> NodeId {
        let origin_id = self.intern_origin(origin);
        let type_id = self.types.intern(inferred_type);
        let id = self.ensure_node(name, type_id, true);
        if self.nodes[id as usize].open_version_mut().is_none() {
            self.approx_bytes += VERSION_OVERHEAD + 4;
            self.nodes[id as usize].versions.push(NodeVersion {
                observed_at: ts,
                retired_at: T_MAX,
                origin_id,
                props_hash: 0,
                props_json: "null".into(),
            });
        }
        id
    }

    /// Close the node's open version and all its open edges at `ts`.
    pub fn retire_node(&mut self, name: &str, ts: Ts) {
        let Some(&id) = self.name_to_id.get(name) else { return };
        if let Some(open) = self.nodes[id as usize].open_version_mut() {
            open.retired_at = ts;
        }
        let touching: Vec<EdgeId> = self.out_adj[id as usize]
            .iter()
            .chain(self.in_adj[id as usize].iter())
            .copied()
            .collect();
        for eid in touching {
            let e = self.edges[eid as usize];
            if e.valid_to == T_MAX {
                self.close_edge(eid, ts);
            }
        }
    }

    fn close_edge(&mut self, eid: EdgeId, ts: Ts) {
        let e = &mut self.edges[eid as usize];
        e.valid_to = ts;
        let key = (e.src, e.dst, e.kind_id);
        self.open_edges.remove(&key);
    }

    /// Record that an edge exists at `ts`. Idempotent while the edge is open
    /// (the origin of the FIRST observation of an open interval sticks).
    pub fn observe_edge(&mut self, src: NodeId, dst: NodeId, kind: &str, ts: Ts) -> EdgeId {
        self.observe_edge_from(src, dst, kind, ts, None)
    }

    pub fn observe_edge_from(
        &mut self,
        src: NodeId,
        dst: NodeId,
        kind: &str,
        ts: Ts,
        origin: Option<&str>,
    ) -> EdgeId {
        let origin_id = self.intern_origin(origin);
        let kind_id = self.kinds.intern(kind);
        let key = (src, dst, kind_id);
        if let Some(&eid) = self.open_edges.get(&key) {
            return eid;
        }
        let eid = self.edges.len() as EdgeId;
        self.approx_bytes += EDGE_OVERHEAD;
        self.edges.push(Edge {
            src,
            dst,
            kind_id,
            origin_id,
            valid_from: ts,
            valid_to: T_MAX,
        });
        self.open_edges.insert(key, eid);
        self.out_adj[src as usize].push(eid);
        self.in_adj[dst as usize].push(eid);
        eid
    }

    pub fn retire_edge(&mut self, src: NodeId, dst: NodeId, kind: &str, ts: Ts) {
        if let Some(kind_id) = self.kinds.get(kind) {
            if let Some(&eid) = self.open_edges.get(&(src, dst, kind_id)) {
                self.close_edge(eid, ts);
            }
        }
    }

    // -- Reconciliation ------------------------------------------------------

    /// Full-snapshot reconciliation: after ingesting a complete snapshot taken
    /// at `ts`, retire every non-placeholder node that was alive but NOT in
    /// `seen`. This is the zombie-pruning path: deletions in the cloud that
    /// produce no event still converge on the next snapshot.
    pub fn reconcile(&mut self, seen: &HashSet<String>, ts: Ts) -> Vec<String> {
        let stale = self.stale_nodes(seen, ts);
        for name in &stale {
            self.retire_node(name, ts);
        }
        stale
    }

    /// Read-only half of reconciliation: which live, non-placeholder nodes
    /// did this snapshot NOT assert? Used by the persistence layer, which
    /// must journal the resulting retirements as explicit ops.
    pub fn stale_nodes(&self, seen: &HashSet<String>, ts: Ts) -> Vec<String> {
        self.nodes
            .iter()
            .filter(|n| !n.placeholder && n.alive_at(ts) && !seen.contains(&n.name))
            .map(|n| n.name.clone())
            .collect()
    }

    /// Open outgoing edges of `src` as (dst_name, kind) pairs. Used by
    /// snapshot ingestion to retire edges a re-observed source stopped
    /// asserting (reference retargeting with both endpoints still alive).
    pub fn open_out_edges_named(&self, src: &str) -> Vec<(String, String)> {
        let Some(id) = self.id_of(src) else { return Vec::new() };
        self.out_adj[id as usize]
            .iter()
            .filter_map(|&eid| {
                let e = &self.edges[eid as usize];
                (e.valid_to == T_MAX).then(|| {
                    (
                        self.nodes[e.dst as usize].name.clone(),
                        self.kinds.resolve(e.kind_id).to_string(),
                    )
                })
            })
            .collect()
    }

    /// Apply a serialized op. The single entry point the WAL replays through;
    /// ops are name-addressed so the log is stable across processes
    /// (NodeIds are process-local).
    pub fn apply(&mut self, op: &Op) {
        match op {
            Op::ObserveNode { name, asset_type, props, ts, origin } => {
                self.observe_node_from(name, asset_type, props.clone(), *ts, origin.as_deref());
            }
            Op::ObservePlaceholder { name, inferred_type, ts, origin } => {
                self.observe_placeholder_from(name, inferred_type, *ts, origin.as_deref());
            }
            Op::RetireNode { name, ts } => self.retire_node(name, *ts),
            Op::ObserveEdge { src, dst, kind, ts, origin } => {
                // Edges may arrive before either endpoint's real record;
                // materialize endpoints as untyped placeholders if needed.
                let s = self.placeholder_if_absent(src, *ts);
                let d = self.placeholder_if_absent(dst, *ts);
                self.observe_edge_from(s, d, kind, *ts, origin.as_deref());
            }
            Op::RetireEdge { src, dst, kind, ts } => {
                if let (Some(s), Some(d)) = (self.id_of(src), self.id_of(dst)) {
                    self.retire_edge(s, d, kind, *ts);
                }
            }
        }
    }

    fn placeholder_if_absent(&mut self, name: &str, ts: Ts) -> NodeId {
        match self.id_of(name) {
            Some(id) => id,
            None => self.observe_placeholder(name, "unknown/unknown", ts),
        }
    }

    // -- Temporal queries ----------------------------------------------------

    /// Resolve free-text tokens to node ids via the name index: each query
    /// token matches nodes whose name contains a token equal to it or
    /// PREFIXED by it ("meter" finds "svc/metering-api"). Results are
    /// deterministic (ascending NodeId), capped at `limit`.
    pub fn match_tokens(&self, tokens: &[String], limit: usize) -> Vec<NodeId> {
        let mut hits: std::collections::BTreeSet<NodeId> = std::collections::BTreeSet::new();
        for q in tokens {
            let q = q.to_lowercase();
            if q.is_empty() {
                continue;
            }
            for (tok, ids) in self.name_index.range(q.clone()..) {
                if !tok.starts_with(&q) {
                    break;
                }
                hits.extend(ids.iter().copied());
                if hits.len() >= limit.saturating_mul(4) {
                    break;
                }
            }
        }
        hits.into_iter().take(limit).collect()
    }

    /// Names of all nodes alive at t.
    pub fn alive_at(&self, t: Ts) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|n| n.alive_at(t))
            .map(|n| n.name.as_str())
            .collect()
    }

    /// What changed between t1 and t2. This is the incident question:
    /// "what changed in this stack in the last 5 minutes?"
    pub fn diff(&self, t1: Ts, t2: Ts) -> GraphDiff {
        let mut d = GraphDiff::default();
        for n in &self.nodes {
            if n.placeholder {
                continue;
            }
            let v1 = n.version_at(t1);
            let v2 = n.version_at(t2);
            match (v1, v2) {
                (None, Some(_)) => d.nodes_added.push(n.name.clone()),
                (Some(_), None) => d.nodes_removed.push(n.name.clone()),
                (Some(a), Some(b)) if a.props_hash != b.props_hash => {
                    d.nodes_changed.push(n.name.clone())
                }
                _ => {}
            }
        }
        for e in &self.edges {
            let a1 = e.alive_at(t1);
            let a2 = e.alive_at(t2);
            if a1 == a2 {
                continue;
            }
            let s = format!(
                "{} -[{}]-> {}",
                self.nodes[e.src as usize].name,
                self.kinds.resolve(e.kind_id),
                self.nodes[e.dst as usize].name
            );
            if a2 {
                d.edges_added.push(s)
            } else {
                d.edges_removed.push(s)
            }
        }
        d
    }

    /// Cycle-safe BFS over edges alive at `t`. Returns reachable nodes with
    /// the traversal path that got there — the explainability payload.
    pub fn blast_radius(
        &self,
        origin: &str,
        t: Ts,
        dir: Direction,
        max_depth: u32,
    ) -> Vec<Reach> {
        let Some(start) = self.id_of(origin) else { return Vec::new() };
        if !self.nodes[start as usize].alive_at(t) {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut visited: HashSet<NodeId> = HashSet::from([start]);
        let mut q: VecDeque<(NodeId, u32, String)> =
            VecDeque::from([(start, 0, origin.to_string())]);

        while let Some((cur, depth, path)) = q.pop_front() {
            if depth >= max_depth {
                continue;
            }
            let step = |nb: NodeId, kind_id: u32, arrow: &str, out: &mut Vec<Reach>,
                            q: &mut VecDeque<(NodeId, u32, String)>,
                            visited: &mut HashSet<NodeId>| {
                if visited.contains(&nb) {
                    return;
                }
                let n = &self.nodes[nb as usize];
                if !n.alive_at(t) {
                    return;
                }
                visited.insert(nb);
                let p = format!("{} {}[{}]{} {}", path,
                    if arrow == ">" { "-" } else { "<" },
                    self.kinds.resolve(kind_id),
                    if arrow == ">" { "->" } else { "-" },
                    n.name);
                out.push(Reach {
                    node: n.name.clone(),
                    node_type: self.types.resolve(n.type_id).to_string(),
                    depth: depth + 1,
                    path: p.clone(),
                });
                q.push_back((nb, depth + 1, p));
            };
            if matches!(dir, Direction::Down | Direction::Both) {
                for &eid in &self.out_adj[cur as usize] {
                    let e = self.edges[eid as usize];
                    if e.alive_at(t) {
                        step(e.dst, e.kind_id, ">", &mut out, &mut q, &mut visited);
                    }
                }
            }
            if matches!(dir, Direction::Up | Direction::Both) {
                for &eid in &self.in_adj[cur as usize] {
                    let e = self.edges[eid as usize];
                    if e.alive_at(t) {
                        step(e.src, e.kind_id, "<", &mut out, &mut q, &mut visited);
                    }
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use std::time::Instant;

    /// Not a real benchmark harness — a scale smoke test. 200k nodes, ~600k
    /// edges (a large enterprise org), timed diff and blast radius.
    #[test]
    fn scale_smoke() {
        let mut g = TemporalGraph::new();
        let n_projects = 200;
        let per_project = 1000;
        let t1 = 1_000;
        let t2 = 2_000;
        for p in 0..n_projects {
            let proj = format!("//crm/projects/p{p}");
            let pid = g.observe_node(&proj, "crm/Project", Value::Null, t1);
            let vpc = format!("//compute/p{p}/networks/vpc");
            let vid = g.observe_node(&vpc, "compute/Network", Value::Null, t1);
            g.observe_edge(pid, vid, "CONTAINS", t1);
            for i in 0..per_project {
                let name = format!("//run/p{p}/services/s{i}");
                let props = serde_json::json!({"image": format!("img:v{}", i % 3)});
                let sid = g.observe_node(&name, "run/Service", props, t1);
                g.observe_edge(pid, sid, "CONTAINS", t1);
                g.observe_edge(sid, vid, "REFERENCES", t1);
            }
        }
        // drift: 1% of services change at t2
        for p in 0..n_projects {
            for i in (0..per_project).step_by(100) {
                let name = format!("//run/p{p}/services/s{i}");
                g.observe_node(&name, "run/Service", serde_json::json!({"image": "img:vNEW"}), t2);
            }
        }
        let t = Instant::now();
        let d = g.diff(t1 + 1, t2 + 1);
        let diff_ms = t.elapsed().as_millis();
        assert_eq!(d.nodes_changed.len(), n_projects * (per_project / 100));

        let t = Instant::now();
        let hit = g.blast_radius("//compute/p0/networks/vpc", t2 + 1, Direction::Up, 3);
        let br_us = t.elapsed().as_micros();
        assert!(hit.len() >= per_project);

        eprintln!(
            "nodes={} edges={} | full diff: {diff_ms}ms | blast radius ({} hits): {br_us}us",
            g.node_count(), g.edge_count(), hit.len()
        );
    }
}
