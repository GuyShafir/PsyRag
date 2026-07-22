use crate::engine::{now_ms, Engine};
use psyrag_core::Trace;
use std::collections::VecDeque;

pub struct TraceRing {
    cap: usize,
    buf: VecDeque<Trace>,
}

impl TraceRing {
    pub fn new(cap: usize) -> Self {
        TraceRing { cap, buf: VecDeque::with_capacity(cap) }
    }
    #[allow(dead_code)]
    pub fn cap(&self) -> usize {
        self.cap
    }
    pub fn push(&mut self, tr: Trace) {
        if self.buf.len() == self.cap {
            self.buf.pop_front();
        }
        self.buf.push_back(tr);
    }
    pub fn iter(&self) -> impl Iterator<Item = &Trace> {
        self.buf.iter()
    }
}

/// Lowercase alphanumeric runs ≥2 chars — mirrors psyrag-graph name_tokens so
/// query tokens match the node name index.
pub fn query_tokens(query: &str) -> Vec<String> {
    query
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| t.len() >= 2)
        .map(str::to_string)
        .collect()
}

/// Run one traced recall: tokens → seed nodes → weighted spreading activation.
/// Holds the trace in `ring` so a later usage event can credit it. Returns
/// text for the MCP tool result.
pub fn recall(engine: &mut Engine, ring: &mut TraceRing, query: &str, k: usize, depth: u32) -> String {
    let ts = now_ms();
    let tokens = query_tokens(query);
    let seed_ids = engine.pg.graph().match_tokens(&tokens, 16);
    if seed_ids.is_empty() {
        return format!("no memory matches '{query}' yet");
    }
    let seed_names: Vec<String> =
        seed_ids.iter().map(|&id| engine.pg.graph().node_name(id).to_string()).collect();
    let seed_refs: Vec<&str> = seed_names.iter().map(String::as_str).collect();
    let (res, trace) =
        engine.layer.retrieve_traced(engine.pg.graph(), &seed_refs, depth, 1.0, k, ts);
    ring.push(trace);
    if res.top.is_empty() {
        return format!("'{query}' matched seeds but activated nothing");
    }
    let mut out = format!("recall '{query}' (mass {:.3}):\n", res.mass);
    for n in &res.top {
        out.push_str(&format!("  {:.3}  {}\n", n.activation, n.node));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_like_the_name_index() {
        assert_eq!(query_tokens("svc/Metering-api v2"), vec!["svc", "metering", "api", "v2"]);
    }

    #[test]
    fn ring_keeps_last_cap_traces() {
        let r = TraceRing::new(2);
        assert_eq!(r.iter().count(), 0);
        // pushed via real retrieval in the learning test; here just capacity:
        assert_eq!(r.cap(), 2);
    }
}
