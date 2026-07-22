use crate::engine::Engine;
use crate::mcp::jsonrpc::{Request, Response};
use crate::mcp::recall::{recall, TraceRing};
use serde_json::json;

pub const PROTOCOL_VERSION: &str = "2024-11-05";

fn tool_schema() -> serde_json::Value {
    json!({
        "name": "psyrag_recall",
        "description": "Recall project files relevant to a query, ranked by learned usefulness. \
                        Memory adapts: files you actually open after a recall grow more salient.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "what you're looking for"},
                "k": {"type": "integer", "description": "max results (default 10)"},
                "depth": {"type": "integer", "description": "hops to spread (default 2)"}
            },
            "required": ["query"]
        }
    })
}

pub fn dispatch(engine: &mut Engine, ring: &mut TraceRing, req: &Request) -> Option<Response> {
    match req.method.as_str() {
        "initialize" => Some(Response::result(&req.id, json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "psyrag", "version": env!("CARGO_PKG_VERSION")}
        }))),
        "notifications/initialized" => None, // notification: no reply
        "ping" => Some(Response::result(&req.id, json!({}))),
        "tools/list" => Some(Response::result(&req.id, json!({"tools": [tool_schema()]}))),
        "tools/call" => {
            let args = &req.params["arguments"];
            if req.params["name"] != "psyrag_recall" {
                return Some(Response::error(&req.id, -32602,
                    &format!("unknown tool {}", req.params["name"])));
            }
            let query = args["query"].as_str().unwrap_or("");
            if query.is_empty() {
                return Some(Response::error(&req.id, -32602, "query is required"));
            }
            let k = args["k"].as_u64().unwrap_or(10) as usize;
            let depth = args["depth"].as_u64().unwrap_or(2) as u32;
            let text = recall(engine, ring, query, k, depth);
            Some(Response::result(&req.id, json!({
                "content": [{"type": "text", "text": text}]
            })))
        }
        // notifications (no id) we don't handle: swallow. requests: method not found.
        _ => req.id.as_ref().map(|_| Response::error(&req.id, -32601, "method not found")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::jsonrpc::parse;

    fn eng() -> Engine {
        // minimal in-memory-ish engine over a temp WAL. Each call gets its own
        // directory (pid + a monotonic counter) — these 4 tests run in parallel
        // by default, and PersistentGraph::open takes an exclusive flock keyed
        // to the open file description, so two tests sharing one WAL path would
        // spuriously fail with "locked by another process".
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("psyrag-proto-{}-{}", std::process::id(), n));
        std::fs::create_dir_all(&dir).unwrap();
        let wal = dir.join("w.wal");
        let pg = psyrag_graph::PersistentGraph::open(&wal).unwrap();
        let mut layer = psyrag_core::PlasticityLayer::new(Default::default());
        layer.sync(pg.graph());
        Engine { pg, layer, sidecar_path: dir.join("s.json").to_string_lossy().into(),
                 traces: crate::engine::TraceStore::in_memory(16),
                 idem: crate::engine::IdemStore::in_memory(16, 60_000), wedged: None }
    }

    #[test]
    fn initialize_advertises_tools_capability() {
        let mut e = eng();
        let mut ring = TraceRing::new(8);
        let req = parse(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#).unwrap();
        let resp = dispatch(&mut e, &mut ring, &req).unwrap();
        assert_eq!(resp.0["result"]["protocolVersion"], PROTOCOL_VERSION);
        assert!(resp.0["result"]["capabilities"]["tools"].is_object());
    }

    #[test]
    fn tools_list_contains_recall() {
        let mut e = eng();
        let mut ring = TraceRing::new(8);
        let req = parse(r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#).unwrap();
        let resp = dispatch(&mut e, &mut ring, &req).unwrap();
        let tools = resp.0["result"]["tools"].as_array().unwrap();
        assert_eq!(tools[0]["name"], "psyrag_recall");
    }

    #[test]
    fn notification_gets_no_reply() {
        let mut e = eng();
        let mut ring = TraceRing::new(8);
        let req = parse(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#).unwrap();
        assert!(dispatch(&mut e, &mut ring, &req).is_none());
    }

    #[test]
    fn unknown_method_is_method_not_found() {
        let mut e = eng();
        let mut ring = TraceRing::new(8);
        let req = parse(r#"{"jsonrpc":"2.0","id":9,"method":"nope","params":{}}"#).unwrap();
        let resp = dispatch(&mut e, &mut ring, &req).unwrap();
        assert_eq!(resp.0["error"]["code"], -32601);
    }
}
