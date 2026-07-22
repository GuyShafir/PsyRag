use serde_json::{json, Value};

#[derive(Debug)]
pub struct Request {
    pub id: Option<Value>,
    pub method: String,
    pub params: Value,
}

#[derive(Debug)]
pub struct Response(pub Value);

impl Response {
    pub fn result(id: &Option<Value>, r: Value) -> Response {
        Response(json!({"jsonrpc": "2.0", "id": id.clone().unwrap_or(Value::Null), "result": r}))
    }
    pub fn error(id: &Option<Value>, code: i64, msg: &str) -> Response {
        Response(json!({"jsonrpc": "2.0", "id": id.clone().unwrap_or(Value::Null),
                        "error": {"code": code, "message": msg}}))
    }
    pub fn to_line(&self) -> String {
        self.0.to_string()
    }
}

/// Parse one line as a JSON-RPC request. On malformed JSON, return a ready
/// parse-error response (-32700, id null) so the caller can just send it.
pub fn parse(line: &str) -> Result<Request, Response> {
    let v: Value = serde_json::from_str(line)
        .map_err(|_| Response::error(&None, -32700, "parse error"))?;
    let method = v["method"].as_str()
        .ok_or_else(|| Response::error(&None, -32600, "missing method"))?
        .to_string();
    // id absent => notification (id stays None; no reply expected).
    let id = if v.get("id").is_some() { Some(v["id"].clone()) } else { None };
    Ok(Request { id, method, params: v["params"].clone() })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_request() {
        let r = parse(r#"{"jsonrpc":"2.0","id":1,"method":"ping","params":{}}"#).unwrap();
        assert_eq!(r.method, "ping");
        assert_eq!(r.id, Some(json!(1)));
    }

    #[test]
    fn parse_error_is_a_sendable_response() {
        let resp = parse("not json").unwrap_err();
        let v = resp.0;
        assert_eq!(v["error"]["code"], json!(-32700));
        assert_eq!(v["id"], Value::Null);
    }

    #[test]
    fn result_and_error_shapes() {
        let ok = Response::result(&Some(json!(7)), json!({"pong": true}));
        let ov: Value = serde_json::from_str(&ok.to_line()).unwrap();
        assert_eq!(ov["jsonrpc"], "2.0");
        assert_eq!(ov["id"], json!(7));
        assert_eq!(ov["result"]["pong"], json!(true));

        let er = Response::error(&Some(json!(7)), -32601, "no such method");
        let ev: Value = serde_json::from_str(&er.to_line()).unwrap();
        assert_eq!(ev["error"]["code"], json!(-32601));
        assert_eq!(ev["error"]["message"], "no such method");
    }
}
