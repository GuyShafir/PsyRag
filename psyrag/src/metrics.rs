//! Metrics DTO (mirrors the layer's `Stats`) plus a tiny dependency-free HTTP
//! GET so the monitor needs no client crate. localhost polling only.

use serde::Deserialize;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct Metrics {
    pub edges_total: usize,
    pub edges_live: usize,
    pub edges_dead: usize,
    pub nodes: usize,
    pub lambda_scale: f32,
    pub setpoint: f32,
    pub ewma_mass: f32,
    pub integral: f32,
    pub weight_min: f32,
    pub weight_max: f32,
    pub weight_mean: f32,
}

/// GET a URL like http://127.0.0.1:8080/metrics and parse JSON.
pub fn fetch(url: &str) -> Result<Metrics, String> {
    let body = http_get(url)?;
    serde_json::from_str(&body).map_err(|e| format!("parse: {e}"))
}

fn http_get(url: &str) -> Result<String, String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or("only http:// supported")?;
    let (hostport, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let host = hostport.split(':').next().unwrap_or("127.0.0.1");
    let mut stream =
        TcpStream::connect(hostport).map_err(|e| format!("connect {hostport}: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|e| e.to_string())?;
    let req = format!("GET {path} HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).map_err(|e| e.to_string())?;
    // split headers/body on the blank line
    match raw.split_once("\r\n\r\n") {
        Some((_, body)) => Ok(body.to_string()),
        None => Err("malformed response".to_string()),
    }
}
