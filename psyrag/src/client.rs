//! Tiny dependency-free HTTP/1.0 client: just enough to let a standby pull
//! WAL segments and sidecar snapshots from a primary. http:// only (TLS is a
//! reverse-proxy concern, same as the server side).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

/// GET `url` with an optional bearer token and read timeout. Returns the
/// status, headers, and raw body bytes (binary-safe).
pub fn get(url: &str, token: Option<&str>, timeout: Duration) -> Result<Response, String> {
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
        .set_read_timeout(Some(timeout))
        .map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| e.to_string())?;
    let auth = match token {
        Some(t) => format!("Authorization: Bearer {t}\r\n"),
        None => String::new(),
    };
    let req = format!("GET {path} HTTP/1.0\r\nHost: {host}\r\n{auth}Connection: close\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).map_err(|e| e.to_string())?;
    // Split headers/body on the first blank line (CRLF CRLF).
    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("malformed response (no header terminator)")?;
    let head = String::from_utf8_lossy(&raw[..sep]).into_owned();
    let body = raw[sep + 4..].to_vec();
    let status_line = head.split("\r\n").next().unwrap_or("");
    // "HTTP/1.1 200 OK" -> 200
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| format!("bad status line: {status_line:?}"))?;
    Ok(Response { status, body })
}
