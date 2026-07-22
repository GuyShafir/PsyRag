use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

fn temp_repo() -> std::path::PathBuf {
	static N: AtomicU64 = AtomicU64::new(0);
	let uniq = N.fetch_add(1, Ordering::Relaxed);
	// Use a short base path directly under /tmp (not std::env::temp_dir(), which on
	// macOS resolves to a long /var/folders/.../T/ path). run_mcp binds a unix-domain
	// socket at .psyrag/mcp.sock, and macOS caps socket paths at ~104 bytes — a long
	// temp_dir() base would overflow that and break the bind.
	let d = std::path::PathBuf::from("/tmp")
		.join(format!("psyrag-mcp-it-{}-{}", std::process::id(), uniq));
	std::fs::create_dir_all(d.join(".git")).unwrap();
	d
}

#[test]
fn handshake_then_recall_over_stdio() {
	let repo = temp_repo();
	let mut child = Command::new(env!("CARGO_BIN_EXE_psyrag"))
		.arg("mcp")
		.current_dir(&repo)
		.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null())
		.spawn().unwrap();
	let mut sin = child.stdin.take().unwrap();
	let mut sout = BufReader::new(child.stdout.take().unwrap());

	let send = |sin: &mut std::process::ChildStdin, s: &str| {
		sin.write_all(s.as_bytes()).unwrap();
		sin.write_all(b"\n").unwrap();
		sin.flush().unwrap();
	};
	let recv = |sout: &mut BufReader<std::process::ChildStdout>| -> serde_json::Value {
		let mut line = String::new();
		sout.read_line(&mut line).unwrap();
		serde_json::from_str(&line).unwrap()
	};

	send(&mut sin, r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#);
	let init = recv(&mut sout);
	assert_eq!(init["result"]["serverInfo"]["name"], "psyrag");

	send(&mut sin, r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#);
	let tools = recv(&mut sout);
	assert_eq!(tools["result"]["tools"][0]["name"], "psyrag_recall");

	send(&mut sin, r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"psyrag_recall","arguments":{"query":"anything"}}}"#);
	let call = recv(&mut sout);
	assert_eq!(call["result"]["content"][0]["type"], "text");

	drop(sin); // EOF => clean shutdown
	let _ = child.wait();
	std::fs::remove_dir_all(&repo).ok();
}
