use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Build a short-path temp repo directly under /tmp (not `std::env::temp_dir()`,
/// which on macOS resolves to a long /var/folders/.../T/ path). `run_mcp` binds
/// a unix-domain socket at `.psyrag/mcp.sock`, and macOS caps socket paths at
/// ~104 bytes — a long temp_dir() base would overflow that and break the bind.
fn temp_repo() -> std::path::PathBuf {
	static N: AtomicU64 = AtomicU64::new(0);
	let uniq = N.fetch_add(1, Ordering::Relaxed);
	let d = std::path::PathBuf::from("/tmp")
		.join(format!("psyrag-learn-{}-{}", std::process::id(), uniq));
	std::fs::create_dir_all(d.join(".git")).unwrap();
	d
}

/// Parse a recall text block's ranked lines (format `  <activation>  <path>`,
/// two leading spaces then a float then the path) into the 0-based rank of
/// `path`, or `None` if it doesn't appear.
fn rank_of(block: &str, path: &str) -> Option<usize> {
	block
		.lines()
		.filter(|l| l.trim_start().starts_with(|c: char| c.is_ascii_digit()))
		.position(|l| l.trim_end().ends_with(path))
}

#[test]
fn recall_reorders_around_used_files() {
	let repo = temp_repo();
	let sock = repo.join(".psyrag/mcp.sock");

	let mut child = Command::new(env!("CARGO_BIN_EXE_psyrag"))
		.arg("mcp")
		.current_dir(&repo)
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.stderr(Stdio::null())
		.spawn()
		.unwrap();
	let mut sin = child.stdin.take().unwrap();
	let mut sout = BufReader::new(child.stdout.take().unwrap());

	let line = |s: &mut BufReader<std::process::ChildStdout>| {
		let mut l = String::new();
		s.read_line(&mut l).unwrap();
		l
	};
	let rpc = |sin: &mut std::process::ChildStdin, s: &str| {
		sin.write_all(s.as_bytes()).unwrap();
		sin.write_all(b"\n").unwrap();
		sin.flush().unwrap();
	};
	// Send an rpc call and return the recall tool's `text` content (with real
	// newlines, since serde_json un-escapes it) rather than the raw response
	// line (whose newlines are JSON-escaped and would defeat `.lines()`).
	let recall_text = |sin: &mut std::process::ChildStdin,
	                    sout: &mut BufReader<std::process::ChildStdout>,
	                    id: u64| {
		let req = format!(
			r#"{{"jsonrpc":"2.0","id":{id},"method":"tools/call","params":{{"name":"psyrag_recall","arguments":{{"query":"auth db util"}}}}}}"#
		);
		rpc(sin, &req);
		let resp = line(sout);
		let v: serde_json::Value = serde_json::from_str(&resp)
			.unwrap_or_else(|e| panic!("bad json response: {e}: {resp}"));
		v["result"]["content"][0]["text"]
			.as_str()
			.unwrap_or_else(|| panic!("no text in response: {resp}"))
			.to_string()
	};

	// wait for the socket to exist (listener thread bind)
	for _ in 0..50 {
		if sock.exists() {
			break;
		}
		std::thread::sleep(Duration::from_millis(20));
	}
	assert!(sock.exists(), "mcp socket never appeared at {}", sock.display());

	let feed = |ev: &str| {
		let mut s = UnixStream::connect(&sock).unwrap();
		s.write_all(ev.as_bytes()).unwrap();
		s.write_all(b"\n").unwrap();
		drop(s);
		std::thread::sleep(Duration::from_millis(30)); // let the listener apply
	};

	// Build a small co-touch graph seeded on the query tokens "auth", "db",
	// "util": touch each in turn so recall on "auth db util" has candidates
	// and the three files become co-touched.
	feed(r#"{"kind":"used","path":"auth.rs"}"#);
	feed(r#"{"kind":"used","path":"db.rs"}"#);
	feed(r#"{"kind":"used","path":"util.rs"}"#);

	let before = recall_text(&mut sin, &mut sout, 1);

	// Mark db.rs useful many times after recall — credit lands on the trace
	// that just surfaced it (held in the server's TraceRing) and reinforces
	// the edges that carried activation to it. It should climb.
	for _ in 0..20 {
		let _ = recall_text(&mut sin, &mut sout, 2);
		feed(r#"{"kind":"used","path":"db.rs"}"#);
	}

	let after = recall_text(&mut sin, &mut sout, 3);

	drop(sin);
	let _ = child.wait();
	std::fs::remove_dir_all(&repo).ok();

	let before_rank = rank_of(&before, "db.rs");
	let after_rank = rank_of(&after, "db.rs");

	eprintln!("=== BEFORE ===\n{before}=== AFTER ===\n{after}before_rank={before_rank:?} after_rank={after_rank:?}");

	assert!(
		after_rank.is_some(),
		"db.rs must surface in the final recall after learning:\nbefore:\n{before}\nafter:\n{after}"
	);
	// Strict `<` (not `<=`) is required here: equality would let a no-op credit
	// regression pass silently, since db.rs would keep the same rank in `before`
	// and `after`. `unwrap_or(usize::MAX)` still counts absent-in-before ->
	// present-in-after as a legitimate improvement.
	assert!(
		after_rank.unwrap() < before_rank.unwrap_or(usize::MAX),
		"db.rs must strictly climb (or newly appear) after learning:\n\
		 before rank = {before_rank:?}\nafter rank = {after_rank:?}\n\
		 before:\n{before}\nafter:\n{after}"
	);
}
