pub mod paths;
pub mod jsonrpc;
pub mod recall;
pub mod graph_ops;
pub mod events;
pub mod maintenance;
pub mod protocol;

use crate::args::Args;
use crate::engine::now_ms;
use crate::mcp::events::{apply, send, Event};
use crate::mcp::graph_ops::{cold_start_from_git, TouchWindow};
use crate::mcp::maintenance::sleep_if_stale;
use crate::mcp::protocol::dispatch;
use crate::mcp::recall::TraceRing;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Shared session state behind one lock: stdio and the socket both mutate it.
struct Shared {
	engine: crate::engine::Engine,
	ring: TraceRing,
	window: TouchWindow,
}

// A poisoned lock means a prior holder panicked mid-op; the engine state is
// still usable (the WAL is the source of truth on restart), so recover the
// guard rather than cascade-panicking every future lock and wedging the server.
fn lock_shared(m: &std::sync::Mutex<Shared>) -> std::sync::MutexGuard<'_, Shared> {
	m.lock().unwrap_or_else(|e| e.into_inner())
}

/// `psyrag mcp` — embed the engine, serve MCP on stdio, ingest hook events
/// on a unix socket. Holds the WAL flock for the session.
pub fn run_mcp(a: &Args) -> Result<(), String> {
	let start = std::env::current_dir().map_err(|e| e.to_string())?;
	let paths = paths::resolve(&start)?;
	// open_engine reads --wal; point it at our resolved WAL.
	let a = a.clone()
		.with_override("wal", paths.wal.to_string_lossy().as_ref())
		.with_override("sidecar", paths.sidecar.to_string_lossy().as_ref());
	let mut engine = crate::open_engine(&a)
		.map_err(|e| format!("open memory at {}: {e}", paths.wal.display()))?;

	// cold start + sleep-if-stale before serving.
	if engine.pg.graph().node_count() == 0 {
		let _ = cold_start_from_git(&mut engine, paths.dir.parent().unwrap_or(&paths.dir));
	}
	sleep_if_stale(&mut engine, &paths.last_sleep, now_ms());

	let shared = Arc::new(Mutex::new(Shared {
		engine, ring: TraceRing::new(8), window: TouchWindow::new(5),
	}));

	// socket listener thread
	let _ = std::fs::remove_file(&paths.sock); // clear a stale socket
	// macOS/BSD sockaddr_un caps the socket path near 104 bytes; a very long
	// repo path would otherwise fail bind with an opaque errno.
	if paths.sock.as_os_str().len() > 100 {
		return Err(format!(
			"socket path too long for this platform ({} bytes): {}. \
			 Move the repo to a shorter path.",
			paths.sock.as_os_str().len(), paths.sock.display()
		));
	}
	let listener = UnixListener::bind(&paths.sock).map_err(|e| format!("bind socket: {e}"))?;
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		let _ = std::fs::set_permissions(&paths.sock, std::fs::Permissions::from_mode(0o600));
	}
	let sock_shared = Arc::clone(&shared);
	std::thread::spawn(move || {
		for stream in listener.incoming() {
			let Ok(stream) = stream else { continue };
			let reader = BufReader::new(stream);
			for line in reader.lines().map_while(Result::ok) {
				let Ok(ev) = serde_json::from_str::<Event>(&line) else { continue };
				let mut g = lock_shared(&sock_shared);
				let Shared { engine, window, ring } = &mut *g;
				let _ = apply(engine, window, ring, &ev);
			}
		}
	});

	// stdio JSON-RPC loop (main thread)
	let stdin = std::io::stdin();
	let mut stdout = std::io::stdout();
	for line in stdin.lock().lines().map_while(Result::ok) {
		if line.trim().is_empty() { continue; }
		let resp = match jsonrpc::parse(&line) {
			Ok(req) => {
				let mut g = lock_shared(&shared);
				let Shared { engine, ring, .. } = &mut *g;
				dispatch(engine, ring, &req)
			}
			Err(err) => Some(err),
		};
		if let Some(resp) = resp {
			writeln!(stdout, "{}", resp.to_line()).map_err(|e| e.to_string())?;
			stdout.flush().map_err(|e| e.to_string())?;
		}
	}
	// stdin EOF => clean shutdown
	let mut g = lock_shared(&shared);
	g.engine.save_sidecar()?;
	let _ = std::fs::remove_file(&paths.sock);
	Ok(())
}

/// `psyrag mcp-send` — hook shim. Reads a Claude Code hook JSON payload on
/// stdin, maps it to an Event, sends it. Exits 0 unconditionally.
pub fn run_mcp_send(_a: &Args) -> Result<(), String> {
	let start = match std::env::current_dir() { Ok(d) => d, Err(_) => return Ok(()) };
	let paths = match paths::resolve(&start) { Ok(p) => p, Err(_) => return Ok(()) };
	let mut input = String::new();
	use std::io::Read as _;
	let _ = std::io::stdin().read_to_string(&mut input);
	let v: serde_json::Value = serde_json::from_str(&input).unwrap_or(serde_json::Value::Null);
	// PreCompact hook => Compact; PostToolUse(Read|Edit) => Used{file_path}.
	let hook = v["hook_event_name"].as_str().unwrap_or("");
	let ev = if hook == "PreCompact" {
		Event::Compact
	} else {
		let path = v["tool_input"]["file_path"].as_str()
			.or_else(|| v["tool_input"]["path"].as_str())
			.unwrap_or("");
		if path.is_empty() { return Ok(()); }
		// store repo-relative to match ingest namespace
		let root = paths.dir.parent().unwrap_or(&paths.dir);
		let rel = repo_relative(root, path);
		Event::Used { path: rel }
	};
	send(&paths.sock, &ev);
	Ok(())
}

/// Derive the repo-relative node name for a touched file. Canonicalizes the
/// path (best-effort — a just-deleted file falls back to the raw string) so it
/// strips cleanly against the canonicalized repo root even when the root is
/// reached through a symlink (e.g. macOS /tmp -> /private/tmp). Without this,
/// an absolute path leaks into the node namespace and credit never lands.
fn repo_relative(root: &Path, raw: &str) -> String {
	let abs = std::fs::canonicalize(raw).unwrap_or_else(|_| std::path::PathBuf::from(raw));
	abs.strip_prefix(root)
		.map(|p| p.to_string_lossy().into_owned())
		.unwrap_or_else(|_| abs.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::atomic::{AtomicU64, Ordering};

	#[test]
	fn repo_relative_strips_canonical_root() {
		static N: AtomicU64 = AtomicU64::new(0);
		let n = N.fetch_add(1, Ordering::Relaxed);
		let base = std::path::PathBuf::from("/tmp").join(format!("psyrag-rr-{}-{}", std::process::id(), n));
		let root = base.join("repo");
		std::fs::create_dir_all(root.join("src")).unwrap();
		std::fs::write(root.join("src/a.rs"), b"x").unwrap();
		let croot = std::fs::canonicalize(&root).unwrap();
		// raw absolute path to the file (as a hook would pass tool_input.file_path)
		let raw = root.join("src/a.rs");
		let rel = repo_relative(&croot, raw.to_str().unwrap());
		assert_eq!(rel, "src/a.rs");
		std::fs::remove_dir_all(&base).ok();
	}
}
