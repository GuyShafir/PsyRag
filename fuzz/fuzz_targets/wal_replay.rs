#![no_main]

use libfuzzer_sys::fuzz_target;
use psyrag_graph::persist::{verify_wal, PersistentGraph};
use std::io::Write;

fuzz_target!(|data: &[u8]| {
    // Hostile bytes as a WAL file: may error, must not panic.
    let dir = std::env::temp_dir().join(format!(
        "psyrag_fuzz_wal_{}_{}",
        std::process::id(),
        data.len()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("wal.bin");
    if let Ok(mut f) = std::fs::File::create(&path) {
        let _ = f.write_all(data);
        let _ = f.flush();
        let _ = verify_wal(&path);
        let _ = PersistentGraph::open(&path);
    }
    let _ = std::fs::remove_dir_all(&dir);
});
