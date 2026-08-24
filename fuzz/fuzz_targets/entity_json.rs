#![no_main]

use libfuzzer_sys::fuzz_target;
use psyrag_graph::entity::ingest_entities_mem;
use psyrag_graph::TemporalGraph;

fuzz_target!(|data: &[u8]| {
    // Arbitrary text into entity JSON ingest: errors ok, panics not.
    let s = String::from_utf8_lossy(data);
    let mut g = TemporalGraph::new();
    let _ = ingest_entities_mem(&mut g, &s, 0, false);
});
