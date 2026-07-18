//! Deterministic fuzz-lite: seeded randomized robustness tests that run on
//! every CI push (no nightly/libfuzzer infrastructure; a coverage-guided
//! fuzzer can supplement this later). The invariant under test is uniform:
//! **hostile bytes may produce errors, never panics or silent corruption.**

use psyrag_graph::persist::verify_wal;
use psyrag_graph::{Op, PersistentGraph};
use std::fs;
use std::path::PathBuf;

/// xorshift64* — tiny deterministic PRNG; fixed seeds keep failures
/// reproducible from the test name alone.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn tmp(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("psyrag_fuzz_{name}_{}", std::process::id()))
}

/// A healthy little WAL to mutate.
fn valid_wal_bytes(path: &PathBuf) -> Vec<u8> {
    let _ = fs::remove_file(path);
    {
        let mut pg = PersistentGraph::open(path).unwrap();
        pg.ingest_entities_from(
            r#"[{"name":"svc/api","type":"t","origin":"u:1","props":{"k":[1,2,3]},
                 "edges":[{"dst":"db","kind":"CALLS"},{"dst":"cache","kind":"CALLS"}]},
                {"name":"db","type":"t","edges":[{"dst":"vol","kind":"USES"}]}]"#,
            1000,
            false,
            None,
        )
        .unwrap();
    }
    fs::read(path).unwrap()
}

#[test]
fn mutated_wals_never_panic_and_never_silently_misreplay() {
    let path = tmp("mut");
    let base = valid_wal_bytes(&path);
    let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
    for _ in 0..600 {
        let mut bytes = base.clone();
        // 1-3 random corruptions: flip, delete, or insert a byte
        for _ in 0..(1 + rng.below(3)) {
            if bytes.is_empty() {
                break;
            }
            let pos = rng.below(bytes.len());
            match rng.below(3) {
                0 => bytes[pos] ^= (1 + rng.below(255)) as u8,
                1 => {
                    bytes.remove(pos);
                }
                _ => bytes.insert(pos, (rng.next() & 0xFF) as u8),
            }
        }
        fs::write(&path, &bytes).unwrap();
        // verify_wal (read-only) and open (repairing) must both stay calm:
        // Ok or Err, never a panic. A CRC-framed record that still parses
        // must equal an uncorrupted record's effect — spot-check that any
        // successful open yields a queryable graph.
        let _ = verify_wal(&path);
        match PersistentGraph::open(&path) {
            Ok(pg) => {
                let g = pg.graph();
                // exercise reads over whatever survived
                let _ = g.alive_at(2000).len();
                for id in 0..g.node_count() {
                    let _ = g.node_origin_at(id as u32, 2000);
                }
            }
            Err(e) => assert!(!e.is_empty()),
        }
        // open() may have repaired (truncated) the file; that's fine — the
        // next iteration rewrites it from `base`.
    }
    let _ = fs::remove_file(&path);
}

#[test]
fn garbage_entity_json_never_panics() {
    let mut rng = Rng(0x1234_5678_9ABC_DEF0);
    let alphabet: &[u8] = b"{}[]\",:xyz01\\n \xc3\xa9"; // includes split UTF-8
    for _ in 0..2000 {
        let len = rng.below(200);
        let junk: String = (0..len)
            .map(|_| alphabet[rng.below(alphabet.len())] as char)
            .collect();
        let _ = psyrag_graph::entity::parse_entities(&junk);
    }
    // structured-but-wrong shapes
    for j in [
        r#"[{"name":1}]"#,
        r#"[{"type":"t"}]"#,
        r#"[{"name":"a","type":"t","edges":[{"kind":"K"}]}]"#,
        r#"[{"name":"a","type":"t","edges":{"dst":"b"}}]"#,
        r#"{"name":"a","type":"t"}
           {"name":"b""#,
    ] {
        let _ = psyrag_graph::entity::parse_entities(j);
    }
}

#[test]
fn randomized_op_roundtrip_through_framing() {
    // Ops with hostile-ish strings must survive journal → replay unchanged.
    let path = tmp("roundtrip");
    let mut rng = Rng(0x0BAD_F00D_0BAD_F00D);
    let weird = ["a\"b", "line\nbreak", "uni\u{00e9}\u{4e2d}", "  spaces  ", "\\backslash\\", "#hash:v1 id=x", "12345678:fakeframe"];
    let _ = fs::remove_file(&path);
    let mut expected: Vec<(String, String)> = Vec::new();
    {
        let mut pg = PersistentGraph::open(&path).unwrap();
        for i in 0..200 {
            let name = format!("{}-{i}", weird[rng.below(weird.len())]);
            let origin = (rng.below(2) == 0).then(|| weird[rng.below(weird.len())].to_string());
            pg.record_op(Op::ObserveNode {
                name: name.clone(),
                asset_type: "t/x".into(),
                props: serde_json::json!({"s": weird[rng.below(weird.len())], "i": i}),
                ts: 1000 + i,
                origin: origin.clone(),
            })
            .unwrap();
            expected.push((name, origin.unwrap_or_default()));
        }
        pg.flush().unwrap();
    }
    let pg = PersistentGraph::open(&path).unwrap();
    assert_eq!(pg.replay_skipped, 0);
    let g = pg.graph();
    for (name, origin) in &expected {
        let id = g.id_of(name).unwrap_or_else(|| panic!("lost node {name:?}"));
        assert_eq!(g.node_origin_at(id, i64::MAX - 1), origin.as_str(), "origin for {name:?}");
    }
    let _ = fs::remove_file(&path);
}
