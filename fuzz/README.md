# PsyRag coverage-guided fuzzing

This `fuzz/` crate is **excluded from the workspace** so normal
`cargo build --workspace` / `cargo test --workspace` / `cargo deny` stays
zero-dep. It uses [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz)
(libFuzzer) on nightly.

## Targets

| Target | Input | Entry points |
|--------|--------|--------------|
| `wal_replay` | arbitrary bytes as a WAL file | `verify_wal`, `PersistentGraph::open` |
| `entity_json` | arbitrary UTF-8 text | `ingest_entities_mem` |

Invariant (same as `fuzz_lite`): hostile bytes may produce **errors**, never
**panics** or silent corruption.

## Local run

```bash
cargo install cargo-fuzz
rustup install nightly

cd fuzz
cargo +nightly fuzz run wal_replay -- -max_total_time=600
cargo +nightly fuzz run entity_json -- -max_total_time=600
```

Reproduce a crash artifact:

```bash
cargo +nightly fuzz run wal_replay artifacts/wal_replay/crash-<id>
```

## CI

See `.github/workflows/fuzz.yml` (weekly schedule + `workflow_dispatch`).
