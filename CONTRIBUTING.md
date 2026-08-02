# Contributing to PsyRag

Contributions are wanted — including first ones. PsyRag's first outside
contribution (`psyrag mcp`, v0.6.0) came from someone who had never touched
the codebase before. Issues tagged
[`good first issue`](https://github.com/GuyShafir/PsyRag/labels/good%20first%20issue)
are scoped to be doable without knowing the whole system; open a draft PR
early if you want feedback before polishing.

## Dev setup

Any recent stable Rust. No other toolchain, no services, no network at
build time:

```bash
rustup default stable
scripts/install.sh          # cargo build --release -p psyrag, prints the binary path
```

The dependency tree is `serde`, `serde_json`, `tiny_http`. That's the whole
setup.

## The zero-dependency rule

Runtime dependencies are deliberately frozen at those three crates. This is
a feature: the binary builds fast and offline anywhere, the attack surface
is auditable, and `cargo deny` stays quiet.

**A PR that adds a dependency is answering "no" by default.** If you think
a dep is justified, open an issue first and make the case — some things
(managed backends, coverage-guided fuzzing) are expected to take deps, but
always behind a seam or a feature gate so the core build is untouched.
"This crate would save 40 lines" is not a case.

## Running the CI gates locally

CI runs all of these on every push. Run them before opening a PR — they are
the review:

```bash
cargo fmt --all --check                      # formatting
cargo clippy --workspace -- -D warnings      # lints, warnings are errors
cargo build --release --workspace            # the smoke/crash suites need this
cargo test --workspace                       # unit + golden suite + fixture zoo + fuzz-lite
bash scripts/smoke.sh                        # end-to-end assertions against a live server
bash scripts/crash.sh 5                      # kill -9 durability suite
bash scripts/standby.sh                      # replication + failover drill
bash scripts/load.sh 15 8                    # load/soak with SLO assertions
```

Supply chain (advisories, licenses, bans — config in `deny.toml`):

```bash
cargo install cargo-deny && cargo deny check
```

For a quick iteration loop, `fmt` + `clippy` + `cargo test` catches most
problems; run the shell suites before pushing.

## Code style

`rustfmt` with default settings is enforced (`cargo fmt --all --check`
fails the build). Run `cargo fmt --all` before pushing and don't
hand-format — a tabs-indented PR has failed CI on exactly this before.
Clippy runs with `-D warnings`; fix the lint rather than `#[allow]`-ing it
unless there's a comment-worthy reason.

## Tests

New behavior needs a test. Where it goes:

- **Unit / property tests** — next to the code, `cargo test --workspace`.
- **Durability or crash behavior** — extend `scripts/crash.sh` or the
  format fixture zoo, not just a happy-path unit test.
- **New HTTP surface or CLI behavior** — add an assertion block to
  `scripts/smoke.sh`. The pattern is boring on purpose: `curl`, extract a
  field, `ok`/`no`. Every claim in the README maps to an assertion
  somewhere in CI; keep it that way.
- **Learning behavior** — the golden suite pins retrieval quality and
  determinism. If your change legitimately moves a golden number, say why
  in the PR.

## PRs

- **Small and focused.** One behavior per PR. Refactors ride separately.
- **Descriptive messages.** Plain prose, present tense, no prefixes, no
  ticket numbers: "Cap /graph response with limit+offset params", not
  "feat(serve): ...".
- **No AI-attribution trailers.** No `Co-Authored-By: Claude`, no
  "Generated with" footers, in commits or PR bodies. The contributor list
  stays authors only — tools don't get credit lines.
- Update the docs that state the behavior you changed (`docs/reference.md`
  for API/CLI surface, `docs/runbook.md` for ops-visible behavior), and add
  a line to `CHANGELOG.md` under the unreleased heading if there is one.

If CI is green and the diff does one thing, review is fast. If you're
unsure whether an approach fits, open the issue conversation before writing
code — cheaper for everyone.
