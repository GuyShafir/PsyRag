#!/usr/bin/env bash
# Build the release binary and (optionally) symlink it onto PATH.
set -euo pipefail
cd "$(dirname "$0")/.."
echo ">> building release workspace (deps: serde/serde_json/tiny_http only)…"
cargo build --release -p psyrag
BIN="$(pwd)/target/release/psyrag"
echo ">> built: $BIN"
if [ "${1:-}" = "--link" ]; then
  DEST="${2:-/usr/local/bin/psyrag}"
  ln -sf "$BIN" "$DEST" && echo ">> linked -> $DEST"
fi
"$BIN" --help >/dev/null 2>&1 || true
echo ">> done. try:  $BIN --wal /tmp/psyrag.wal serve --addr 127.0.0.1:8080"
