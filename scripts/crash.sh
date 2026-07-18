#!/usr/bin/env bash
# kill -9 crash-recovery suite.
#
# Each round: start the server, stream rapid ingests recording every ACKED
# (2xx) name, SIGKILL the server at a random moment mid-stream, then verify:
#   1. the WAL replays healthy (`psyrag verify` — a torn tail is fine,
#      mid-file corruption is not),
#   2. a restarted server holds EVERY acked name (the fsync contract:
#      a 2xx means it survived the crash),
#   3. the restarted server accepts new writes.
# The WAL persists across rounds, so recovery is exercised on a log that has
# itself survived previous crashes.
#
# Usage: scripts/crash.sh [ROUNDS] [BIN] [PORT]
set -uo pipefail
cd "$(dirname "$0")/.."
ROUNDS="${1:-5}"
BIN="${2:-./target/release/psyrag}"
PORT="${3:-8811}"
URL="http://127.0.0.1:${PORT}"
WORK="$(mktemp -d)"
WAL="$WORK/crash.wal"
ACKED="$WORK/acked.txt"
: > "$ACKED"
FAIL=0

[ -x "$BIN" ] || { echo "binary not found: $BIN (run scripts/install.sh)"; exit 2; }

for round in $(seq 1 "$ROUNDS"); do
  "$BIN" --wal "$WAL" serve --addr "127.0.0.1:${PORT}" >"$WORK/serve-$round.log" 2>&1 &
  SRV=$!
  sleep 1
  # killer: SIGKILL at a random point 200-1400ms into the write stream
  ( DELAY=$(python3 -c "import random;print(random.uniform(0.2,1.4))"); sleep "$DELAY"; kill -9 $SRV 2>/dev/null ) &
  KILLER=$!
  # writer: rapid unique ingests; record every acked name
  for i in $(seq 1 400); do
    NAME="r${round}n${i}"
    CODE=$(curl -s -o /dev/null -w "%{http_code}" -m 2 -X POST "$URL/ingest" \
      -H 'Content-Type: application/json' \
      -d "{\"json\":\"[{\\\"name\\\":\\\"$NAME\\\",\\\"type\\\":\\\"t\\\",\\\"edges\\\":[{\\\"dst\\\":\\\"hub\\\",\\\"kind\\\":\\\"R\\\"}]}]\",\"ts\":$((round*100000+i))}" 2>/dev/null)
    if [ "$CODE" = "200" ]; then echo "$NAME" >> "$ACKED"; else break; fi
  done
  wait "$KILLER" 2>/dev/null
  kill -9 $SRV 2>/dev/null   # in case the stream outlived the killer window
  wait $SRV 2>/dev/null

  # 1. structural recovery
  if ! "$BIN" --wal "$WAL" verify >"$WORK/verify-$round.json" 2>&1; then
    echo "✗ round $round: WAL failed verification after SIGKILL"
    cat "$WORK/verify-$round.json"
    FAIL=1
    break
  fi
  # 2. every acked write survived
  "$BIN" --wal "$WAL" serve --addr "127.0.0.1:${PORT}" >"$WORK/check-$round.log" 2>&1 &
  CHK=$!
  sleep 1
  MISSING=$(python3 - "$ACKED" "$URL" <<'PYEOF'
import json, sys, urllib.request
acked = [l.strip() for l in open(sys.argv[1]) if l.strip()]
url = sys.argv[2]
missing = []
# batch the membership check through /match (exact names are substrings of themselves)
for i in range(0, len(acked), 50):
    chunk = acked[i:i+50]
    req = urllib.request.Request(url + "/match",
        data=json.dumps({"tokens": chunk, "limit": 100000}).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=5) as r:
        found = set(json.loads(r.read())["nodes"])
    missing += [n for n in chunk if n not in found]
print(len(missing))
for n in missing[:5]:
    print("missing:", n, file=sys.stderr)
PYEOF
)
  # 3. the recovered server accepts new writes
  POST=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/ingest" \
    -H 'Content-Type: application/json' \
    -d "{\"json\":\"[{\\\"name\\\":\\\"post-crash-$round\\\",\\\"type\\\":\\\"t\\\"}]\",\"ts\":$((round*100000+99999))}")
  kill $CHK 2>/dev/null; wait $CHK 2>/dev/null
  ACKED_N=$(wc -l < "$ACKED" | tr -d ' ')
  if [ "$MISSING" != "0" ]; then
    echo "✗ round $round: $MISSING of $ACKED_N acked writes LOST after SIGKILL (fsync contract broken)"
    FAIL=1
    break
  fi
  if [ "$POST" != "200" ]; then
    echo "✗ round $round: recovered server refused new writes ($POST)"
    FAIL=1
    break
  fi
  echo "✓ round $round: $ACKED_N acked writes all present after SIGKILL; recovery accepts writes"
done

rm -rf "$WORK"
if [ "$FAIL" -eq 0 ]; then
  echo "==== crash suite: $ROUNDS rounds passed ===="
else
  echo "==== crash suite FAILED ===="
fi
exit "$FAIL"
