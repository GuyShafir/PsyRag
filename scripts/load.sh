#!/usr/bin/env bash
# Load/soak harness: drive a mixed workload, then assert SLOs from the
# server's OWN Prometheus histograms (the numbers users would scrape).
#
#   - zero 5xx under load
#   - retrieve p95 under SLO_P95_MS (default 250ms — generous for shared CI;
#     tighten locally with SLO_P95_MS=25 scripts/load.sh)
#   - approx_bytes stable across a read-only soak tail (no unbounded growth)
#
# Usage: scripts/load.sh [SECONDS] [THREADS] [BIN] [PORT]
set -uo pipefail
cd "$(dirname "$0")/.."
SECONDS_RUN="${1:-15}"
THREADS="${2:-8}"
BIN="${3:-./target/release/psyrag}"
PORT="${4:-8821}"
URL="http://127.0.0.1:${PORT}"
SLO_P95_MS="${SLO_P95_MS:-250}"
WORK="$(mktemp -d)"
FAIL=0

[ -x "$BIN" ] || { echo "binary not found: $BIN (run scripts/install.sh)"; exit 2; }

"$BIN" --wal "$WORK/load.wal" serve --addr "127.0.0.1:${PORT}" --workers 8 >"$WORK/serve.log" 2>&1 &
SRV=$!
sleep 1

echo "== load: ${SECONDS_RUN}s x ${THREADS} threads =="
SUMMARY=$(python3 scripts/load.py "$URL" "$SECONDS_RUN" "$THREADS")
echo "$SUMMARY"

# read-only soak tail: memory estimate must not grow without writes
B1=$(curl -s "$URL/dbs" | python3 -c "import sys,json;print(json.load(sys.stdin)['dbs'][0]['approx_bytes'])")
for _ in $(seq 1 200); do
  curl -s -o /dev/null -X POST "$URL/retrieve" -H 'Content-Type: application/json' \
    -d '{"seeds":["svc/n1"],"adapt":false}'
done
B2=$(curl -s "$URL/dbs" | python3 -c "import sys,json;print(json.load(sys.stdin)['dbs'][0]['approx_bytes'])")

METRICS=$(curl -s "$URL/metrics")
kill $SRV 2>/dev/null

echo "$SUMMARY" > "$WORK/summary.json"
echo "$METRICS" > "$WORK/metrics.txt"
VERDICT=$(python3 - "$SLO_P95_MS" "$B1" "$B2" "$WORK/summary.json" "$WORK/metrics.txt" <<'VEOF'
import json, sys
slo_ms, b1, b2 = float(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
summary = json.load(open(sys.argv[4]))
metrics = open(sys.argv[5]).read().splitlines()

fails = []
# 1. zero 5xx anywhere (load driver counts + network failures)
for code, n in summary["by_status"].items():
    if code.startswith("5") or code == "0":
        fails.append(f"{n} responses with status {code} under load")
# 2. retrieve p95 from the server's histogram
buckets = []  # (le, cumulative)
count = 0
for m in metrics:
    if m.startswith('psyrag_request_duration_seconds_bucket{route="retrieve"'):
        le = m.split('le="')[1].split('"')[0]
        v = int(m.rsplit(" ", 1)[1])
        buckets.append((float("inf") if le == "+Inf" else float(le), v))
    if m.startswith('psyrag_request_duration_seconds_count{route="retrieve"'):
        count = int(m.rsplit(" ", 1)[1])
p95 = None
if count:
    target = 0.95 * count
    for le, cum in buckets:
        if cum >= target:
            p95 = le
            break
if p95 is None:
    fails.append("no retrieve latency data collected")
elif p95 * 1000 > slo_ms:
    fails.append(f"retrieve p95 bucket {p95*1000:.0f}ms exceeds SLO {slo_ms:.0f}ms")
# 3. read-only soak must not grow memory
if b2 > b1:
    fails.append(f"approx_bytes grew during read-only soak: {b1} -> {b2}")

print(json.dumps({
    "retrieve_p95_bucket_ms": None if p95 is None else round(p95 * 1000, 1),
    "retrieve_count": count,
    "soak_bytes": [b1, b2],
    "fails": fails,
}))
VEOF
)
echo "$VERDICT"
python3 -c "import json,sys; sys.exit(1 if json.loads('''$VERDICT''')['fails'] else 0)" || FAIL=1

rm -rf "$WORK"
if [ "$FAIL" -eq 0 ]; then echo "==== load/soak passed ===="; else echo "==== load/soak FAILED ===="; fi
exit "$FAIL"
