#!/usr/bin/env bash
# WAL-shipping warm-standby test: replication, lag, read-only enforcement,
# learned-weight shipping, primary compaction resync, failover promotion.
# Exits non-zero on any failed assertion.
# Usage: scripts/standby.sh [BIN] [PORT]
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${1:-./target/release/psyrag}"
PORT="${2:-8871}"
SPORT=$((PORT+1))
PURL="http://127.0.0.1:${PORT}"
SURL="http://127.0.0.1:${SPORT}"
WORK="$(mktemp -d)"
PASS=0; FAIL=0
ok(){ echo "  ✓ $1"; PASS=$((PASS+1)); }
no(){ echo "  ✗ $1"; FAIL=$((FAIL+1)); }
j(){ python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
AT='Authorization: Bearer root'

[ -x "$BIN" ] || { echo "binary not found: $BIN"; exit 2; }

echo "== 1. primary up + seeded =="
"$BIN" --wal "$WORK/primary.wal" serve --addr "127.0.0.1:${PORT}" --token root >"$WORK/primary.log" 2>&1 &
PSRV=$!; sleep 1.2
curl -s -X POST -H "$AT" "$PURL/ingest" -d '{"json":"[{\"name\":\"api\",\"type\":\"svc\",\"edges\":[{\"dst\":\"db\",\"kind\":\"CALLS\"},{\"dst\":\"cache\",\"kind\":\"CALLS\"}]},{\"name\":\"db\",\"type\":\"store\"},{\"name\":\"cache\",\"type\":\"store\"}]","ts":1000,"origin":"seed/"}' >/dev/null
# learn something so the sidecar is non-trivial
curl -s -X POST -H "$AT" "$PURL/feedback" -d '{"seeds":["api"],"used":["db"],"ts":2000}' >/dev/null
N=$(curl -s -H "$AT" "$PURL/stats" | j 'd["nodes"]')
[ "$N" = "3" ] && ok "primary seeded (3 nodes)" || no "primary seed failed ($N)"

echo "== 2. standby catches up =="
"$BIN" standby --primary "$PURL" --primary-token root --wal "$WORK/standby.wal" \
  --addr "127.0.0.1:${SPORT}" --poll-ms 300 >"$WORK/standby.log" 2>&1 &
SSRV=$!; sleep 2.5
SN=$(curl -s "$SURL/stats" | j 'd["nodes"]')
SE=$(curl -s "$SURL/stats" | j 'd["edges_total"]')
[ "$SN" = "3" ] && ok "standby replicated nodes" || no "standby nodes=$SN"
[ "$SE" = "2" ] && ok "standby replicated edges" || no "standby edges=$SE"
TOP=$(curl -s -X POST "$SURL/retrieve" -d '{"seeds":["api"],"adapt":false}' | j 'd["top"][0]["node"]')
[ "$TOP" = "api" ] && ok "standby serves retrieval" || no "standby retrieve broken ($TOP)"

echo "== 3. standby is read-only =="
C=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$SURL/ingest" -d '{"json":"[{\"name\":\"rogue\",\"type\":\"x\"}]"}')
[ "$C" = "503" ] && ok "writes to standby -> 503" || no "standby accepted a write ($C)"

echo "== 4. continuous replication =="
curl -s -X POST -H "$AT" "$PURL/ingest" -d '{"json":"[{\"name\":\"queue\",\"type\":\"svc\",\"edges\":[{\"dst\":\"db\",\"kind\":\"WRITES\"}]}]","ts":3000,"origin":"seed/"}' >/dev/null
sleep 1.5
SN=$(curl -s "$SURL/stats" | j 'd["nodes"]')
[ "$SN" = "4" ] && ok "new primary write reached standby" || no "replication lagging (nodes=$SN)"

echo "== 5. learned weights ship (sidecar) =="
for i in 1 2 3 4 5; do
  curl -s -X POST -H "$AT" "$PURL/feedback" -d "{\"seeds\":[\"api\"],\"used\":[\"db\"],\"ts\":$((4000+i))}" >/dev/null
done
sleep 6.5  # > sidecar_every on the follower
WMAX=$(curl -s "$SURL/stats" | python3 -c 'import sys,json;print(1 if json.load(sys.stdin)["weight_max"]>0.5 else 0)')
[ "$WMAX" = "1" ] && ok "reinforced weight visible on standby" || no "standby weights stayed cold"

echo "== 6. primary compaction forces clean resync =="
curl -s -X POST -H "$AT" "$PURL/checkpoint" -d '{"archive":false}' >/dev/null
curl -s -X POST -H "$AT" "$PURL/ingest" -d '{"json":"[{\"name\":\"metrics\",\"type\":\"svc\"}]","ts":6000,"origin":"seed/"}' >/dev/null
sleep 2.5
SN=$(curl -s "$SURL/stats" | j 'd["nodes"]')
[ "$SN" = "5" ] && ok "standby resynced after checkpoint" || no "resync failed (nodes=$SN)"
grep -q "standby_resync" "$WORK/standby.log" && ok "resync path exercised" || no "no resync logged"

echo "== 7. failover: primary dies, standby stays warm, promote =="
kill $PSRV 2>/dev/null; wait $PSRV 2>/dev/null
sleep 1
SN=$(curl -s "$SURL/stats" | j 'd["nodes"]')
[ "$SN" = "5" ] && ok "standby serves reads with primary down" || no "standby died with primary ($SN)"
# promote: stop the follower, restart the same WAL as a normal primary
kill $SSRV 2>/dev/null; wait $SSRV 2>/dev/null
"$BIN" --wal "$WORK/standby.wal" serve --addr "127.0.0.1:${SPORT}" --token root >"$WORK/promoted.log" 2>&1 &
NSRV=$!; sleep 1.2
SN=$(curl -s -H "$AT" "$SURL/stats" | j 'd["nodes"]')
[ "$SN" = "5" ] && ok "promoted with zero acked-write loss (RPO: replicated set)" || no "promotion lost data ($SN)"
C=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$AT" "$SURL/ingest" -d '{"json":"[{\"name\":\"post-failover\",\"type\":\"svc\"}]","ts":7000}')
[ "$C" = "200" ] && ok "promoted standby accepts writes" || no "promoted standby rejects writes ($C)"
kill $NSRV 2>/dev/null; wait $NSRV 2>/dev/null

rm -rf "$WORK"
echo; echo "==== $PASS passed, $FAIL failed ===="
[ "$FAIL" -eq 0 ]
