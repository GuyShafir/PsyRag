#!/usr/bin/env bash
# End-to-end scripted test. Exits non-zero on any failed assertion.
# Usage: scripts/smoke.sh [BIN] [PORT]
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${1:-./target/release/psyrag}"
PORT="${2:-8791}"
URL="http://127.0.0.1:${PORT}"
WORK="$(mktemp -d)"
WAL="$WORK/g.wal"
PASS=0; FAIL=0
ok(){ echo "  ✓ $1"; PASS=$((PASS+1)); }
no(){ echo "  ✗ $1"; FAIL=$((FAIL+1)); }
j(){ python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }

[ -x "$BIN" ] || { echo "binary not found: $BIN (run scripts/install.sh)"; exit 2; }

echo "== 1. ingest =="
cat > "$WORK/inv.json" <<'JSON'
[{"name":"api","type":"svc","edges":[
  {"dst":"db","kind":"CALLS"},{"dst":"cache","kind":"CALLS"},
  {"dst":"queue","kind":"CALLS"},{"dst":"authz","kind":"CALLS"}]}]
JSON
E=$("$BIN" --wal "$WAL" ingest --file "$WORK/inv.json" --ts 1000 | j "d['edges']")
[ "$E" = "4" ] && ok "ingested 4 edges" || no "expected 4 edges got $E"

echo "== 2. serve + health =="
"$BIN" --wal "$WAL" serve --addr "127.0.0.1:${PORT}" >"$WORK/serve.log" 2>&1 &
SRV=$!; sleep 1.5
curl -sf "$URL/health" >/dev/null && ok "server healthy" || no "server not healthy"

echo "== 3. retrieve (traced) =="
RT=$(curl -s -X POST "$URL/retrieve" -H 'Content-Type: application/json' \
     -d '{"seeds":["api"],"depth":1,"top_k":5,"ts":2000,"trace":true}')
MASS=$(echo "$RT" | j "d['result']['mass']"); TID=$(echo "$RT" | j "d['trace_id']")
python3 -c "import sys;exit(0 if $MASS>0 else 1)" && ok "retrieval mass>0 ($MASS)" || no "mass not positive"

echo "== 4. learning converges (db becomes dominant) =="
for i in $(seq 1 20); do T=$((1000+i*1000));
  curl -s -X POST "$URL/feedback" -H 'Content-Type: application/json' \
    -d "{\"seeds\":[\"api\"],\"used\":[\"db\"],\"depth\":1,\"ts\":$T}" >/dev/null
  curl -s -X POST "$URL/consolidate" -H 'Content-Type: application/json' -d "{\"ts\":$T}" >/dev/null
done
AFT=$(curl -s -X POST "$URL/retrieve" -H 'Content-Type: application/json' \
      -d '{"seeds":["api"],"depth":1,"top_k":5,"ts":21000}')
DBW=$(echo "$AFT" | python3 -c "import sys,json;d=json.load(sys.stdin);t={x['node']:x['activation'] for x in d['top']};print(t.get('db',0))")
OTH=$(echo "$AFT" | python3 -c "import sys,json;d=json.load(sys.stdin);t={x['node']:x['activation'] for x in d['top']};print(max(t.get('cache',0),t.get('queue',0),t.get('authz',0)))")
python3 -c "import sys;exit(0 if $DBW>1.5*$OTH else 1)" && ok "used edge dominates (db=$DBW vs sibling=$OTH)" || no "no convergence (db=$DBW sibling=$OTH)"

echo "== 5. durable trace survives restart =="
kill $SRV 2>/dev/null; sleep 0.5
"$BIN" --wal "$WAL" serve --addr "127.0.0.1:${PORT}" >"$WORK/serve2.log" 2>&1 &
SRV=$!; sleep 1.5
FB=$(curl -s -X POST "$URL/feedback" -H 'Content-Type: application/json' -d "{\"trace_id\":$TID,\"used\":[\"db\"],\"ts\":30000}")
RE=$(echo "$FB" | j "d.get('edges_reinforced',0)")
[ "$RE" -ge 1 ] && ok "deferred credit applied after restart (trace #$TID)" || no "trace did not survive restart"

echo "== 6. consolidation reports =="
CS=$(curl -s -X POST "$URL/consolidate" -H 'Content-Type: application/json' -d '{"ts":31000}' | j "d['stats']['live_edges']")
[ "$CS" -ge 1 ] && ok "consolidation ran (live_edges=$CS)" || no "consolidation failed"

echo "== 7. sleep (downscale + protect) =="
SL=$(curl -s -X POST "$URL/sleep" -H 'Content-Type: application/json' -d '{"ts":40000}')
DS=$(echo "$SL" | j "d['downscaled']"); PR=$(echo "$SL" | j "d['protected']")
[ "$DS" -ge 1 ] && ok "sleep ran (downscaled=$DS, protected=$PR)" || no "sleep did not run"

kill $SRV 2>/dev/null; rm -rf "$WORK"
echo; echo "==== $PASS passed, $FAIL failed ===="
[ "$FAIL" -eq 0 ]
