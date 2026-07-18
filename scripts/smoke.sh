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

kill $SRV 2>/dev/null; sleep 0.5

echo "== 8. multidb: isolation + auth =="
MPORT=$((PORT+1)); MURL="http://127.0.0.1:${MPORT}"
"$BIN" --data-dir "$WORK/dbs" serve --addr "127.0.0.1:${MPORT}" \
  --token adminsecret --read-token viewersecret >"$WORK/serve-mdb.log" 2>&1 &
MSRV=$!; sleep 1.5
AH='Authorization: Bearer adminsecret'; RH='Authorization: Bearer viewersecret'
# unauthenticated write refused
C=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$MURL/ingest" -d '{"json":"[]"}')
[ "$C" = "401" ] && ok "unauthenticated write -> 401" || no "expected 401 got $C"
# create a second db and ingest disjoint facts into both
curl -s -X POST -H "$AH" "$MURL/db/tenant-b" >/dev/null
curl -s -X POST -H "$AH" "$MURL/ingest" -H 'Content-Type: application/json' \
  -d '{"json":"[{\"name\":\"alpha\",\"type\":\"t\",\"edges\":[{\"dst\":\"beta\",\"kind\":\"REL\"}]}]","ts":1000}' >/dev/null
curl -s -X POST -H "$AH" "$MURL/db/tenant-b/ingest" -H 'Content-Type: application/json' \
  -d '{"json":"[{\"name\":\"gamma\",\"type\":\"t\",\"edges\":[{\"dst\":\"delta\",\"kind\":\"REL\"}]}]","ts":1000}' >/dev/null
DEF=$(curl -s -X POST -H "$AH" "$MURL/retrieve" -d '{"seeds":["gamma"],"adapt":false,"ts":2000}' | j "len(d['top'])")
TEN=$(curl -s -X POST -H "$AH" "$MURL/db/tenant-b/retrieve" -d '{"seeds":["gamma"],"adapt":false,"ts":2000}' | j "len(d['top'])")
[ "$DEF" = "0" ] && [ "$TEN" -ge 2 ] && ok "dbs are isolated (gamma only in tenant-b)" || no "isolation broken (default=$DEF tenant-b=$TEN)"
# read token can read but not write or adapt
C=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$RH" "$MURL/db/tenant-b/retrieve" -d '{"seeds":["gamma"],"adapt":false}')
[ "$C" = "200" ] && ok "read token can retrieve" || no "read token retrieve got $C"
C=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$RH" "$MURL/db/tenant-b/ingest" -d '{"json":"[]"}')
[ "$C" = "403" ] && ok "read token cannot write -> 403" || no "expected 403 got $C"
kill $MSRV 2>/dev/null; sleep 0.5

echo "== 9. single-writer lock =="
"$BIN" --wal "$WAL" serve --addr "127.0.0.1:${PORT}" >"$WORK/serve3.log" 2>&1 &
SRV=$!; sleep 1.5
"$BIN" --wal "$WAL" stats >/dev/null 2>&1 \
  && no "CLI opened a WAL held by the server" \
  || ok "CLI refused while server holds the WAL lock"
kill $SRV 2>/dev/null; sleep 0.5

echo "== 10. checkpoint: log shrinks, learned salience survives replay =="
# build up version history so there is something for compaction to drop
for i in 1 2 3; do
  sed "s/\"type\":\"svc\"/\"type\":\"svc\",\"props\":{\"rev\":$i}/" "$WORK/inv.json" > "$WORK/inv$i.json"
  "$BIN" --wal "$WAL" ingest --file "$WORK/inv$i.json" --ts $((41000+i*100)) >/dev/null
done
BEFORE=$(wc -c < "$WAL" | tr -d ' ')
CK=$("$BIN" --wal "$WAL" checkpoint)
AFTER=$(echo "$CK" | j "d['report']['bytes_after']")
python3 -c "import sys;exit(0 if $AFTER < $BEFORE else 1)" \
  && ok "checkpoint shrank the WAL ($BEFORE -> $AFTER bytes)" || no "WAL did not shrink"
"$BIN" --wal "$WAL" verify >/dev/null 2>&1 && ok "compacted WAL verifies" || no "verify failed"
# the learned dominance from step 4 must survive the id renumbering
CKW=$("$BIN" --wal "$WAL" retrieve --seed api --depth 1 --top-k 5 --ts 50000 \
  | python3 -c "import sys,json;d=json.load(sys.stdin);t={x['node']:x['activation'] for x in d['top']};print(1 if t.get('db',0)>t.get('cache',1) else 0)")
[ "$CKW" = "1" ] && ok "learned weights survive checkpoint (stable keys)" || no "weights lost in checkpoint"

echo "== 11. backup =="
"$BIN" --wal "$WAL" backup --out "$WORK/bak" >/dev/null \
  && [ -f "$WORK/bak/manifest.json" ] \
  && ok "backup wrote manifest + files" || no "backup failed"

echo "== 12. idempotent retry (same key never double-applies) =="
"$BIN" --wal "$WAL" serve --addr "127.0.0.1:${PORT}" >"$WORK/serve4.log" 2>&1 &
SRV=$!; sleep 1.5
IH='Idempotency-Key: smoke-idem-1'
F1=$(curl -s -H "$IH" -X POST "$URL/feedback" -H 'Content-Type: application/json' \
     -d '{"seeds":["api"],"used":["db"],"ts":60000}')
F2=$(curl -sD "$WORK/idem.hdr" -H "$IH" -X POST "$URL/feedback" -H 'Content-Type: application/json' \
     -d '{"seeds":["api"],"used":["db"],"ts":60000}')
[ "$F1" = "$F2" ] && ok "retry returned the identical response" || no "responses differ: $F1 vs $F2"
grep -qi "idempotency-replayed: true" "$WORK/idem.hdr" \
  && ok "retry was served from the replay cache" || no "no replay marker on retry"
kill $SRV 2>/dev/null; sleep 0.5

echo "== 13. provenance: trust quarantine + purge-by-subject =="
PWAL="$WORK/prov.wal"; PPORT=$((PORT+2)); PURL="http://127.0.0.1:${PPORT}"
"$BIN" --wal "$PWAL" serve --addr "127.0.0.1:${PPORT}" --token provsecret >"$WORK/serve-prov.log" 2>&1 &
PSRV=$!; sleep 1.5
PH='Authorization: Bearer provsecret'
curl -s -X POST -H "$PH" "$PURL/ingest" -H 'Content-Type: application/json' -d '{
  "json":"[{\"name\":\"hub\",\"type\":\"t\",\"edges\":[{\"dst\":\"good\",\"kind\":\"REL\"}]}]",
  "origin":"user:bob","ts":1000}' >/dev/null
curl -s -X POST -H "$PH" "$PURL/ingest" -H 'Content-Type: application/json' -d '{
  "json":"[{\"name\":\"hub\",\"type\":\"t\",\"edges\":[{\"dst\":\"evil-fact\",\"kind\":\"REL\"}]}]",
  "origin":"user:mallory","ts":1000}' >/dev/null
BOTH=$(curl -s -X POST -H "$PH" "$PURL/retrieve" -d '{"seeds":["hub"],"adapt":false,"ts":2000}' \
  | j "sorted(x['node'] for x in d['top'] if x['node']!='hub')")
[ "$BOTH" = "['evil-fact', 'good']" ] && ok "both origins recalled pre-quarantine" || no "unexpected recall: $BOTH"
curl -s -X POST -H "$PH" "$PURL/quarantine" -d '{"origin_prefix":"user:mallory","trust":0.0}' >/dev/null
QUAR=$(curl -s -X POST -H "$PH" "$PURL/retrieve" -d '{"seeds":["hub"],"adapt":false,"ts":2000}' \
  | j "[x['node'] for x in d['top'] if x['node']=='evil-fact']")
[ "$QUAR" = "[]" ] && ok "quarantined origin masked from recall" || no "quarantine leak: $QUAR"
PR=$(curl -s -X POST -H "$PH" "$PURL/purge" -d '{"origin_prefix":"user:mallory"}' | j "d['report']['nodes_dropped']")
[ "$PR" -ge 1 ] && ok "purge dropped the subject's facts (nodes=$PR)" || no "purge failed"
grep -q "evil-fact" "$PWAL" && no "purged name still on disk" || ok "purged name gone from the WAL bytes"
kill $PSRV 2>/dev/null; sleep 0.5
GOOD=$("$BIN" --wal "$PWAL" retrieve --seed hub --top-k 5 --ts 3000 | j "[x['node'] for x in d['top']]")
echo "$GOOD" | grep -q "good" && ok "unrelated facts survive purge + restart" || no "collateral loss: $GOOD"

rm -rf "$WORK"
echo; echo "==== $PASS passed, $FAIL failed ===="
[ "$FAIL" -eq 0 ]
