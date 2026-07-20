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
kill $SRV 2>/dev/null; wait $SRV 2>/dev/null
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

kill $SRV 2>/dev/null; wait $SRV 2>/dev/null

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
kill $MSRV 2>/dev/null; wait $MSRV 2>/dev/null

echo "== 9. single-writer lock =="
"$BIN" --wal "$WAL" serve --addr "127.0.0.1:${PORT}" >"$WORK/serve3.log" 2>&1 &
SRV=$!; sleep 1.5
"$BIN" --wal "$WAL" stats >/dev/null 2>&1 \
  && no "CLI opened a WAL held by the server" \
  || ok "CLI refused while server holds the WAL lock"
kill $SRV 2>/dev/null; wait $SRV 2>/dev/null

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
kill $SRV 2>/dev/null; wait $SRV 2>/dev/null

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
kill $PSRV 2>/dev/null; wait $PSRV 2>/dev/null
GOOD=$("$BIN" --wal "$PWAL" retrieve --seed hub --top-k 5 --ts 3000 | j "[x['node'] for x in d['top']]")
echo "$GOOD" | grep -q "good" && ok "unrelated facts survive purge + restart" || no "collateral loss: $GOOD"

echo "== 14. operability: prometheus, api header, json logs, scheduler =="
OWAL="$WORK/ops.wal"; OPORT=$((PORT+3)); OURL="http://127.0.0.1:${OPORT}"
"$BIN" --wal "$OWAL" serve --addr "127.0.0.1:${OPORT}" --log-format json \
  --consolidate-every 1s >"$WORK/serve-ops.log" 2>&1 &
OSRV=$!; sleep 1.5
curl -s -X POST "$OURL/ingest" -H 'Content-Type: application/json' \
  -d '{"json":"[{\"name\":\"x\",\"type\":\"t\",\"edges\":[{\"dst\":\"y\",\"kind\":\"REL\"}]}]","ts":1000}' >/dev/null
curl -sI "$OURL/health" | grep -qi "x-psyrag-api: 1" && ok "API version header present" || no "missing X-PsyRag-Api"
M=$(curl -s "$OURL/metrics")
echo "$M" | grep -q "psyrag_requests_total{route=\"ingest\",status=\"2xx\"}" \
  && ok "prometheus request counters" || no "no request counters in /metrics"
echo "$M" | grep -q "psyrag_db_wal_lsn{db=\"default\"}" \
  && ok "prometheus per-db gauges" || no "no per-db gauges in /metrics"
sleep 1.5
grep -q '"event":"request"' "$WORK/serve-ops.log" && ok "json request log emitted" || no "no structured request log"
grep -q '"event":"maintenance"' "$WORK/serve-ops.log" && ok "scheduled maintenance ran" || no "scheduler did not fire"
kill $OSRV 2>/dev/null; wait $OSRV 2>/dev/null

echo "== 15. quotas: over-quota ingest sheds, maintenance still works =="
QWAL="$WORK/quota.wal"; QPORT=$((PORT+4)); QURL="http://127.0.0.1:${QPORT}"
"$BIN" --wal "$QWAL" serve --addr "127.0.0.1:${QPORT}" --max-db-edges 2 >"$WORK/serve-q.log" 2>&1 &
QSRV=$!; sleep 1.5
curl -s -X POST "$QURL/ingest" -H 'Content-Type: application/json' \
  -d '{"json":"[{\"name\":\"q\",\"type\":\"t\",\"edges\":[{\"dst\":\"e1\",\"kind\":\"R\"},{\"dst\":\"e2\",\"kind\":\"R\"}]}]","ts":1000}' >/dev/null
C=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$QURL/ingest" -H 'Content-Type: application/json' \
  -d '{"json":"[{\"name\":\"q\",\"type\":\"t\",\"edges\":[{\"dst\":\"e3\",\"kind\":\"R\"}]}]","ts":2000}')
[ "$C" = "507" ] && ok "over-quota ingest -> 507" || no "expected 507 got $C"
C=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$QURL/consolidate" -d '{}')
[ "$C" = "200" ] && ok "maintenance allowed at quota" || no "consolidate blocked at quota ($C)"
kill $QSRV 2>/dev/null; wait $QSRV 2>/dev/null

echo "== 16. ENOSPC fault injection (tiny volume) =="
EVOL=""
if [ "$(uname)" = "Darwin" ]; then
  DEV=$(hdiutil attach -nomount ram://4096 2>/dev/null | tr -d ' ')
  if [ -n "$DEV" ] && diskutil erasevolume HFS+ psyragsmoke "$DEV" >/dev/null 2>&1; then
    EVOL="/Volumes/psyragsmoke"
  fi
elif [ "$(uname)" = "Linux" ] && sudo -n true 2>/dev/null; then
  EVOL="$WORK/tinyfs"; mkdir -p "$EVOL"
  sudo -n mount -t tmpfs -o size=2m tmpfs "$EVOL" || EVOL=""
fi
if [ -z "$EVOL" ]; then
  echo "  - skipped (no ramdisk/tmpfs available on this host)"
else
  EPORT=$((PORT+5)); EURL="http://127.0.0.1:${EPORT}"
  "$BIN" --wal "$EVOL/e.wal" serve --addr "127.0.0.1:${EPORT}" >"$WORK/serve-e.log" 2>&1 &
  ESRV=$!; sleep 1.5
  BLOB=$(python3 -c "print('z'*65536)")
  LAST=200
  for i in $(seq 1 80); do
    LAST=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$EURL/ingest" -H 'Content-Type: application/json' \
      -d "{\"json\":\"[{\\\"name\\\":\\\"f$i\\\",\\\"type\\\":\\\"t\\\",\\\"props\\\":{\\\"b\\\":\\\"$BLOB\\\"}}]\",\"ts\":$((5000+i))}")
    [ "$LAST" != "200" ] && break
  done
  [ "$LAST" = "500" ] && ok "disk-full ingest failed clean (500, no partial ack)" || no "expected 500 on ENOSPC, got $LAST"
  W=$(curl -s "$EURL/dbs" | j "d['dbs'][0].get('wedged') is not None and d['dbs'][0]['wedged'] is not None")
  [ "$W" = "True" ] && ok "database wedged after WAL write failure" || no "not wedged: $W"
  C=$(curl -s -o /dev/null -w "%{http_code}" "$EURL/stats")
  [ "$C" = "200" ] && ok "reads keep serving while wedged" || no "reads failed while wedged ($C)"
  C=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$EURL/ingest" -d '{"json":"[]"}')
  [ "$C" = "503" ] && ok "writes refused while wedged -> 503" || no "expected 503 got $C"
  kill $ESRV 2>/dev/null; wait $ESRV 2>/dev/null
  # restart on the still-full volume: replay must recover (torn tail tolerated)
  "$BIN" --wal "$EVOL/e.wal" serve --addr "127.0.0.1:${EPORT}" >"$WORK/serve-e2.log" 2>&1 &
  ESRV=$!; sleep 1.5
  C=$(curl -s -o /dev/null -w "%{http_code}" "$EURL/stats")
  [ "$C" = "200" ] && ok "restart on full volume recovers (replay + torn-tail repair)" || no "restart failed ($C)"
  kill $ESRV 2>/dev/null; wait $ESRV 2>/dev/null
  if [ "$(uname)" = "Darwin" ]; then hdiutil detach "$EVOL" -force >/dev/null 2>&1; else sudo -n umount "$EVOL" 2>/dev/null; fi
fi

echo "== 17. durable idempotency: replay survives restart =="
DWAL="$WORK/didem.wal"; DPORT=$((PORT+6)); DURL="http://127.0.0.1:${DPORT}"
"$BIN" --wal "$DWAL" serve --addr "127.0.0.1:${DPORT}" >"$WORK/serve-d.log" 2>&1 &
DSRV=$!; sleep 1.5
DH='Idempotency-Key: durable-key-1'
curl -s -X POST "$DURL/ingest" -H 'Content-Type: application/json' \
  -d '{"json":"[{\"name\":\"seedn\",\"type\":\"t\",\"edges\":[{\"dst\":\"tgt\",\"kind\":\"R\"}]}]","ts":1000}' >/dev/null
R1=$(curl -s -H "$DH" -X POST "$DURL/feedback" -H 'Content-Type: application/json' \
  -d '{"seeds":["seedn"],"used":["tgt"],"ts":2000}')
kill $DSRV 2>/dev/null; wait $DSRV 2>/dev/null
"$BIN" --wal "$DWAL" serve --addr "127.0.0.1:${DPORT}" >"$WORK/serve-d2.log" 2>&1 &
DSRV=$!; sleep 1.5
R2=$(curl -sD "$WORK/didem.hdr" -H "$DH" -X POST "$DURL/feedback" -H 'Content-Type: application/json' \
  -d '{"seeds":["seedn"],"used":["tgt"],"ts":2000}')
[ "$R1" = "$R2" ] && ok "post-restart retry replayed the original response" || no "responses differ across restart"
grep -qi "idempotency-replayed: true" "$WORK/didem.hdr" \
  && ok "post-restart retry served from the DURABLE replay log" || no "restart lost the dedup record"
kill $DSRV 2>/dev/null; wait $DSRV 2>/dev/null

echo "== 18. per-db tokens + poisoning limits =="
SPORT=$((PORT+7)); SURL="http://127.0.0.1:${SPORT}"
"$BIN" --data-dir "$WORK/scoped" serve --addr "127.0.0.1:${SPORT}" \
  --token rootsecret --db-token tenant-a=akey --max-feedback-per-min 2 >"$WORK/serve-s.log" 2>&1 &
SSRV=$!; sleep 1.5
RT='Authorization: Bearer rootsecret'; AT='Authorization: Bearer akey'
curl -s -X POST -H "$RT" "$SURL/db/tenant-a" >/dev/null
curl -s -X POST -H "$RT" "$SURL/db/tenant-b" >/dev/null
C=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$AT" "$SURL/db/tenant-a/ingest" \
  -H 'Content-Type: application/json' \
  -d '{"json":"[{\"name\":\"own\",\"type\":\"t\",\"edges\":[{\"dst\":\"x\",\"kind\":\"R\"}]}]","ts":1000}')
[ "$C" = "200" ] && ok "db token writes its own db" || no "db token blocked on own db ($C)"
C=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$AT" "$SURL/db/tenant-b/retrieve" -d '{"seeds":["own"],"adapt":false}')
[ "$C" = "403" ] && ok "db token denied on another db -> 403" || no "cross-db leak ($C)"
C=$(curl -s -o /dev/null -w "%{http_code}" -H "$AT" "$SURL/dbs")
[ "$C" = "403" ] && ok "db token denied server admin -> 403" || no "admin leak ($C)"
curl -s -X POST -H "$AT" "$SURL/db/tenant-a/feedback" -d '{"seeds":["own"],"used":["x"],"ts":2000}' >/dev/null
curl -s -X POST -H "$AT" "$SURL/db/tenant-a/feedback" -d '{"seeds":["own"],"used":["x"],"ts":2001}' >/dev/null
C=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$AT" "$SURL/db/tenant-a/feedback" -d '{"seeds":["own"],"used":["x"],"ts":2002}')
[ "$C" = "429" ] && ok "feedback rate limit -> 429" || no "rate limit missing ($C)"
kill $SSRV 2>/dev/null; wait $SSRV 2>/dev/null

echo "== 19. semantic seed selection (embeddings) =="
VPORT=$((PORT+8)); VURL="http://127.0.0.1:${VPORT}"
"$BIN" --data-dir "$WORK/vec" serve --addr "127.0.0.1:${VPORT}" >"$WORK/serve-v.log" 2>&1 &
VSRV=$!; sleep 1.5
# three nodes with 2-D embeddings pointing in distinct directions
curl -s -X POST "$VURL/ingest" -H 'Content-Type: application/json' -d '{"json":"[{\"name\":\"east\",\"type\":\"doc\",\"props\":{\"embedding\":[1.0,0.0]}},{\"name\":\"north\",\"type\":\"doc\",\"props\":{\"embedding\":[0.0,1.0]}},{\"name\":\"nearby\",\"type\":\"doc\",\"props\":{\"embedding\":[0.9,0.1]}}]","ts":1000}' >/dev/null
# vector /match: a query near "east" ranks east first, nearby second, north last
M=$(curl -s -X POST "$VURL/match" -d '{"vector":[1.0,0.0],"limit":3}')
FIRST=$(echo "$M" | j 'd["nodes"][0]'); IDX=$(echo "$M" | j 'd["indexed"]')
[ "$FIRST" = "east" ] && ok "vector match ranks nearest first" || no "vector match wrong order ($FIRST)"
[ "$IDX" = "3" ] && ok "match reports 3 indexed embeddings" || no "wrong indexed count ($IDX)"
# scored hits present and descending
DESC=$(echo "$M" | python3 -c 'import sys,json;h=json.load(sys.stdin)["hits"];print("ok" if h[0]["score"]>=h[1]["score"]>=h[2]["score"] else "no")')
[ "$DESC" = "ok" ] && ok "vector match scores descending" || no "scores not sorted"
# semantic retrieve: seed_vector resolves seeds and echoes resolved_seeds
R=$(curl -s -X POST "$VURL/retrieve" -d '{"seed_vector":[1.0,0.0],"seed_k":1,"adapt":false}')
RS=$(echo "$R" | j 'd["resolved_seeds"][0]["node"]')
TOP=$(echo "$R" | j 'd["result"]["top"][0]["node"]')
[ "$RS" = "east" ] && ok "retrieve resolves seed_vector -> east" || no "seed_vector resolution wrong ($RS)"
[ "$TOP" = "east" ] && ok "semantic retrieval surfaces the resolved seed" || no "no surfaced node ($TOP)"
# empty/zero vector rejected
C=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$VURL/match" -d '{"vector":[0.0,0.0],"limit":3}')
[ "$C" = "400" ] && ok "zero query vector -> 400" || no "zero vector not rejected ($C)"
# embedding survives restart (rides the WAL) and updates on re-observe
kill $VSRV 2>/dev/null; wait $VSRV 2>/dev/null
"$BIN" --data-dir "$WORK/vec" serve --addr "127.0.0.1:${VPORT}" >"$WORK/serve-v2.log" 2>&1 &
VSRV=$!; sleep 1.5
M=$(curl -s -X POST "$VURL/match" -d '{"vector":[1.0,0.0],"limit":1}')
[ "$(echo "$M" | j 'd["indexed"]')" = "3" ] && ok "embeddings survive restart (WAL replay)" || no "embeddings lost on restart"
kill $VSRV 2>/dev/null; wait $VSRV 2>/dev/null

rm -rf "$WORK"
echo; echo "==== $PASS passed, $FAIL failed ===="
[ "$FAIL" -eq 0 ]
