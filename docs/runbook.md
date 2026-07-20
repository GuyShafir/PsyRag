# Operations runbook

The reference for running PsyRag in production: capacity, upgrades, backup
drills, recovery objectives, and the high-availability position.

## Capacity planning

Everything lives in memory; plan RAM from the structural estimate the server
itself reports (`approx_bytes` in `GET /dbs`, `psyrag_db_approx_bytes` in
metrics — quota decisions use the same number).

Rules of thumb (measured via the built-in accounting):

| item | approximate cost |
|------|------------------|
| node | ~160 B + 2× name length |
| node version | ~72 B + serialized props length |
| edge | ~104 B + ~33 B sidecar columns |
| name-index token | token length + ~28 B |

Worked example: 1M edges, 400k nodes with 60-char names and 200 B props ≈
40 MB names/index + 110 MB versions + 137 MB edges+sidecar ≈ **~300 MB**, plus
transient request memory. Set `--max-db-mb` / `--max-mem-mb` from this and
watch `psyrag_db_approx_bytes` growth against quota; schedule checkpoints
before hitting it.

Throughput reference (GitHub Linux runner, release, 8 threads): ~2,500 req/s
mixed 80/15/5 read/feedback/ingest, retrieve p95 ≤ 5 ms, memory byte-stable
on read-only soak (`scripts/load.sh` reproduces this).

## Upgrade procedure

1. Take a backup (below) — upgrades are the moment format bugs would bite.
2. Stop the server (SIGTERM: it drains, flushes WALs, saves sidecars).
3. Swap the binary; restart. Replay is automatic; formats are versioned.
4. If you must **roll back** after a new binary wrote data: the old binary
   *refuses loudly* on any file written in a newer format ("newer than this
   binary understands"). That refusal is your signal to restore the step-1
   backup rather than run mixed-format state.

## Backup & restore drill

Backup (per database — a DB is one directory):

```bash
# offline / maintenance window (takes the WAL lock => consistent set):
psyrag --data-dir /data/dbs --db tenant-a backup --out /backups/tenant-a-$(date +%F)
# live alternatives: filesystem/volume snapshots, or stop-copy-start.
```

Restore + verify (practice this before you need it):

```bash
systemctl stop psyrag                  # or docker stop
cp /backups/tenant-a-2026-07-18/* /data/dbs/tenant-a/
psyrag --data-dir /data/dbs --db tenant-a verify   # CRC sweep + replay + sidecar binding
systemctl start psyrag
```

`verify` reports the WAL lineage id, record counts, sidecar binding and the
**learning gap** (feedback applied after the sidecar's LSN is lost on crash —
that is the learning-state RPO, bounded because every /feedback saves the
sidecar).

## RPO / RTO

- **Graph facts (WAL)**: RPO **0** for acknowledged writes — a 2xx means
  fsynced; proven continuously by the kill -9 suite in CI.
- **Learned salience (sidecar)**: RPO = since the last sidecar save. Every
  feedback/sleep/consolidate/checkpoint saves it, so in practice ≤ one
  feedback interval; worst case is bounded and *measured* by `verify`'s
  learning-gap report.
- **Deferred traces / idempotency records**: fsynced per write; RPO 0
  (unless `--ephemeral-traces`).
- **RTO**: process start + WAL replay. Replay is proportional to the
  *working log*, not all history — keep it small with scheduled checkpoints
  (`--checkpoint-every 24h`). Watch `psyrag_db_wal_lsn` growth between
  checkpoints as the replay-time proxy.

## Point-in-time access & history

Time-travel *reads* are native: any retrieval/diff accepts `ts`. Compaction
archives (`<wal>.archive-<ms>`) retain dropped history — to inspect a
pre-checkpoint state, copy the archive chain aside, concatenate archive +
current WAL in order into a scratch file, and open it read-only
(`psyrag --wal scratch verify` / `retrieve --ts ...`). Rotate archives
off-box on your backup cadence; **purged data lives in archives/backups
until you rotate them** — that is part of your deletion SLA.

## Redacting retained facts (props-level)

Purge-by-origin removes whole facts. To redact *properties* of a fact you
keep: re-observe the entity with cleaned props (supersession — the graph's
native update), then checkpoint to drop the old version from the working
log, then rotate archives/backups:

```bash
curl -X POST $URL/ingest -d '{"json":"[{\"name\":\"user-42\",\"type\":\"person\",\"props\":{\"email\":\"[redacted]\"}}]"}'
curl -X POST $URL/checkpoint -d '{"archive": false}'
```

## High availability: warm standby (WAL shipping)

Writes are still **single-node** (one process owns one data directory,
enforced by the WAL lock), but a read-only warm replica is built in:

```bash
# on the replica host
psyrag standby --primary http://primary:8080 --primary-token $ADMIN_TOKEN \
  --wal /data/standby.wal --addr 0.0.0.0:8080 [--follow-db NAME] [--poll-ms 1000]
```

How it works, and what it guarantees:

- The standby polls `GET /wal/tail/{offset}` and appends the primary's
  durable WAL bytes to its own log — the local WAL is an exact **byte-copy**,
  verified on every poll by re-fetching a 64-byte overlap window. Any
  divergence (primary checkpoint/purge, a swapped primary) is detected and
  the standby re-replicates from offset 0 automatically.
- Learned plasticity weights ship too: the standby refreshes the primary's
  sidecar snapshot every ~5 s, so ranking on the replica stays warm, not
  just the facts.
- The standby serves the full read surface (retrieve with
  `adapt=false`/`trace=false` semantics enforced by 503 on writes, `/graph`,
  `/stats`, `/match`, the console) and refuses every write with 503.
- **RPO**: facts acked by the primary up to one poll interval before failure
  (default 1 s); learned weights up to the sidecar cadence (~5 s). **RTO**:
  the standby is already serving reads; write failover = restart it as a
  normal primary.
- **Failover** (manual, deliberately): stop the standby process and start a
  regular `psyrag serve` on the same `--wal`. The replicated log replays and
  the promoted node accepts writes. Ensure the old primary stays down
  (fencing is the operator's job — there is no automatic leader election).
- The replication endpoints (`/wal/tail`, `/wal/sidecar`) expose raw facts
  and are denied to read-only tokens; give the standby a full or db-scoped
  token via `--primary-token`.

`scripts/standby.sh` drills the whole lifecycle in CI: replicate, read-only
enforcement, weight shipping, checkpoint resync, primary kill, promote,
zero acked-write loss.

Remaining posture for anything beyond one warm replica:

- fast restart (systemd/K8s restart policy) + small working logs = RTO in
  seconds;
- backups + the drill above = disaster recovery;
- **managed-backend tier (roadmap)**: the `GraphBackend` seam
  (`psyrag_core::backend`) is real, compilable, and conformance-tested
  against the in-memory reference; a Spanner/AlloyDB implementation moves
  durability and scale to the managed store. It requires taking client
  dependencies — a deliberate departure from the zero-dep core to be decided
  when needed.

## Monitoring quick reference

Scrape `GET /metrics`. Page on `psyrag_db_wedged > 0` and any 5xx rate;
watch `psyrag_db_approx_bytes` vs. quota, `psyrag_db_wal_lsn` vs. checkpoint
cadence, and `psyrag_request_duration_seconds` p99 per route. Details in
[deployment.md](deployment.md).
