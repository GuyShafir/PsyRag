# Reference

Complete reference for the `psyrag` binary: CLI commands, HTTP API, and every
config knob.

## Global flags

Apply to every subcommand:

| flag | default | meaning |
|------|---------|---------|
| `--wal PATH` | `psyrag.wal` | graph WAL (facts). Created if absent. |
| `--sidecar PATH` | `<wal>.psyrag.json` | plasticity state (weights, decay, homeostat) |
| `--config PATH` | built-in defaults | config JSON (see below) |

## CLI commands

### `psyrag config [--write PATH]`
Print the effective config as JSON, or with `--write` emit a fully-commented
example to `PATH`.

### `psyrag ingest --file F [--reconcile] [--cai] [--ts MS]`
Ingest entity JSON into the WAL, then sync the sidecar.
- `--file` — entity array / NDJSON, or a Cloud Asset Inventory snapshot with `--cai`.
- `--reconcile` — treat the snapshot as ground truth (retire missing edges).
- `--cai` — parse a GCP CAI export (requires the `gcp` build feature).
- `--ts` — event time in ms (default: now).

### `psyrag retrieve --seed N [--seed N…] [--depth D] [--fan F] [--top-k K] [--ts MS] [--adapt]`
Weighted spreading-activation retrieval.
- `--seed` — one or more seed node names (repeatable).
- `--depth` / `--fan` — spread hops / per-hop outflow factor (default from config).
- `--top-k` — number of results (default 10).
- `--adapt` — also feed the homeostat (updates `lambda_scale`, persists sidecar).

### `psyrag touch --edge src,dst,kind,R [--edge …] [--ts MS]`
Low-level manual reinforcement of specific edges by amount `R` (repeatable).

### `psyrag feedback --seed N… { --used NODE… | --credit name,score… | --reward R [--spread by_activation|uniform] } [--depth D] [--top-k K] [--ts MS]`
Usage feedback (the learning step). Choose one credit mode:
- `--used NODE…` — explicit: these surfaced nodes were useful.
- `--credit name,score…` — graded / contrastive (mixed signs allowed).
- `--reward R [--spread …]` — episodic: one scalar spread over surfaced nodes.

### `psyrag consolidate [--ts MS] [--apply-conflicts]`
Run a daytime consolidation cycle (materialize / prune / renorm + conflict
detection). `--apply-conflicts` journals detected supersessions (a truth change).

### `psyrag sleep [--ts MS]`
Run an offline sleep cycle (downscale + protected prune + renorm). Schedule this
nightly; it is not on the retrieval path.

### `psyrag stats`
Print sidecar + graph stats as JSON.

### `psyrag export-bq [--out DIR] [--dataset NAME] [--ts MS]`
Export the learned graph as BigQuery-ready NDJSON + DDL + GQL + a load script.
Defaults: `--out ./bq_out`, `--dataset psyrag`. No GCP credentials needed to
produce the artifacts. See [gcp/README.md](../gcp/README.md).

### `psyrag serve [--addr HOST:PORT]`
Run the HTTP server (default `0.0.0.0:8080`) with the web console at `/`.

### `psyrag monitor [--url URL] [--interval-ms N]`
Live terminal dashboard polling a running server's `/metrics`.

## HTTP API

Base URL is the `serve` address. All bodies and responses are JSON.

### `GET /` and `GET /ui`
The web console (HTML). See [integrations.md](integrations.md).

### `GET /health`
`{ "ok": true }`.

### `GET /stats` and `GET /metrics`
Sidecar + graph stats: `nodes`, `edges_total`, `edges_live`, `edges_dead`,
`lambda_scale`, `setpoint`, `ewma_mass`, `integral`, `weight_min/mean/max`.

### `POST /ingest`
`{ "json": "<entities>", "reconcile"?: bool, "cai"?: bool, "ts"?: int }` →
`{ "edges", "nodes", "stale_retired" }`.

### `POST /retrieve`
`{ "seeds": [..], "depth"?, "fan"?, "top_k"?, "ts"?, "adapt"?: bool, "trace"?: bool }`.
Returns `{ mass, lambda_scale, top: [{node, node_type, activation}] }`. If
`"trace": true`, returns `{ result, trace_id }` and stores the trace for deferred
feedback.

### `POST /match`
`{ "tokens": [..], "limit"? }` → `{ "nodes": [names] }`. Resolve free-text tokens
to existing node names (substring, case-insensitive). Used for seed selection.

### `POST /feedback`
Provide **one** target and **one** credit spec.
- Target: `"seeds": [..]` (retrieve fresh at `ts`) **or** `"trace_id": N` (credit a
  stored trace — deferred).
- Credit: `"used": [names]` (explicit) **or** `"nodes": [[name,score]]`
  (graded/contrastive) **or** `"reward": R, "spread": "by_activation"|"uniform"`
  (episodic).
Returns `{ edges_reinforced, total_positive_r, total_negative_r, hits, misses }`.

### `POST /consolidate`
`{ "ts"?, "apply_conflicts"?: bool }` → `{ stats, conflicts, applied_ops }`.

### `POST /sleep`
`{ "ts"? }` → `{ downscaled, pruned, protected, renormalized_sources, live_after,
mean_weight_before, mean_weight_after }`.

### `GET /graph`
`{ nodes: [{id, name, type}], edges: [{source, target, kind, weight, open}],
truncated }`. Current weighted graph for visualization (bounded).

### `GET /traces`
`{ ids: [..], count }`. Stored trace ids.

### `GET /trace/{id}`
`{ t, surfaced: [{node, activation}], fired: [{src, dst, kind, delta}] }`.

## Config reference

Config is JSON (`psyrag config --write config.json` emits a commented example).
Every field is optional; omitted fields take the default shown.

### Update rule
| key | default | meaning |
|-----|---------|---------|
| `alpha` | 0.05 | reinforcement gain per unit clipped R |
| `lambda_base` | 0.05 | base decay rate per second |
| `beta` | 1.0 | authority sensitivity in `λ_base/(1+β·auth)` |
| `w0` | 0.5 | initial weight of a new edge |
| `r_clip` | 1.0 | per-touch clip on R |

### Homeostat
| key | default | meaning |
|-----|---------|---------|
| `setpoint` | 0.5 | target mean activated mass |
| `k_i` | 0.02 | integral gain |
| `ewma_beta` | 0.8 | smoothing on observed mass |
| `scale_min` / `scale_max` | 0.25 / 8.0 | bounds on `lambda_scale` |
| `integral_min` / `integral_max` | −500 / 500 | anti-windup clamp |

### Retrieval
| key | default | meaning |
|-----|---------|---------|
| `depth` | 2 | spread hops |
| `fan` | 0.9 | per-hop outflow factor |
| `activation_epsilon` | 1e-6 | prune propagation below this |

### Consolidation
| key | default | meaning |
|-----|---------|---------|
| `theta` | 0.01 | daytime prune floor |
| `norm_target` | 1.0 | per-source L1 budget |

### Authority & conflicts
| key | default | meaning |
|-----|---------|---------|
| `authority_by_kind` | `{}` | per-kind authority (raises decay resistance) |
| `authority_default` | 0.0 | authority for unlisted kinds |
| `functional_kinds` | `[]` | single-valued predicates (only these are conflict-checked) |

### Feedback
| key | default | meaning |
|-----|---------|---------|
| `feedback_gamma` | 0.5 | path-credit decay per hop upstream |
| `feedback_hit` | 1.0 | credit magnitude for a used node |
| `feedback_miss_penalty` | 0.0 | negative credit for surfaced-but-unused (0 = off) |

### Sleep
| key | default | meaning |
|-----|---------|---------|
| `sleep_downscale` | 0.6 | multiplicative weight downscale (<1) |
| `sleep_theta` | 0.05 | aggressive prune floor after downscale |
| `protect_top_frac` | 0.2 | top fraction by weight protected from the sleep prune |
