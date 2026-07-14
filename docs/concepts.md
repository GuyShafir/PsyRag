# Concepts & mechanism

PsyRag models memory the way a brain does: relationships strengthen with use,
fade without it, compete for limited budget, and consolidate offline. This
document is the complete "how it works" — the mental model and the math.

## 1. The substrate: a temporal typed property graph

Facts are stored in `psyrag-graph`: an **append-only**, **temporal**, **typed**
property graph.

- **Nodes** have a name, a type, and versioned properties.
- **Edges** are `(src, dst, kind)` with a validity interval `[valid_from, valid_to)`.
  `kind` is the interned predicate (e.g. `CALLS`, `DEPENDS_ON`, `MENTIONS`).
- **Append-only**: nothing is ever deleted. Retiring an edge sets `valid_to`;
  a new fact opens a new interval. This gives **native supersession** and full
  time-travel — you can ask what the graph looked like at any past instant.
- `Ts` is `i64` **milliseconds**. Edge ids (`EdgeId = u32`) are **dense and
  stable** — the property that makes everything else efficient.

## 2. The memory trace: plasticity as a sidecar

Every edge carries a plasticity **weight** — its salience. This lives in
`psyrag-core` as a **sidecar**: parallel columns keyed by the graph's stable
`EdgeId`, never inside the graph's edge records. The graph stays pristine (pure
truth); the sidecar holds *what's worth surfacing*.

The weight evolves by a Hebbian update rule:

```
w_ij(t) = w_ij(t_prev) · e^{-λ_ij · Δt} + α · clip(R, ±r_clip)
```

- **α (alpha)** — reinforcement gain: how much a use adds.
- **λ (lambda)** — decay rate per second: how fast an unused trace fades.
- **R** — the reinforcement signal (see §6, feedback).

### Lazy decay

Decay is never applied by a background sweep. Each edge stores only
`(w_last, t_last)`; the current weight at any time `t` is **one `exp` on read**:

```
w(t) = w_last · e^{-λ · Δt},   Δt = (t − t_last)/1000
```

Reads never mutate. Only reinforcement (`touch`) writes. Consequences: no global
tick, no time-series of weights to store or index, and writes happen *only* when
something is actually used. This is the single most important efficiency
property — it collapses what would otherwise be the heaviest write/index load.

### Authority → decay resistance

Not all relationships should fade equally. Each edge kind has an *authority* that
slows its decay:

```
λ_ij = λ_base / (1 + β · authority(kind))
```

High-authority kinds (e.g. IAM bindings, provenance) are remembered far longer
than noise (e.g. incidental log co-occurrence). Configured via
`authority_by_kind`.

## 3. Retrieval: weighted spreading activation

A query names **seed** nodes; activation spreads outward along edges **alive at
`t`** (the temporal filter), weighted by current salience, for `depth` hops.

Outflow uses a **saturating conductance**, not plain normalization:

```
out_mass    = Σ live effective weights out of u
conductance = out_mass / (1 + out_mass)          # in [0,1), grows with mass
outflow     = activation(u) · fan · conductance
delta(u→v)  = outflow · w(u→v) / out_mass         # routed by edge weight
```

Why conductance and not "divide by out_mass": pure normalization discards weight
*magnitude*, so decay would have no effect on total activated mass and the
homeostat (below) would have nothing to control. Conductance keeps magnitude in
the loop while still bounding outflow. Retrieval returns the total activated
**mass** and the **top-k** activated nodes.

## 4. Homeostasis

Reinforcement only ever adds; without regulation, total activation drifts and
saturates. A single integral controller keeps mean activated mass near a
**setpoint** by scaling every edge's decay through one global knob,
`lambda_scale`:

```
ewma_mass   = ewma_β·ewma_mass + (1−ewma_β)·mass         # smooth observed mass
integral   += (ewma_mass − setpoint)                     # clamped (anti-windup)
lambda_scale = clip(1 + k_i·integral, scale_min, scale_max)
```

Mass above setpoint → `lambda_scale` rises → everything forgets faster → mass
returns to band. One scalar, global, stable.

## 5. Salience vs. truth — a hard boundary

Two different things are deliberately kept separate:

- **Salience** (PsyRag owns it): decay, weighting, pruning. A pruned edge is
  masked from weighted recall but **the fact stays in the graph**. "We stopped
  surfacing this" ≠ "this stopped being true."
- **Truth** (the graph owns it): an edge existing or being retired. PsyRag only
  changes truth in one narrow case — a genuine contradiction on a *declared
  functional* predicate — and even then it only **proposes** `RetireEdge` ops for
  the caller to journal. Nothing is silently rewritten.

This is why decay/prune is a sidecar mask and supersession is native graph
mutation. Forgetting-for-retrieval and the-relationship-ended are different
events.

## 6. Feedback: where R comes from

Co-activation is not usefulness. Reinforcing everything that fired collapses into
rich-get-richer. So `R` comes from **downstream usage**, assigned to edges by an
**eligibility-trace** credit assignment:

1. A **traced** retrieval records the fired edges (in hop order) and each node's
   incoming activation.
2. The consumer acts; some surfaced nodes turn out **useful**.
3. Credit for a useful node is split among its incoming fired edges in proportion
   to the activation each delivered, and a `feedback_gamma` fraction propagates
   one hop upstream — so an entire *path* to a useful node gains salience, with
   geometric decay. Only edges that both fired *and* led somewhere useful are
   reinforced.
4. Per-source renormalization (see §7) makes it competitive: a winner's gain
   costs its siblings.

### Feedback modes

One mechanism, several ways to report usefulness (a `Credit` value):

| mode | you provide | fits |
|------|-------------|------|
| **explicit / graded** | named useful nodes, optional weights | RAG citations, click-through, thumbs |
| **contrastive** | mixed-sign node credits (+A, −B) | learning-to-rank |
| **episodic** | one reward scalar + spread policy | task success, incident resolved, bandit reward |
| **deferred** | a `trace_id` credited later | delayed / async outcomes |
| **unsupervised** | nothing (reinforce on retrieve) | "what's hot" baseline; can't tell useful from frequent |

Episodic reward works even though it's coarse: genuinely useful edges *recur*
across episodes while noise is random, so recurrence compounds under
reinforcement and random co-occurrence renorms away. Fine-grained credit learns
fast from few episodes; episodic learns slowly from many. Both converge.

**Causal caveat**: usage credit means "on a path to something useful," not
"causal." Keep edge kinds labelled `predicts` / `precedes`, never `causes`.

## 7. Consolidation (daytime, light)

A periodic batch pass over the sidecar:

1. **Materialize** lazy decay to now.
2. **Prune** (salience mask) edges whose live weight `< theta`. The fact remains.
3. **Renormalize** each source's live out-edges to an L1 budget (`norm_target`) —
   edges compete for fixed salience per node.
4. **Conflict detection** (truth, opt-in): for **declared functional** predicates
   only, a source with >1 open edge of that kind is a contradiction; the losers
   are returned as `RetireEdge` ops for the caller to journal. Multi-valued
   predicates (a service `CALLS` many things) are never flagged — functionality
   is domain knowledge you *declare* (`functional_kinds`), not structure inferred
   from co-occurrence.

## 8. Sleep (offline, heavy)

Consolidation restores order; **sleep** restores *capacity*. Modeled on the
synaptic homeostasis hypothesis:

1. **Downscale** every live weight multiplicatively by `sleep_downscale` (<1).
   Multiplicative, so relative structure — everything learned — is preserved,
   while absolute magnitudes reset and dynamic range is restored.
2. **Prune** below `sleep_theta` (more aggressive than daytime `theta`).
3. **Protect** the top `protect_top_frac` of edges by weight — "consolidated to
   long-term memory," exempt from the prune even if unused today. This is the
   anti-catastrophic-forgetting guard: a quiet day doesn't erase durable memories.
4. **Renormalize** per source.

In a tiered deployment (see architecture) sleep is also when the working graph's
learned deltas are flushed to long-term store, and the durable **trace log**
(the replay buffer) is replayed to settle any late-arriving deferred credit.

Sleep is a scheduled batch op (`psyrag sleep`, or `POST /sleep`) — deliberately
*not* on the retrieval path. Wire it to a nightly job.

## 9. Glossary

- **engram** — a memory trace; here, a plasticity-weighted edge.
- **salience** — an edge's current retrieval weight.
- **trace** — the record of one retrieval's fired edges, used for feedback.
- **sidecar** — the plasticity state, keyed by graph `EdgeId`.
- **homeostat** — the controller keeping activated mass near setpoint.
- **functional kind** — a single-valued predicate (only these are conflict-checked).
- **sleep** — offline downscale + protected prune + renorm + long-term flush.
