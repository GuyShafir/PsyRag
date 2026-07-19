# Integrations

## Web console

`psyrag serve` hosts a self-contained console at `/` (no external assets). The
header holds the **bearer token** field (stored in the browser, sent on every
request — required when the server runs with `--token`/`--read-token`/
`--db-token`) and the **database picker** (multi-DB aware; with a db-scoped
token, type the database name directly). Tabs are deep-linkable (`/ui#trust`):

- **Dashboard** — live homeostat (λ-scale, setpoint, ewma mass, integral), edge
  liveness (total / live / dead), weight distribution, database size/traces,
  and the wedged flag. Auto-refreshes.
- **Graph** — force-directed view of the graph; edge width = current
  (trust-masked) weight, faded = retired. Click a node to make it a seed.
- **Retrieve** — seed search via `POST /match` (indexed token-prefix or
  substring), full retrieval controls (depth / fan / top-k / ts / adapt /
  trace / explain), the fired-edge **explain table**, and the trace subgraph
  visualization. Feedback in all three modes: mark-as-used, per-node graded
  scores, or episodic reward. Under a read-only token retrieval automatically
  drops `adapt`/`trace`.
- **Ingest** — paste-and-ingest entities with **origin** (provenance label),
  reconcile, and CAI-snapshot mode; direct edge reinforcement via `/touch`.
- **Maintenance** — consolidate (optionally journaling conflict
  supersessions), sleep, WAL checkpoint, and the **durable trace store**;
  click a stored trace id to re-visualize it.
- **Trust** — the `trust_by_origin` mask with one-click quarantine/restore,
  and the irreversible **purge-by-origin** (type-to-confirm; requires
  `--token` server-side).
- **Settings** — view and edit the database's full plasticity config
  (`GET`/`PUT /config`); changes apply live and persist to the DB's
  `config.json` in multi-DB mode.
- **Server** — database list with create/drop (type-to-confirm), per-DB
  state/size, uptime, memory budget, and request counters/latency by route
  (parsed from `/metrics`). Hidden for db-scoped tokens.

Suggested first run: ingest a small graph → Graph tab → click a node →
Retrieve → mark a result useful a few times → re-Retrieve and watch it climb.

## Python / Google ADK

The `python/` package makes PsyRag a drop-in **adaptive memory** for
[Google ADK](https://google.github.io/adk-docs/) agents. Full quickstart in
[python/README.md](../python/README.md); summary here.

### The client (`engrag_client.py`)

Zero-dependency async HTTP client (stdlib only) wrapping every endpoint:
`ingest`, `retrieve` (with `trace`), `feedback` (all modes), `match_nodes`,
`consolidate`, `stats`. Every other adapter builds on this.

### The memory service (`engrag_memory.py`)

`PsyRagMemoryService(BaseMemoryService)` — swap it into any ADK `Runner` and the
agent gets memory whose **recall learns from use**:

```python
from google.adk.agents import LlmAgent
from google.adk.tools import load_memory
from engrag_memory import PsyRagMemoryService, make_citation_feedback_callback

memory = PsyRagMemoryService("http://127.0.0.1:8080", top_k=8, depth=2)
agent = LlmAgent(
    model="gemini-flash-latest", name="assistant",
    instruction="Recall relevant facts with load_memory and name what you use.",
    tools=[load_memory],
    after_agent_callback=make_citation_feedback_callback(memory),  # closes the loop
)
```

- `add_session_to_memory` → turns a session into graph facts.
- `search_memory` → weighted spreading activation from query-matched seeds; returns
  `MemoryEntry` results and remembers the retrieval `trace_id`.
- `make_citation_feedback_callback` → after the agent answers, any recalled memory
  it grounded its answer in is reinforced automatically.

Two pluggable hooks fit both structured domains (feed domain events, seed on entity
names — exact) and conversational memory (extract entities from dialogue):
`extractor(session)` and `seed_selector(query)`.

vs. Vertex Memory Bank / RAG: those extract memories with an LLM but recall is
static — the same query returns the same memories forever. PsyRag makes **recall
itself adaptive**.

### Feedback adapters (`adapters.py`)

If you're not using the memory service, feed the layer directly. Each consumer
signal maps to a mode, ~5 lines over the client:

| consumer signal | adapter | mode |
|-----------------|---------|------|
| LLM cites retrieved sources | `rag_citation_feedback` | explicit |
| task / incident succeeded | `outcome_reward_feedback` | episodic |
| user clicked A over B | `click_preference_feedback` | contrastive |

All three run standalone against a live `psyrag serve`.
