# PsyRag — Python / ADK integration

Adaptive graph memory for [Google ADK](https://google.github.io/adk-docs/) agents.
The Rust core (`psyrag serve`) is a sidecar; this package is the idiomatic Python
glue: a drop-in `BaseMemoryService`, feedback adapters, and a thin client.

## Why this exists

ADK's memory services (Vertex Memory Bank, RAG) do **static** retrieval — the
same query returns the same memories forever, regardless of which ones actually
helped. `PsyRagMemoryService` makes **recall itself adaptive**: a memory that
keeps getting used stays salient; a memory that's surfaced-but-ignored decays.
It's the same idea as long-term potentiation in a brain, applied to retrieval.

Same query "pricing", before and after "metering" proved useful 20×:

```
recall #1 :  model 0.112   uses 0.112   seats 0.112   metering 0.112   alpha 0.112
recall #N :  metering 0.270   model 0.090   uses 0.090   seats 0.090   alpha 0.090
```

Recall reordered itself by learned usefulness. No re-embedding, no re-indexing.

## Install / run

```bash
# 1. the Rust core (build once; see ../README.md)
psyrag serve --addr 127.0.0.1:8080

# 2. this package
pip install google-adk          # for the ADK integration
# engrag_client.py has zero deps (stdlib only)
```

## Drop-in memory service

```python
from google.adk.agents import LlmAgent
from google.adk.tools import load_memory
from google.adk.runners import Runner
from engrag_memory import PsyRagMemoryService, make_citation_feedback_callback

memory = PsyRagMemoryService("http://127.0.0.1:8080", top_k=8, depth=2)

agent = LlmAgent(
    model="gemini-flash-latest", name="assistant",
    instruction="Recall relevant facts with load_memory and name what you use.",
    tools=[load_memory],
    after_agent_callback=make_citation_feedback_callback(memory),  # closes the loop
)
runner = Runner(agent=agent, app_name="app", session_service=ss, memory_service=memory)
```

`add_session_to_memory` turns a session into graph facts; `search_memory` runs
weighted spreading activation from query-matched seeds; the callback reinforces
whichever recalled memories the agent grounded its answer in. See
`agent_example.py` for a full runnable harness.

### Two pluggable hooks

The service fits both structured and conversational domains:

- `extractor(session) -> entities_json` — how a session becomes graph ops. The
  default pulls salient tokens and links co-occurring terms; production plugs in
  NER / an LLM extractor, **or** skips it entirely by feeding structured domain
  events (the strong case — see below).
- `seed_selector(query) -> [tokens]` — how a query picks seed nodes. Default is
  token match against node names via the service's `/match`.

## Feedback modes (adapters.py)

If you're not using the memory service, feed the layer directly. Every consumer
signal maps to a mode:

| Consumer signal | Adapter | Mode |
|-----------------|---------|------|
| LLM cites retrieved sources | `rag_citation_feedback` | explicit |
| Task/incident succeeded or not | `outcome_reward_feedback` | episodic |
| User clicked A over B | `click_preference_feedback` | contrastive |

All three are ~5 lines over `PsyRagClient` and run standalone against `psyrag serve`.

## Example: adaptive blast-radius over Cloud Asset Inventory

psyrag-graph already ingests GCP Cloud Asset Inventory (`psyrag ingest --cai`) into a
temporal typed property graph — projects, networks, IAM, PSC, the works. Blast
radius ("what's downstream of this VPC?") is a graph traversal today, and it
treats every edge as equally important.

Plasticity makes it **learn which dependency paths actually matter**:

1. Ingest CAI snapshots → the live dependency graph.
2. During an incident, an SRE agent retrieves the blast radius of the failing
   resource (`search_memory` / `/retrieve`) — here the *seeds are resources*, no
   text extraction needed, so the structured path is exact.
3. When the incident resolves, the paths that led to the actual root cause /
   affected services get episodic reward (`outcome_reward_feedback`).
4. Over many incidents, the graph specializes: the dependency edges that
   repeatedly matter during real outages become salient; incidental wiring
   decays out of the retrieved blast radius.

The result is an SRE memory that gets sharper with every incident — built on GCP
primitives (CAI, Vertex/Gemini agents via ADK), self-hostable, with no vendor lock
on the memory layer itself.

## Honest caveats

- **Extraction quality** is the user's problem, same as Memory Bank (which uses an
  LLM). The distinction here is adaptive *recall*, not extraction. The structured
  path (feed domain events, seed on entity names) sidesteps it entirely.
- **Causal caveat**: usage credit means "on a path to something useful," not
  "causal." Keep edge kinds `predicts`/`precedes`, never `causes`.
- **Deferred trace store is in-memory**: pending traces are lost if `psyrag serve`
  restarts. Fine for online feedback; durable deferred credit is future work.
