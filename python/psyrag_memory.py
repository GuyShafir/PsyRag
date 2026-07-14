"""PsyRagMemoryService — an ADK ``BaseMemoryService`` whose *recall learns
from use*.

Standard ADK memory (Vertex Memory Bank, RAG) does static similarity search:
the same query always returns the same memories, no matter which ones actually
helped. This service is backed by PsyRag, so:

* ``add_session_to_memory`` turns a session into graph facts (nodes + edges).
* ``search_memory`` runs weighted spreading activation from query-matched seed
  nodes and returns the most *salient* subgraph — and remembers the retrieval
  ``trace_id``.
* When those recalled memories turn out useful (the agent cited them, or the
  task succeeded), you call ``record_use`` / ``record_reward``; credit flows back
  along the retrieval paths and reinforces them. Memories that keep getting used
  stay salient; memories surfaced-but-ignored decay.

It talks to a running ``psyrag serve`` over HTTP via ``PsyRagClient`` — the Rust core is
a sidecar, this is the idiomatic ADK adapter. Drop it into any Runner:

    from google.adk.runners import Runner
    memory = PsyRagMemoryService("http://127.0.0.1:8080")
    runner = Runner(agent=agent, app_name="app", session_service=ss,
                    memory_service=memory)

Two hooks are pluggable so this fits both structured domains (cloud inventory,
CMDB — the agent's tools already emit a graph) and conversational memory
(entities extracted from dialogue):

* ``extractor(session) -> entities_json`` — how a session becomes graph ops.
* ``seed_selector(query, client) -> [seeds]`` — how a query picks seed nodes.

Defaults are provided for both; production users supply domain-specific ones.
"""
from __future__ import annotations

import re
import time
from typing import Awaitable, Callable, Optional

from google.adk.memory.base_memory_service import BaseMemoryService, SearchMemoryResponse
from google.adk.memory.memory_entry import MemoryEntry
from google.genai import types

from psyrag_client import PsyRagClient

_WORD = re.compile(r"[A-Za-z0-9_./-]{3,}")
_STOP = {"the", "and", "for", "what", "did", "was", "our", "about", "with", "that",
         "this", "have", "has", "are", "you", "your", "how", "why", "when"}


def _now_ms() -> int:
    return int(time.time() * 1000)


def default_extractor(session, max_terms: int = 16) -> str:
    """A minimal, domain-agnostic session->graph extractor. Pulls salient tokens
    from user/model text and links **co-occurring terms** to each other
    (``CO_OCCURS``), plus a session node for provenance (``MENTIONS``). The
    co-occurrence edges are what make recall work: seeding on one query term
    spreads to the terms it was mentioned alongside. Crude on purpose — real
    deployments plug in NER / an LLM extractor, or feed structured domain events
    directly. Returns driftgraph entity JSON (array)."""
    import json

    sid = getattr(session, "id", None) or getattr(session, "session_id", "session")
    terms: list[str] = []
    seen: set[str] = set()
    for ev in getattr(session, "events", []) or []:
        content = getattr(ev, "content", None)
        for p in getattr(content, "parts", None) or []:
            text = getattr(p, "text", None)
            if not text:
                continue
            for tok in _WORD.findall(text.lower()):
                if tok in _STOP or tok in seen:
                    continue
                seen.add(tok)
                terms.append(f"term/{tok}")
                if len(terms) >= max_terms:
                    break
    entities = []
    # session node links every term (provenance)
    entities.append({
        "name": f"session/{sid}", "type": "adk/Session",
        "edges": [{"dst": t, "kind": "MENTIONS"} for t in terms],
    })
    # each term co-occurs with the others (bounded clique)
    for i, t in enumerate(terms):
        edges = [{"dst": u, "kind": "CO_OCCURS"} for j, u in enumerate(terms) if j != i]
        entities.append({"name": t, "type": "adk/Term", "edges": edges})
    return json.dumps(entities)


def default_seed_selector(query: str) -> list[str]:
    """Tokenize a natural-language query into candidate match tokens."""
    return [t for t in _WORD.findall(query.lower()) if t not in _STOP]


class PsyRagMemoryService(BaseMemoryService):
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8080",
        *,
        top_k: int = 8,
        depth: Optional[int] = None,
        extractor: Callable[[object], str] = default_extractor,
        seed_selector: Callable[[str], list[str]] = default_seed_selector,
        reconcile: bool = False,
    ):
        super().__init__()
        self.client = PsyRagClient(base_url)
        self.top_k = top_k
        self.depth = depth
        self.extractor = extractor
        self.seed_selector = seed_selector
        self.reconcile = reconcile
        # (app,user,query) -> (trace_id, surfaced_node_names) for later feedback
        self._pending: dict[tuple[str, str, str], tuple[int, list[str]]] = {}

    def is_enabled(self) -> bool:
        return True

    # -- ingest -------------------------------------------------------------
    async def add_session_to_memory(self, session) -> None:
        entities_json = self.extractor(session)
        if not entities_json:
            return
        await self.client.ingest(entities_json, ts=_now_ms(), reconcile=self.reconcile)

    # ADK's incremental-ingest hook; treat the same for our append-only store.
    async def add_events_to_memory(self, *, app_name: str, user_id: str,
                                   session_id: str, events) -> None:
        class _S:
            pass
        s = _S()
        s.id = session_id
        s.events = list(events)
        await self.add_session_to_memory(s)

    # -- recall -------------------------------------------------------------
    async def search_memory(self, *, app_name: str, user_id: str,
                            query: str) -> SearchMemoryResponse:
        tokens = self.seed_selector(query)
        if not tokens:
            return SearchMemoryResponse(memories=[])
        seeds = await self.client.match_nodes(tokens, limit=16)
        if not seeds:
            return SearchMemoryResponse(memories=[])
        res = await self.client.retrieve(
            seeds, depth=self.depth, top_k=self.top_k + len(seeds), ts=_now_ms(), trace=True
        )
        result = res.get("result", res)
        trace_id = res.get("trace_id")
        top = result.get("top", [])
        seed_set = set(seeds)
        # return memories *related to* the query, not the matched query terms echoed
        related = [t for t in top if t["node"] not in seed_set][: self.top_k]
        surfaced = [t["node"] for t in related]
        if trace_id is not None:
            self._pending[(app_name, user_id, query)] = (trace_id, surfaced)

        memories = []
        for t in related:
            text = f"{t['node']} [{t.get('node_type','')}] (salience {t.get('activation',0):.3f})"
            memories.append(MemoryEntry(
                content=types.Content(parts=[types.Part(text=text)], role="model"),
                author="driftgraph",
                timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            ))
        return SearchMemoryResponse(memories=memories)

    # -- learning -----------------------------------------------------------
    async def record_use(self, *, app_name: str, user_id: str, query: str,
                         used_nodes: list[str]) -> Optional[dict]:
        """Positive feedback: these recalled nodes were actually useful. Credits
        the stored retrieval trace (deferred) so paths to them gain salience."""
        key = (app_name, user_id, query)
        pend = self._pending.get(key)
        if not pend or not used_nodes:
            return None
        trace_id, _surfaced = pend
        return await self.client.feedback(trace_id=trace_id, used=used_nodes)

    async def record_reward(self, *, app_name: str, user_id: str, query: str,
                            reward: float, spread: str = "by_activation") -> Optional[dict]:
        """Episodic feedback: one scalar for whether the retrieval helped the task,
        spread over the surfaced memories."""
        key = (app_name, user_id, query)
        pend = self._pending.get(key)
        if not pend:
            return None
        trace_id, _surfaced = pend
        return await self.client.feedback(trace_id=trace_id, reward=reward, spread=spread)

    def surfaced_for(self, app_name: str, user_id: str, query: str) -> list[str]:
        pend = self._pending.get((app_name, user_id, query))
        return list(pend[1]) if pend else []


def make_citation_feedback_callback(memory: PsyRagMemoryService):
    """An ADK ``after_agent_callback`` that closes the loop automatically: after
    the agent answers, any surfaced memory whose node name appears in the final
    response is treated as *used*, and positive feedback is applied. This turns
    "the agent grounded its answer in this memory" into the learning signal with
    no extra human step.

    Usage:
        agent = LlmAgent(..., after_agent_callback=make_citation_feedback_callback(mem))
    """
    async def _cb(callback_context) -> None:
        try:
            app_name = getattr(callback_context, "app_name", "app")
            user_id = getattr(callback_context, "user_id", "user")
            # best-effort: last user query + final response text from context
            query = getattr(callback_context, "user_content", None)
            query_text = ""
            if query is not None:
                for p in getattr(query, "parts", []) or []:
                    query_text += getattr(p, "text", "") or ""
            surfaced = memory.surfaced_for(app_name, user_id, query_text)
            if not surfaced:
                return
            # inspect the produced output for cited node names
            out = ""
            for p in getattr(getattr(callback_context, "content", None), "parts", []) or []:
                out += getattr(p, "text", "") or ""
            used = [n for n in surfaced if n.lower() in out.lower()]
            if used:
                await memory.record_use(app_name=app_name, user_id=user_id,
                                        query=query_text, used_nodes=used)
        except Exception:
            # feedback is best-effort; never break the agent turn on it
            return
    return _cb
