"""Standalone feedback adapters — no ADK required, just a running `psyrag serve`.

Each shows how to turn a real consumer's behavior into a plasticity feedback
signal. Run any of them directly (they self-check against the service):

    psyrag serve --addr 127.0.0.1:8080 &
    python adapters.py
"""
from __future__ import annotations

import asyncio
import re
from typing import Iterable

from psyrag_client import PsyRagClient


# ---------------------------------------------------------------------------
# 1) RAG citations -> explicit/graded feedback
#    The LLM's answer cites some of the retrieved nodes. Cited = used.
# ---------------------------------------------------------------------------
async def rag_citation_feedback(client: PsyRagClient, seeds: list[str],
                                answer_text: str, candidates: list[dict],
                                trace_id: int) -> dict:
    """`candidates` are the nodes returned by a traced retrieval; `answer_text`
    is the LLM output. Any candidate whose name appears in the answer is credited.
    A node that was surfaced but not cited is left alone (decays naturally)."""
    cited = [c["node"] for c in candidates if c["node"].lower() in answer_text.lower()]
    if not cited:
        return {"skipped": "no citations"}
    return await client.feedback(trace_id=trace_id, used=cited)


# ---------------------------------------------------------------------------
# 2) Agent/task outcome -> episodic reward (the bandit case)
#    You only know if the whole episode worked. One scalar, spread over surfaced.
# ---------------------------------------------------------------------------
async def outcome_reward_feedback(client: PsyRagClient, seeds: list[str],
                                  success: bool, trace_id: int) -> dict:
    reward = 1.0 if success else -0.25  # mild negative on failure
    return await client.feedback(trace_id=trace_id, reward=reward,
                                 spread="by_activation")


# ---------------------------------------------------------------------------
# 3) Contrastive click -> preference (learning-to-rank)
#    User picked A over the also-shown B.
# ---------------------------------------------------------------------------
async def click_preference_feedback(client: PsyRagClient, chosen: str,
                                    passed_over: Iterable[str], trace_id: int) -> dict:
    nodes = [(chosen, 1.0)] + [(p, -1.0) for p in passed_over]
    return await client.feedback(trace_id=trace_id, nodes=nodes)


async def _demo():
    c = PsyRagClient("http://127.0.0.1:8080")
    inv = ('[{"name":"api","type":"svc","edges":['
           '{"dst":"db","kind":"CALLS"},{"dst":"cache","kind":"CALLS"},'
           '{"dst":"queue","kind":"CALLS"}]}]')
    await c.ingest(inv, ts=1000)
    r = await c.retrieve(["api"], depth=1, top_k=5, ts=2000, trace=True)
    tid, cands = r["trace_id"], r["result"]["top"]
    print("candidates:", [x["node"] for x in cands])

    print("RAG citation:", await rag_citation_feedback(
        c, ["api"], "The db holds the records.", cands, tid))
    print("outcome reward:", await outcome_reward_feedback(c, ["api"], True, tid))
    print("click preference:", await click_preference_feedback(c, "db", ["cache"], tid))


if __name__ == "__main__":
    asyncio.run(_demo())
