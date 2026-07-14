"""Copy-paste ADK agent using the adaptive PsyRagMemoryService.

Prereqs:
  1. `psyrag serve --addr 127.0.0.1:8080` running (the Rust core).
  2. `pip install google-adk` and a Gemini/Vertex credential.

What you get over stock ADK memory: recall that *learns from use*. The
`load_memory` tool lets the agent pull long-term memories; the
`make_citation_feedback_callback` closes the loop — any recalled memory the agent
grounds its answer in is reinforced, so it surfaces more readily next time.

Run:  adk web   (or use a Runner directly, as in __main__ below)
"""
from __future__ import annotations

from google.adk.agents import LlmAgent
from google.adk.tools import load_memory

from psyrag_memory import PsyRagMemoryService, make_citation_feedback_callback

# The adaptive memory service (Rust core over HTTP).
memory_service = PsyRagMemoryService("http://127.0.0.1:8080", top_k=8, depth=2)

root_agent = LlmAgent(
    model="gemini-flash-latest",
    name="adaptive_memory_agent",
    instruction=(
        "You help the user with their ongoing work. Use the load_memory tool to "
        "recall relevant facts from past sessions before answering, and ground "
        "your answer in the specific items you recall (name them). If nothing "
        "relevant comes back, answer from the current conversation."
    ),
    tools=[load_memory],
    # Closes the learning loop automatically: recalled memories that end up in the
    # answer are reinforced.
    after_agent_callback=make_citation_feedback_callback(memory_service),
)


if __name__ == "__main__":
    # Minimal Runner harness (no `adk web`). Ingest a past session, then ask.
    import asyncio
    from google.adk.runners import Runner
    from google.adk.sessions import InMemorySessionService
    from google.genai import types

    APP, USER = "adaptive_demo", "u1"

    async def main():
        ss = InMemorySessionService()
        runner = Runner(agent=root_agent, app_name=APP,
                        session_service=ss, memory_service=memory_service)

        # 1) a prior session, committed to long-term memory
        s1 = await ss.create_session(app_name=APP, user_id=USER, session_id="past")
        async for _ in runner.run_async(
            user_id=USER, session_id="past",
            new_message=types.Content(role="user", parts=[types.Part(
                text="For Project Alpha we decided the pricing model is seat-based "
                     "with usage metering for overage.")])):
            pass
        await memory_service.add_session_to_memory(
            await ss.get_session(app_name=APP, user_id=USER, session_id="past"))

        # 2) a new session that should recall it
        await ss.create_session(app_name=APP, user_id=USER, session_id="now")
        async for ev in runner.run_async(
            user_id=USER, session_id="now",
            new_message=types.Content(role="user", parts=[types.Part(
                text="Remind me what we decided about pricing for Alpha?")])):
            if ev.is_final_response():
                print("agent:", ev.content.parts[0].text)

    asyncio.run(main())
