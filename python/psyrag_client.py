"""Thin, dependency-free async client for a running `psyrag serve`.

Uses only the stdlib (urllib + asyncio.to_thread), so it installs anywhere with
no pip tree. Every ADK adapter in this package talks to the Rust service through
this client.
"""
from __future__ import annotations

import asyncio
import json
import urllib.request
from typing import Any, Optional


class PsyRagClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8080", timeout: float = 5.0,
                 *, db: Optional[str] = None, token: Optional[str] = None):
        """`db` addresses a named database on a multi-DB server (routes become
        `/db/{db}/...`); omit it for the default database. `token` is sent as
        `Authorization: Bearer` when the server runs with --token."""
        self._root = base_url.rstrip("/")
        self.base = self._root
        if db:
            self.base += f"/db/{db}"
        self.timeout = timeout
        self.token = token

    # -- low-level ----------------------------------------------------------
    def _headers(self) -> dict:
        h = {"Content-Type": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def _post_sync(self, path: str, body: dict) -> dict:
        req = urllib.request.Request(
            self.base + path,
            data=json.dumps(body).encode(),
            headers=self._headers(),
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return json.loads(r.read().decode())

    def _get_sync(self, path: str) -> dict:
        req = urllib.request.Request(self.base + path, headers=self._headers())
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return json.loads(r.read().decode())

    async def _post(self, path: str, body: dict) -> dict:
        return await asyncio.to_thread(self._post_sync, path, body)

    async def _get(self, path: str) -> dict:
        return await asyncio.to_thread(self._get_sync, path)

    # -- API ----------------------------------------------------------------
    async def health(self) -> dict:
        return await self._get("/health")

    async def stats(self) -> dict:
        return await self._get("/stats")

    async def ingest(self, entities_json: str, ts: Optional[int] = None,
                     reconcile: bool = False, cai: bool = False) -> dict:
        body: dict[str, Any] = {"json": entities_json, "reconcile": reconcile, "cai": cai}
        if ts is not None:
            body["ts"] = ts
        return await self._post("/ingest", body)

    async def retrieve(self, seeds: list[str], depth: Optional[int] = None,
                       top_k: int = 10, ts: Optional[int] = None,
                       adapt: bool = False, trace: bool = False) -> dict:
        body: dict[str, Any] = {"seeds": seeds, "top_k": top_k, "adapt": adapt, "trace": trace}
        if depth is not None:
            body["depth"] = depth
        if ts is not None:
            body["ts"] = ts
        return await self._post("/retrieve", body)

    async def feedback(self, *, seeds: Optional[list[str]] = None,
                       trace_id: Optional[int] = None,
                       used: Optional[list[str]] = None,
                       nodes: Optional[list[tuple[str, float]]] = None,
                       reward: Optional[float] = None, spread: Optional[str] = None,
                       depth: Optional[int] = None, top_k: int = 10,
                       ts: Optional[int] = None) -> dict:
        """Apply feedback in any mode. Provide EITHER trace_id (deferred) OR seeds
        (stateless), plus one credit spec: used / nodes / reward(+spread)."""
        body: dict[str, Any] = {"top_k": top_k}
        if trace_id is not None:
            body["trace_id"] = trace_id
        if seeds is not None:
            body["seeds"] = seeds
        if used is not None:
            body["used"] = used
        if nodes is not None:
            body["nodes"] = [[n, float(s)] for n, s in nodes]
        if reward is not None:
            body["reward"] = reward
        if spread is not None:
            body["spread"] = spread
        if depth is not None:
            body["depth"] = depth
        if ts is not None:
            body["ts"] = ts
        return await self._post("/feedback", body)

    async def consolidate(self, ts: Optional[int] = None,
                          apply_conflicts: bool = False) -> dict:
        body: dict[str, Any] = {"apply_conflicts": apply_conflicts}
        if ts is not None:
            body["ts"] = ts
        return await self._post("/consolidate", body)

    async def match_nodes(self, tokens: list[str], limit: int = 16) -> list[str]:
        """Resolve free-text tokens to existing node names (substring, case-insensitive)."""
        r = await self._post("/match", {"tokens": tokens, "limit": limit})
        return r.get("nodes", [])

    # -- multi-db admin (server-level routes, independent of this client's db) --
    async def create_db(self, name: str) -> dict:
        """Create (or ensure) a named database on a multi-DB server."""
        def _sync() -> dict:
            req = urllib.request.Request(
                f"{self._root}/db/{name}", headers=self._headers(), method="POST"
            )
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return json.loads(r.read().decode())
        return await asyncio.to_thread(_sync)

    async def list_dbs(self) -> list[dict]:
        """List databases on the server (name, open state, sizes)."""
        def _sync() -> dict:
            req = urllib.request.Request(f"{self._root}/dbs", headers=self._headers())
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return json.loads(r.read().decode())
        return (await asyncio.to_thread(_sync)).get("dbs", [])
