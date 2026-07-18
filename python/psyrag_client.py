"""Thin, dependency-free async client for a running `psyrag serve`.

Uses only the stdlib (urllib + asyncio.to_thread), so it installs anywhere with
no pip tree. Every ADK adapter in this package talks to the Rust service through
this client.
"""
from __future__ import annotations

import asyncio
import json
import time
import urllib.error
import urllib.request
import uuid
from typing import Any, Optional


class PsyRagClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8080", timeout: float = 5.0,
                 *, db: Optional[str] = None, token: Optional[str] = None,
                 retries: int = 2, retry_backoff: float = 0.25):
        """`db` addresses a named database on a multi-DB server (routes become
        `/db/{db}/...`); omit it for the default database. `token` is sent as
        `Authorization: Bearer` when the server runs with --token.

        Mutating calls (ingest/feedback/consolidate) automatically carry an
        `Idempotency-Key` and are retried up to `retries` times on network
        errors / 5xx **with the same key**, so an at-least-once retry can
        never double-apply (the server replays the original response)."""
        self._root = base_url.rstrip("/")
        self.base = self._root
        if db:
            self.base += f"/db/{db}"
        self.timeout = timeout
        self.token = token
        self.retries = retries
        self.retry_backoff = retry_backoff

    # -- low-level ----------------------------------------------------------
    def _headers(self) -> dict:
        h = {"Content-Type": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def _post_sync(self, path: str, body: dict,
                   idem_key: Optional[str] = None) -> dict:
        headers = self._headers()
        if idem_key:
            headers["Idempotency-Key"] = idem_key
        data = json.dumps(body).encode()
        attempts = (self.retries + 1) if idem_key else 1
        last: Exception = RuntimeError("unreachable")
        for i in range(attempts):
            try:
                req = urllib.request.Request(
                    self.base + path, data=data, headers=headers, method="POST",
                )
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    return json.loads(r.read().decode())
            except urllib.error.HTTPError as e:
                if e.code < 500:
                    raise  # 4xx is final; replaying it is the server's job
                last = e
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                last = e
            if i < attempts - 1:
                time.sleep(self.retry_backoff * (2 ** i))
        raise last

    async def _post_idem(self, path: str, body: dict,
                         idem_key: Optional[str] = None) -> dict:
        key = idem_key or uuid.uuid4().hex
        return await asyncio.to_thread(self._post_sync, path, body, key)

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
                     reconcile: bool = False, cai: bool = False,
                     origin: Optional[str] = None) -> dict:
        """`origin` labels every fact in the batch with its provenance
        (conventions like "user:alice/session:42" enable trust levels and
        purge-by-subject); a per-entity `origin` in the payload overrides."""
        body: dict[str, Any] = {"json": entities_json, "reconcile": reconcile, "cai": cai}
        if ts is not None:
            body["ts"] = ts
        if origin is not None:
            body["origin"] = origin
        return await self._post_idem("/ingest", body)

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
        return await self._post_idem("/feedback", body)

    async def consolidate(self, ts: Optional[int] = None,
                          apply_conflicts: bool = False) -> dict:
        body: dict[str, Any] = {"apply_conflicts": apply_conflicts}
        if ts is not None:
            body["ts"] = ts
        return await self._post_idem("/consolidate", body)

    async def match_nodes(self, tokens: list[str], limit: int = 16) -> list[str]:
        """Resolve free-text tokens to existing node names (substring, case-insensitive)."""
        r = await self._post("/match", {"tokens": tokens, "limit": limit})
        return r.get("nodes", [])

    async def quarantine(self, origin_prefix: str, trust: float = 0.0) -> dict:
        """Set the trust level for a provenance prefix. 0.0 removes the
        source's influence from retrieval entirely (a mask — learned weights
        are untouched); 1.0 restores it."""
        return await self._post("/quarantine",
                                {"origin_prefix": origin_prefix, "trust": trust})

    async def purge(self, origin_prefix: str) -> dict:
        """Irreversibly delete every fact whose provenance matches the prefix
        (GDPR deletion-by-subject). Requires a server running with --token."""
        return await self._post_idem("/purge", {"origin_prefix": origin_prefix})

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
