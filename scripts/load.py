#!/usr/bin/env python3
"""Mixed-workload load driver for a running psyrag serve (stdlib only).

Seeds a corpus, then hammers the server from N threads with a realistic mix
(80% retrieve, 15% feedback, 5% ingest) for the given duration. Prints a
JSON summary; the caller (load.sh) asserts SLOs from the server's own
Prometheus histograms.

Usage: load.py URL SECONDS THREADS
"""
import concurrent.futures
import json
import random
import sys
import threading
import time
import urllib.request

URL, SECONDS, THREADS = sys.argv[1], float(sys.argv[2]), int(sys.argv[3])
CORPUS = 300

counts: dict = {}
lock = threading.Lock()


def post(path: str, body: dict, timeout: float = 10.0) -> int:
    req = urllib.request.Request(
        URL + path,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0  # network-level failure


def seed() -> None:
    ents = []
    for i in range(CORPUS):
        edges = [
            {"dst": f"svc/n{random.randrange(CORPUS)}", "kind": f"K{random.randrange(4)}"}
            for _ in range(random.randrange(1, 5))
        ]
        ents.append({"name": f"svc/n{i}", "type": "t", "props": {"i": i}, "edges": edges})
    code = post("/ingest", {"json": json.dumps(ents), "ts": 1000})
    assert code == 200, f"seed ingest failed: {code}"


def worker(deadline: float, wid: int) -> None:
    rng = random.Random(wid)
    i = 0
    while time.monotonic() < deadline:
        i += 1
        roll = rng.random()
        seedname = f"svc/n{rng.randrange(CORPUS)}"
        if roll < 0.80:
            code = post("/retrieve", {"seeds": [seedname], "adapt": False, "top_k": 10})
            kind = "retrieve"
        elif roll < 0.95:
            code = post("/feedback", {"seeds": [seedname], "used": [f"svc/n{rng.randrange(CORPUS)}"]})
            kind = "feedback"
        else:
            name = f"load/w{wid}i{i}"
            code = post("/ingest", {"json": json.dumps([{"name": name, "type": "t",
                        "edges": [{"dst": seedname, "kind": "REF"}]}])})
            kind = "ingest"
        with lock:
            counts[(kind, code)] = counts.get((kind, code), 0) + 1


def main() -> None:
    seed()
    deadline = time.monotonic() + SECONDS
    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as ex:
        list(ex.map(lambda w: worker(deadline, w), range(THREADS)))
    elapsed = time.monotonic() - start
    total = sum(counts.values())
    by_status: dict = {}
    for (kind, code), n in counts.items():
        by_status.setdefault(str(code), 0)
        by_status[str(code)] += n
    print(json.dumps({
        "total": total,
        "rps": round(total / elapsed, 1),
        "by_status": by_status,
        "by_kind": {f"{k}:{c}": n for (k, c), n in sorted(counts.items())},
    }))


if __name__ == "__main__":
    main()
