---
name: Bug report
about: Something is broken or violates a stated guarantee
labels: bug
---

**Version / platform**

`psyrag --version` (or commit), OS/arch, and how it runs (bare binary,
Docker, standby, multi-DB).

**Reproduction**

Smallest sequence that triggers it — exact commands / curl calls, in order.
A failing `scripts/smoke.sh`-style snippet is ideal.

**Expected**

What should have happened (quote the doc/README claim if one exists).

**Actual**

What happened instead.

**Logs**

Relevant lines from the server's JSON log (and `psyrag verify` output if
it's a durability/data issue). Trim to the interesting part.
