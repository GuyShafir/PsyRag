---
name: Feature request
about: Propose new behavior
labels: enhancement
---

**Problem**

What you can't do today, and the real situation where it bites. Problem
first — a good problem statement often gets a better solution than the one
proposed.

**Proposal**

How you'd solve it: surface (CLI flag, HTTP endpoint, config), rough
behavior, what stays compatible.

**Dependencies**

PsyRag's runtime deps are frozen at `serde`/`serde_json`/`tiny_http`
(see CONTRIBUTING.md). If the proposal needs a new crate, name it and make
the case — or note how it works without one.
