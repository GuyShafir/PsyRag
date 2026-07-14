//! # psyrag-graph
//!
//! Embeddable temporal typed property graph for any inventory of typed,
//! named entities — cloud resources, Kubernetes objects, CMDB records,
//! IoT fleets, service catalogs.
//!
//! - **Domain-agnostic core**: nodes are (name, type, props), edges are
//!   (src, dst, kind). Adapters translate domain records into ops; the
//!   generic entity JSON format works for anything.
//! - **Append-only**: every node version and edge carries an
//!   `[observed_at, retired_at)` interval; history is never destroyed.
//! - **Diffable**: `diff(t1, t2)` answers "what changed?" natively.
//! - **Blast-radius aware**: cycle-safe temporal BFS that returns the
//!   traversal *path* for each reached node, built for LLM prompt injection.
//! - **Snapshot-is-truth reconciliation**: unasserted nodes AND unasserted
//!   edges of re-observed sources are retired — no zombies, no stale wiring.
//! - **No LLM in the ingest path**: relationships in deterministic domains
//!   are extracted from the payload structure, never guessed.
//!
//! The GCP Cloud Asset Inventory adapter ships behind the `gcp` feature
//! (default on); disable default features for a pure domain-agnostic core.

pub mod entity;
#[cfg(feature = "gcp")]
pub mod gcp;
pub mod graph;
pub mod persist;
pub mod snapshot;

pub use graph::{Direction, GraphDiff, Op, Reach, TemporalGraph, Ts, T_MAX};
pub use persist::PersistentGraph;
pub use snapshot::{ingest_snapshot_ops, Batch, OpSink};
