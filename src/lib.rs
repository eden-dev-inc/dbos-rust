//! Rust SDK surface for DBOS durable workflows.
//!
//! The crate intentionally keeps application-specific concerns out of the SDK so
//! the public API stays suitable for upstream DBOS review.
//!
//! Observability is collected by default through [`DbosObservability`], backed by
//! `fast-telemetry` counters, gauges, distributions, span collection, snapshots,
//! and Prometheus export.

// DBOS errors intentionally carry structured workflow, step, queue, and
// deduplication metadata for Go SDK parity.
#![allow(clippy::result_large_err)]

mod admin;
mod client;
mod conductor;
mod context;
mod debouncer;
mod error;
mod observability;
mod serialization;
mod store;
mod types;

pub use admin::*;
pub use client::*;
pub use conductor::*;
pub use context::*;
pub use debouncer::*;
pub use error::*;
pub use observability::DbosObservability;
pub use observability::DbosTelemetrySnapshot;
pub use serialization::*;
pub use store::{MemoryStore, SystemDatabase, SystemDatabaseHandle};
pub use types::*;
