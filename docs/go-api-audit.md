# DBOS Go API Parity Audit

Audit date: 2026-06-17.

Last updated: 2026-06-18.

Upstream snapshot audited from `dbos-inc/dbos-transact-golang` `main`:

- `dbos.go`
- `workflow.go`
- `client.go`
- `queue.go`
- `scheduler.go`
- `admin_server.go`
- `conductor.go`
- `conductor_protocol.go`
- `dbq.go`
- `debouncer.go`
- `errors.go`
- `recovery.go`
- `serialization.go`
- `system_database.go`
- `sqlite_pool.go`
- `sqlite_migrations.go`
- `dialect.go`

## Completed In This Audit

- Added `WorkflowHandle::get_result_with_options` and `GetResultOptions` for Go's handle timeout and polling interval options.
- Added persisted `WorkflowQueue` getters and setters for global concurrency, worker concurrency, rate limit, priority, partition mode, and polling interval.
- Added `DbosContext::update_queue` and `DbosClient::update_queue` for whole-queue updates.
- Expanded `ListWorkflowsOptions` to cover Go's multi-value and lifecycle filters: names, users, application versions, queue names, executor IDs, fork/parent IDs, deduplication IDs, completion/dequeue ranges, `was_forked_from`, and `has_parent`.
- Honored `load_input` and `load_output` in workflow listing results.
- Added registry inspection with `WorkflowRegistryEntry`, `ListRegisteredWorkflowsOptions`, `list_registered_workflows`, and deprecated `list_registered_queues` compatibility.
- Added `GetWorkflowStepsOptions` with output loading, limit, and offset controls.
- Added `ReadStreamOptions::snapshot_from_offset` and `read_stream_with_options`.
- Added `StepOptions` and `run_as_step_with_options` for retries, backoff, retry predicates, and deterministic next-step IDs.
- Extended the admin list-workflows query adapter to accept the expanded list filters.
- Added API-surface tests covering these additions.
- Closed the 2026-06-17 remaining parity list with:
  - `SystemDatabaseHandle` for injected stores plus real Turso state persistence behind the `turso` feature.
  - `DbosConfig::with_serializer` runtime wiring across workflow inputs/results, steps, messages, events, and streams.
  - `SendOptions`, `SetEventOptions`, and `WriteStreamOptions` for per-call portable communication.
  - `GetWorkflowAggregatesInput` and `GetStepAggregatesInput` with grouping filters and time buckets.
  - `ExportWorkflowOptions` plus event/message/stream/child workflow export/import.
  - `WorkflowQueue::max_polling_interval` and persisted setter support.
  - `TransactionOptions` and `TransactionIsolationLevel`.
  - Rust context helpers for values, deadlines, cancel handles, cancel causes, and `without_cancel`.
  - Default `DbosObservability` backed by `fast-telemetry` counters, distributions, gauges, span collection, snapshots, and Prometheus export.

## Working Coverage

- Core context lifecycle: `DbosConfig`, `DbosContext::new`, `launch`, `shutdown`, app version, executor ID, application ID, patching flag, admin/conductor config, injected system database handles, Postgres URLs, Turso paths/URLs, custom serializers, and default observability handles.
- Workflow execution and management: registration, run/retrieve, result/status, cancel, resume, delete, fork, delay, child parent metadata, authentication metadata, deadlines, priorities, deterministic step validation, workflow and step inspection.
- Runtime primitives: durable steps, transaction-backed steps, sleep, async step spawn/select equivalents, current workflow/step IDs, retryable step options.
- Queue primitives: database-backed queue registration/retrieval/list/delete/update, queue setter methods, listen filters, queue routing, priority, partition keys, delay, deduplication metadata, base/max polling intervals, and multi-executor-oriented status fields.
- Communication: send/recv, set/get event, durable stream write/read/close, async stream reads, typed client reads, and per-call portable send/event/stream options.
- Schedules: create/apply/list/get/pause/resume/delete, trigger, backfill, cron timezone, automatic backfill metadata, queue routing, and schedule reconciler loop.
- Management: standalone client, application-version listing/latest setting, admin HTTP endpoints, Conductor launch wiring, outbound WebSocket transport, request dispatch/encoding, recovery requests, pending-workflow checks, batch cancel/resume/delete, queue/workflow/step inspection, workflow import/export hooks including steps/events/messages/streams/children, and fast-telemetry metrics exposed through `DbosObservability` plus the admin `/metrics` response by default.
- Utilities: debouncer, patch/deprecate patch, structured `DbosError`, portable workflow args/errors, JSON and portable serialization helpers, custom serializer trait, and Rust context value/deadline/cancel/cancel-cause helpers.

## Remaining Parity Notes

- Go's `gob` serializer is intentionally not ported. Rust uses DBOS JSON, portable JSON, and `CustomSerializer`.
- Go's `context.Context` is mapped to Rust-owned `DbosContext` helpers (`with_value`, `value`, `with_cancel`, `with_cancel_cause`, `with_timeout`, `without_cancel`) instead of a literal `From(context.Context)` API.
- The Rust Postgres and Turso stores persist the SDK state through an idempotent JSON-state system database. Exact upstream DBOS physical table/protocol reuse remains subject to partnership/license review before any schema-level convergence.
- The upstreamable observability surface is always `fast-telemetry` based, and the default SDK, `full`, Conductor, admin, and storage features do not depend on application-specific service types, environment variables, or control-plane resources.

With those notes, the crate now exposes full Go public capability coverage for the audited snapshot using idiomatic Rust names and feature gates.
