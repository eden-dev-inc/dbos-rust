# DBOS Go To Rust API Map

This crate targets capability parity with the Go SDK while using idiomatic Rust names.

| Go SDK | Rust SDK |
| --- | --- |
| `Config` | `DbosConfig` |
| configured `pgxpool.Pool` / Turso DB | `SystemDatabaseHandle` / `DbosConfig::with_system_database` |
| Turso system database config | `DbosConfig::with_turso_path` / `turso://...` database URL behind `turso` |
| custom serializer config | `DbosConfig::with_serializer` and `CustomSerializer` |
| observability hooks | `DbosConfig::with_observability` and `DbosObservability`; metrics, spans, snapshots, and Prometheus export are backed by `fast-telemetry` by default |
| `NewDBOSContext` | `DbosContext::new` / `new_dbos_context` |
| `Launch` | `DbosContext::launch` / `launch` |
| `Shutdown` | `DbosContext::shutdown` / `shutdown` |
| `context.WithValue` / `Value` | `DbosContext::with_value` / `DbosContext::value` |
| `context.WithCancel` / `WithCancelCause` / `WithTimeout` / `WithoutCancel` | `DbosContext::with_cancel` / `DbosContext::with_cancel_cause` / `DbosContext::with_timeout` / `DbosContext::without_cancel` |
| `RegisterWorkflow` | `DbosContext::register_workflow` |
| `RunWorkflow` | `DbosContext::run_workflow` |
| `RunAsStep` | `DbosContext::run_as_step` / `run_as_step` |
| `Go` | `DbosContext::spawn_step` |
| `Select` | `DbosContext::select_step` |
| `Sleep` | `DbosContext::sleep` / `sleep` |
| `WorkflowHandle.GetResult` | `WorkflowHandle::get_result` / `WorkflowHandle::get_result_with_options` |
| `WithHandleTimeout` / `WithHandlePollingInterval` | `GetResultOptions::with_timeout` / `GetResultOptions::with_polling_interval` |
| `WorkflowHandle.GetStatus` | `WorkflowHandle::get_status` |
| `WorkflowHandle.GetWorkflowID` | `WorkflowHandle::workflow_id` |
| `RetrieveWorkflow` | `DbosContext::retrieve_workflow` / `DbosClient::retrieve_workflow` |
| `GetWorkflowSteps` | `DbosContext::get_workflow_steps` / `DbosContext::get_workflow_steps_with_options` / `DbosClient::get_workflow_steps_with_options` |
| `WithStepsLoadOutput` / `WithStepsLimit` / `WithStepsOffset` | `GetWorkflowStepsOptions` |
| `ListWorkflows` | `DbosContext::list_workflows` / `DbosClient::list_workflows` |
| `CancelWorkflow` / `CancelWorkflows` | `DbosContext::cancel_workflow` / `DbosContext::cancel_workflows` |
| `ResumeWorkflow` / `ResumeWorkflows` | `DbosContext::resume_workflow` / `DbosContext::resume_workflows` |
| `DeleteWorkflows` | `DbosContext::delete_workflows` / `DbosClient::delete_workflows` |
| `ForkWorkflow` | `DbosContext::fork_workflow` / `DbosClient::fork_workflow` |
| `WorkflowAggregates` / `StepAggregates` | `DbosContext::get_workflow_aggregates`, `DbosContext::get_workflow_aggregates_with_input`, `DbosContext::get_step_aggregates`, `DbosContext::get_step_aggregates_with_input` |
| aggregate input structs | `GetWorkflowAggregatesInput` / `GetStepAggregatesInput` |
| `ExportWorkflow` / `ImportWorkflow` | `DbosContext::export_workflow`, `DbosContext::export_workflow_with_options`, `DbosContext::import_workflow` |
| export child controls | `ExportWorkflowOptions` |
| `ListRegisteredWorkflows` | `DbosContext::list_registered_workflows` / `DbosClient::list_registered_workflows` |
| `ListRegisteredQueues` | `DbosContext::list_registered_queues` / `DbosClient::list_registered_queues` |
| `WithStepMaxRetries` / `WithBackoffFactor` / `WithBaseInterval` / `WithMaxInterval` / `WithRetryPredicate` / `WithNextStepID` | `StepOptions` with `DbosContext::run_as_step_with_options` |
| `Client` | `DbosClient` |
| `NewClient` | `DbosClient::new` / `new_client` |
| `Client.Enqueue` | `DbosClient::enqueue` / `enqueue` |
| `RegisterQueue` | `DbosContext::register_queue` |
| `RetrieveQueue` | `DbosContext::retrieve_queue` |
| `ListQueues` | `DbosContext::list_queues` |
| `DeleteQueue` | `DbosContext::delete_queue` |
| `Queue.Get*` / `Queue.Set*` | `WorkflowQueue` getters and persisted `set_*` methods |
| queue base/max polling interval | `WorkflowQueue::polling_interval` / `WorkflowQueue::max_polling_interval` and setters |
| `ListenQueues` | `DbosContext::listen_queues` |
| `Send` / `Recv` | `DbosContext::send` / `DbosContext::recv` |
| `WithPortableSend` | `SendOptions::portable` with `DbosContext::send_with_options` / `DbosClient::send_with_options` |
| `SetEvent` / `GetEvent` | `DbosContext::set_event` / `DbosContext::get_event` |
| `WithPortableSetEvent` | `SetEventOptions::portable` with `DbosContext::set_event_with_options` |
| `WriteStream` / `ReadStream` | `DbosContext::write_stream` / `DbosContext::read_stream` / `DbosContext::read_stream_with_options` |
| `WithReadStreamSnapshot` | `ReadStreamOptions::snapshot_from_offset` |
| `WithPortableWriteStream` | `WriteStreamOptions::portable` with `DbosContext::write_stream_with_options` |
| `ReadStreamAsync` | `DbosContext::read_stream_async` |
| `CreateSchedule` | `DbosContext::create_schedule` |
| `ApplySchedules` | `DbosContext::apply_schedules` |
| `PauseSchedule` / `ResumeSchedule` | `DbosContext::pause_schedule` / `DbosContext::resume_schedule` |
| `BackfillSchedule` | `DbosContext::backfill_schedule` |
| `TriggerSchedule` | `DbosContext::trigger_schedule` |
| `Patch` / `DeprecatePatch` | `DbosContext::patch` / `DbosContext::deprecate_patch` |
| transaction isolation options | `TransactionOptions` / `TransactionIsolationLevel` with `DbosContext::run_as_transaction_with_options` |
| `Debouncer` | `Debouncer` |
| `AdminServer` | `start_admin_server` behind the `admin` feature; health, workflow, queue, schedule, app-version, and metrics endpoints. `/metrics` includes a fast-telemetry snapshot and Prometheus text by default. |
| `Conductor` | `DbosConfig::with_conductor`, `DbosConfig::with_conductor_executor_metadata`, `DbosContext::launch` outbound connection, `connect_conductor`, `ConductorTransport`, `ConductorRequest`, `ConductorResponse`, `dispatch_conductor_request`, and `handle_conductor_request` behind the `conductor` feature |

The Rust implementation does not port Go's `gob` serializer. It uses DBOS JSON, portable JSON, and a Rust custom serializer trait.

The DBOS crate keeps admin endpoints, Conductor, and database backends behind feature gates, while observability is fast-telemetry backed by default. The default SDK, `full`, and Conductor APIs do not depend on application-specific service types, environment variables, or control-plane resources.

Chaos coverage from the Go SDK's `chaos_tests/chaos_test.go` lives in
`tests/chaos.rs`; see `docs/chaos-tests.md` for the opt-in runbook.
