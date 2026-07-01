#![allow(clippy::result_large_err)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{TimeZone, Utc};
use dbos::{
    ConductorMessageKind, ConductorRequest, CreateScheduleRequest, DbosConfig, DbosContext, DbosError, ForkWorkflowInput, GetResultOptions,
    GetWorkflowStepsOptions, ListRegisteredWorkflowsOptions, ListSchedulesOptions, ListWorkflowsOptions, QueueConflictResolution,
    RateLimiter, ReadStreamOptions, ResumeWorkflowOptions, ScheduleStatus, SetWorkflowDelayOptions, StepOptions, WorkflowOptions,
    WorkflowQueue, WorkflowRegistrationOptions, WorkflowStatusType, decode_conductor_request, encode_conductor_response,
    handle_conductor_request,
};
#[cfg(feature = "turso")]
use dbos::{
    EncodedValue, ExportWorkflowOptions, GetStepAggregatesInput, GetWorkflowAggregatesInput, PORTABLE_JSON, SendOptions, SetEventOptions,
    TransactionIsolationLevel, TransactionOptions, WriteStreamOptions,
};
#[cfg(feature = "turso")]
use serde_json::Value;
use serde_json::json;
#[cfg(feature = "admin")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "admin")]
use tokio::net::TcpStream;

async fn test_context(name: &str) -> dbos::Result<DbosContext> {
    DbosContext::new(DbosConfig::new(name)).await
}

#[tokio::test(flavor = "current_thread")]
async fn records_fast_telemetry_metrics_and_spans() -> dbos::Result<()> {
    let ctx = DbosContext::new(DbosConfig::new("dbos-test-observability")).await?;
    assert!(ctx.observability().is_enabled());
    ctx.register_workflow(
        "observed.workflow",
        |ctx, input: i32| async move { ctx.run_as_step("observed.step", |_ctx| async move { Ok(input + 1) }).await },
        WorkflowRegistrationOptions::default(),
    )
    .await?;

    let handle = ctx
        .run_workflow::<_, i32>(
            "observed.workflow",
            41,
            WorkflowOptions {
                workflow_id: Some("wf-observed".to_string()),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(handle.get_result(Some(Duration::from_secs(3))).await?, 42);

    let snapshot = ctx.observability().snapshot();
    assert!(snapshot.operations_started >= 4, "{snapshot:?}");
    assert!(snapshot.operations_finished >= 4, "{snapshot:?}");
    assert_eq!(snapshot.active_operations, 0, "{snapshot:?}");
    assert!(snapshot.operation_duration_count >= 4, "{snapshot:?}");
    assert!(snapshot.spans_recorded >= 4, "{snapshot:?}");
    let prometheus = ctx.observability().export_prometheus();
    assert!(prometheus.contains("dbos_operations_started_total{operation=\"workflow.run\"}"));
    assert!(prometheus.contains("dbos_operations_finished_total{operation=\"workflow.run\",outcome=\"success\"}"));
    assert!(prometheus.contains("dbos_operation_duration_us_count{operation=\"workflow.run\",outcome=\"success\"}"));
    assert!(prometheus.contains("dbos_active_operations{operation=\"workflow.run\"} 0"));

    let spans = ctx.observability().drain_spans();
    assert!(spans.iter().any(|span| span.name.as_ref() == "dbos.workflow.run"));
    assert!(spans.iter().any(|span| span.name.as_ref() == "dbos.workflow.execute"));
    assert!(spans.iter().any(|span| span.name.as_ref() == "dbos.step.run"));
    assert!(spans.iter().any(|span| span.name.as_ref() == "dbos.workflow.result"));
    Ok(())
}

#[cfg(feature = "turso")]
#[derive(Debug)]
struct PrefixSerializer;

#[cfg(feature = "turso")]
impl dbos::CustomSerializer for PrefixSerializer {
    fn name(&self) -> &str {
        "test-prefix-json"
    }

    fn encode_value(&self, value: &Value) -> dbos::Result<EncodedValue> {
        Ok(EncodedValue {
            data: Some(format!("custom:{}", serde_json::to_string(value)?)),
            serialization: self.name().to_string(),
        })
    }

    fn decode_value(&self, encoded: &EncodedValue) -> dbos::Result<Value> {
        let Some(data) = &encoded.data else {
            return Ok(Value::Null);
        };
        let payload = data.strip_prefix("custom:").ok_or_else(|| DbosError::serialization("custom serializer payload missing prefix"))?;
        serde_json::from_str(payload).map_err(DbosError::from)
    }
}

#[cfg(feature = "admin")]
async fn admin_request(port: u16, method: &str, path: &str) -> dbos::Result<(u16, serde_json::Value)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .map_err(|err| DbosError::database(format!("failed to connect to admin server: {err}")))?;
    let request = format!("{method} {path} HTTP/1.1\r\nhost: 127.0.0.1\r\nconnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|err| DbosError::database(format!("failed to write admin request: {err}")))?;
    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .await
        .map_err(|err| DbosError::database(format!("failed to read admin response: {err}")))?;
    let response = String::from_utf8(bytes).map_err(|err| DbosError::database(format!("admin response was not utf8: {err}")))?;
    let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| DbosError::database("admin response missing header separator"))?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| DbosError::database("admin response missing status"))?;
    let value = serde_json::from_str(body)?;
    Ok((status, value))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registers_and_runs_a_workflow_with_metadata() -> dbos::Result<()> {
    let ctx = test_context("dbos-test-basic").await?;
    ctx.register_workflow(
        "math.double",
        |_ctx, input: i32| async move { Ok(input * 2) },
        WorkflowRegistrationOptions {
            name: Some("double".to_string()),
            class_name: Some("Math".to_string()),
            config_name: Some("default".to_string()),
            max_retries: Some(3),
            ..Default::default()
        },
    )
    .await?;
    ctx.launch().await?;

    let handle = ctx
        .run_workflow::<_, i32>(
            "double",
            21,
            WorkflowOptions {
                workflow_id: Some("wf-basic".to_string()),
                authenticated_user: Some("alice".to_string()),
                authenticated_roles: vec!["admin".to_string()],
                config_name: Some("default".to_string()),
                ..Default::default()
            },
        )
        .await?;
    let result = handle.get_result(Some(Duration::from_secs(2))).await?;
    let status = handle.get_status().await?;

    assert_eq!(result, 42);
    assert_eq!(status.status, WorkflowStatusType::Success);
    assert_eq!(status.authenticated_user.as_deref(), Some("alice"));
    assert_eq!(status.authenticated_roles, vec!["admin".to_string()]);

    let listed = ctx
        .list_workflows(ListWorkflowsOptions {
            workflow_name: Some("double".to_string()),
            load_output: true,
            ..Default::default()
        })
        .await?;
    assert_eq!(listed.len(), 1);

    ctx.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replays_step_checkpoints_after_resume() -> dbos::Result<()> {
    let ctx = test_context("dbos-test-replay").await?;
    let step_calls = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::new(AtomicUsize::new(0));
    let workflow_step_calls = Arc::clone(&step_calls);
    let workflow_attempts = Arc::clone(&attempts);

    ctx.register_workflow(
        "checkpointed",
        move |ctx, _input: ()| {
            let step_calls = Arc::clone(&workflow_step_calls);
            let attempts = Arc::clone(&workflow_attempts);
            async move {
                let value: i32 = ctx
                    .run_as_step("expensive", move |_ctx| {
                        let step_calls = Arc::clone(&step_calls);
                        async move {
                            step_calls.fetch_add(1, Ordering::SeqCst);
                            Ok(7)
                        }
                    })
                    .await?;
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(DbosError::workflow_execution("checkpointed", "first attempt failed after checkpoint"));
                }
                Ok(value)
            }
        },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.launch().await?;

    let handle = ctx
        .run_workflow::<_, i32>(
            "checkpointed",
            (),
            WorkflowOptions {
                workflow_id: Some("wf-replay".to_string()),
                ..Default::default()
            },
        )
        .await?;
    assert!(handle.get_result(Some(Duration::from_secs(2))).await.is_err());

    let resumed = ctx.resume_workflow::<i32>("wf-replay", ResumeWorkflowOptions::default()).await?;
    assert_eq!(resumed.get_result(Some(Duration::from_secs(2))).await?, 7);
    assert_eq!(step_calls.load(Ordering::SeqCst), 1);

    let steps = ctx.get_workflow_steps("wf-replay").await?;
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].step_name, "expensive");

    ctx.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routes_queued_workflows_and_validates_queue_options() -> dbos::Result<()> {
    let ctx = test_context("dbos-test-queue").await?;
    ctx.register_workflow(
        "queued.increment",
        |_ctx, input: i32| async move { Ok(input + 1) },
        WorkflowRegistrationOptions::default(),
    )
    .await?;

    let mut invalid = WorkflowQueue::new("");
    invalid.polling_interval = Duration::from_millis(10);
    assert!(ctx.register_queue(invalid).await.is_err());

    let mut queue = WorkflowQueue::new("fast");
    queue.polling_interval = Duration::from_millis(10);
    queue.worker_concurrency = Some(1);
    queue.global_concurrency = Some(2);
    queue.rate_limit = Some(RateLimiter { limit: 10, period: Duration::from_secs(1) });
    queue.on_conflict = QueueConflictResolution::AlwaysUpdate;
    ctx.register_queue(queue.clone()).await?;
    ctx.launch().await?;

    let handle = ctx
        .run_workflow::<_, i32>(
            "queued.increment",
            41,
            WorkflowOptions {
                workflow_id: Some("wf-queued".to_string()),
                queue_name: Some(queue.name),
                ..Default::default()
            },
        )
        .await?;

    assert_eq!(handle.get_result(Some(Duration::from_secs(4))).await?, 42);
    assert_eq!(ctx.retrieve_queue("fast").await?.map(|queue| queue.name), Some("fast".to_string()));

    ctx.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supports_handle_queue_list_and_step_options() -> dbos::Result<()> {
    let ctx = test_context("dbos-test-api-options").await?;
    let step_attempts = Arc::new(AtomicUsize::new(0));
    let workflow_step_attempts = Arc::clone(&step_attempts);
    ctx.register_workflow(
        "options.workflow",
        move |ctx, input: i32| {
            let attempts = Arc::clone(&workflow_step_attempts);
            async move {
                let first = ctx
                    .run_as_step_with_options(
                        "retrying",
                        StepOptions::default()
                            .with_max_retries(1)
                            .with_base_interval(Duration::from_millis(1))
                            .with_max_interval(Duration::from_millis(5)),
                        move |_ctx| {
                            let attempts = Arc::clone(&attempts);
                            async move {
                                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                                    return Err(DbosError::step_execution("wf-options", "retrying", "transient failure"));
                                }
                                Ok(input + 1)
                            }
                        },
                    )
                    .await?;
                ctx.run_as_step("second", move |_ctx| async move { Ok(first + 1) }).await
            }
        },
        WorkflowRegistrationOptions {
            schedule: Some("0 0 0 * * * *".to_string()),
            ..Default::default()
        },
    )
    .await?;

    let mut queue = WorkflowQueue::new("options-queue");
    queue.on_conflict = QueueConflictResolution::AlwaysUpdate;
    ctx.register_queue(queue.clone()).await?;
    queue.set_worker_concurrency(&ctx, Some(2)).await?;
    queue.set_global_concurrency(&ctx, Some(4)).await?;
    queue.set_rate_limit(&ctx, Some(RateLimiter { limit: 5, period: Duration::from_secs(1) })).await?;
    queue.set_priority_enabled(&ctx, true).await?;
    queue.set_partition_queue(&ctx, true).await?;
    queue.set_polling_interval(&ctx, Duration::from_millis(20)).await?;
    queue.set_max_polling_interval(&ctx, Duration::from_millis(200)).await?;

    assert_eq!(queue.name(), "options-queue");
    assert_eq!(queue.worker_concurrency(), Some(2));
    assert_eq!(queue.global_concurrency(), Some(4));
    assert_eq!(queue.rate_limit().map(|rate_limit| rate_limit.limit), Some(5));
    assert!(queue.priority_enabled());
    assert!(queue.partition_queue());
    assert_eq!(queue.polling_interval(), Duration::from_millis(20));
    assert_eq!(queue.max_polling_interval(), Duration::from_millis(200));
    let persisted = ctx.retrieve_queue("options-queue").await?.ok_or_else(|| DbosError::database("missing queue"))?;
    assert_eq!(persisted.worker_concurrency, Some(2));
    assert!(persisted.partition_queue);
    assert_eq!(persisted.max_polling_interval, Duration::from_millis(200));
    let registered = ctx.list_registered_workflows(ListRegisteredWorkflowsOptions { scheduled_only: true }).await?;
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].name, "options.workflow");
    assert_eq!(ctx.list_registered_queues().await?.len(), 1);

    ctx.launch().await?;
    let handle = ctx
        .run_workflow::<_, i32>(
            "options.workflow",
            5,
            WorkflowOptions {
                workflow_id: Some("wf-options".to_string()),
                queue_name: Some("options-queue".to_string()),
                queue_partition_key: Some("partition-a".to_string()),
                authenticated_user: Some("dbos-auditor".to_string()),
                deduplication_id: Some("dedup-options".to_string()),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(
        handle
            .get_result_with_options(
                GetResultOptions::default().with_timeout(Duration::from_secs(4)).with_polling_interval(Duration::from_millis(25)),
            )
            .await?,
        7
    );
    assert_eq!(step_attempts.load(Ordering::SeqCst), 2);

    let loaded = ctx
        .list_workflows(ListWorkflowsOptions {
            workflow_names: vec!["options.workflow".to_string()],
            authenticated_users: vec!["dbos-auditor".to_string()],
            queue_names: vec!["options-queue".to_string()],
            deduplication_ids: vec!["dedup-options".to_string()],
            executor_ids: vec![ctx.executor_id().to_string()],
            completed_after: Some(Utc::now() - chrono::Duration::minutes(1)),
            dequeued_after: Some(Utc::now() - chrono::Duration::minutes(1)),
            has_parent: Some(false),
            load_input: true,
            load_output: true,
            ..Default::default()
        })
        .await?;
    assert_eq!(loaded.len(), 1);
    assert!(loaded[0].input.is_some());
    assert!(loaded[0].output.is_some());

    let unloaded = ctx
        .list_workflows(ListWorkflowsOptions {
            workflow_id_prefixes: vec!["wf-opt".to_string()],
            load_input: false,
            load_output: false,
            ..Default::default()
        })
        .await?;
    assert_eq!(unloaded.len(), 1);
    assert!(unloaded[0].input.is_none());
    assert!(unloaded[0].output.is_none());

    let steps = ctx
        .get_workflow_steps_with_options(
            "wf-options",
            GetWorkflowStepsOptions { load_output: Some(false), limit: Some(1), offset: Some(1) },
        )
        .await?;
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].step_name, "second");
    assert!(steps[0].output.is_none());

    ctx.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supports_messages_events_and_durable_streams() -> dbos::Result<()> {
    let ctx = test_context("dbos-test-communication").await?;
    ctx.register_workflow(
        "communicate",
        |ctx, _input: ()| async move {
            let message: String = ctx.recv("topic", Duration::from_secs(2)).await?;
            ctx.set_event("received", message.clone()).await?;
            ctx.write_stream("updates", message.clone()).await?;
            ctx.close_stream("updates").await?;
            Ok(message.len())
        },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.launch().await?;

    let handle = ctx
        .run_workflow::<_, usize>(
            "communicate",
            (),
            WorkflowOptions {
                workflow_id: Some("wf-comm".to_string()),
                ..Default::default()
            },
        )
        .await?;
    ctx.send(handle.workflow_id(), "hello".to_string(), "topic").await?;

    assert_eq!(handle.get_result(Some(Duration::from_secs(3))).await?, 5);
    let event: String = ctx.get_event(handle.workflow_id(), "received", Duration::from_secs(1)).await?;
    let (stream, closed): (Vec<String>, bool) = ctx.read_stream(handle.workflow_id(), "updates").await?;
    assert_eq!(event, "hello");
    assert_eq!(stream, vec!["hello".to_string()]);
    assert!(closed);
    let (tail, tail_closed): (Vec<String>, bool) =
        ctx.read_stream_with_options(handle.workflow_id(), "updates", ReadStreamOptions::snapshot_from_offset(1)).await?;
    assert!(tail.is_empty());
    assert!(tail_closed);

    ctx.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[cfg(feature = "turso")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supports_serializer_turso_export_aggregates_and_context_helpers() -> dbos::Result<()> {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|err| DbosError::database(format!("system clock moved backwards: {err}")))?
        .as_nanos();
    path.push(format!("dbos-rust-api-surface-{nanos}.db"));
    let turso_path = path.to_string_lossy().to_string();

    let ctx = DbosContext::new(
        DbosConfig::new("dbos-test-parity").with_turso_path(turso_path.clone()).with_serializer(Arc::new(PrefixSerializer)),
    )
    .await?
    .with_value("tenant", "acme")?;

    let (timed_ctx, cancel) = ctx.with_timeout(Duration::from_secs(1));
    assert!(timed_ctx.deadline().is_some());
    cancel.cancel();
    assert!(timed_ctx.is_cancelled());
    assert!(!timed_ctx.without_cancel().is_cancelled());

    ctx.register_workflow(
        "parity.workflow",
        |ctx, _input: ()| async move {
            let tenant: Option<String> = ctx.value("tenant")?;
            assert_eq!(tenant.as_deref(), Some("acme"));
            let tx_value: i32 = ctx
                .run_as_transaction_with_options(
                    "tx-step",
                    TransactionOptions {
                        isolation_level: Some(TransactionIsolationLevel::Serializable),
                    },
                    |tx| async move {
                        assert_eq!(tx.isolation_level(), Some(TransactionIsolationLevel::Serializable));
                        Ok(2)
                    },
                )
                .await?;
            let message: String = ctx.recv("topic", Duration::from_secs(2)).await?;
            ctx.set_event_with_options("received", message.clone(), SetEventOptions::portable()).await?;
            ctx.write_stream_with_options("updates", message.clone(), WriteStreamOptions::portable()).await?;
            ctx.close_stream("updates").await?;
            Ok(tx_value + i32::try_from(message.len()).map_err(|_| DbosError::invalid_argument("message too long"))?)
        },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.launch().await?;

    let handle = ctx
        .run_workflow::<_, i32>(
            "parity.workflow",
            (),
            WorkflowOptions {
                workflow_id: Some("wf-parity".to_string()),
                ..Default::default()
            },
        )
        .await?;
    ctx.send_with_options(handle.workflow_id(), "hello".to_string(), "topic", SendOptions::portable()).await?;
    assert_eq!(handle.get_result(Some(Duration::from_secs(3))).await?, 7);

    let status = handle.get_status().await?;
    assert_eq!(status.serialization, "test-prefix-json");
    assert!(status.output.as_ref().and_then(Value::as_str).is_some_and(|data| data.starts_with("custom:")));

    let event: String = ctx.get_event(handle.workflow_id(), "received", Duration::from_secs(1)).await?;
    let (stream, closed): (Vec<String>, bool) = ctx.read_stream(handle.workflow_id(), "updates").await?;
    assert_eq!(event, "hello");
    assert_eq!(stream, vec!["hello".to_string()]);
    assert!(closed);

    let export = ctx.export_workflow_with_options(handle.workflow_id(), ExportWorkflowOptions { include_children: true }).await?;
    assert_eq!(export.events.len(), 1);
    assert_eq!(export.events[0].serialization, PORTABLE_JSON);
    assert_eq!(export.streams.iter().filter(|entry| !entry.closed).count(), 1);
    assert_eq!(export.messages.len(), 1);
    assert_eq!(export.messages[0].serialization, PORTABLE_JSON);

    let import_ctx = test_context("dbos-test-parity-import").await?;
    import_ctx.import_workflow(export.clone()).await?;
    assert_eq!(
        import_ctx.retrieve_workflow::<Value>("wf-parity").await.get_status().await?.status,
        WorkflowStatusType::Success
    );
    let imported_event: String = import_ctx.get_event("wf-parity", "received", Duration::from_millis(1)).await?;
    assert_eq!(imported_event, "hello");

    let workflow_aggregates = ctx
        .get_workflow_aggregates_with_input(GetWorkflowAggregatesInput {
            group_by_workflow_name: true,
            group_by_status: true,
            time_bucket_size: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .await?;
    assert!(
        workflow_aggregates
            .iter()
            .any(|row| row.bucket.get("workflow_name") == Some(&Value::String("parity.workflow".to_string())))
    );

    let step_aggregates = ctx
        .get_step_aggregates_with_input(GetStepAggregatesInput {
            group_by_function_name: true,
            group_by_status: true,
            select_max_duration_ms: true,
            ..Default::default()
        })
        .await?;
    assert!(step_aggregates.iter().any(|row| row.bucket.get("function_name") == Some(&Value::String("tx-step".to_string()))));

    ctx.shutdown(Duration::from_secs(1)).await;
    let reopened = DbosContext::new(DbosConfig::new("dbos-test-parity-reopen").with_turso_path(turso_path)).await?;
    assert_eq!(
        reopened.retrieve_workflow::<Value>("wf-parity").await.get_status().await?.status,
        WorkflowStatusType::Success
    );
    let _ = std::fs::remove_file(path);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manages_schedules_and_control_operations() -> dbos::Result<()> {
    let ctx = test_context("dbos-test-schedules").await?;
    ctx.register_workflow(
        "scheduled",
        |_ctx, input: dbos::ScheduledWorkflowInput| async move { Ok(input.context.unwrap_or_else(|| json!({ "missing": true }))) },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.launch().await?;

    ctx.create_schedule(CreateScheduleRequest {
        schedule_name: "nightly".to_string(),
        schedule: "0 0 0 * * * *".to_string(),
        workflow_name: "scheduled".to_string(),
        context: Some(json!({ "kind": "nightly" })),
        automatic_backfill: true,
        cron_timezone: Some("UTC".to_string()),
        queue_name: None,
        workflow_class_name: None,
    })
    .await?;

    let active = ctx.list_schedules(ListSchedulesOptions { statuses: vec![ScheduleStatus::Active], ..Default::default() }).await?;
    assert_eq!(active.len(), 1);

    ctx.pause_schedule("nightly").await?;
    assert_eq!(ctx.get_schedule("nightly").await?.map(|schedule| schedule.status), Some(ScheduleStatus::Paused));
    ctx.resume_schedule("nightly").await?;

    let start = Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .ok_or_else(|| DbosError::invalid_argument("invalid test start time"))?;
    let end = Utc.with_ymd_and_hms(2026, 1, 3, 0, 0, 0).single().ok_or_else(|| DbosError::invalid_argument("invalid test end time"))?;
    let backfilled = ctx.backfill_schedule("nightly", start, end).await?;
    assert_eq!(backfilled.len(), 2);

    let delayed = ctx
        .run_workflow::<_, serde_json::Value>(
            "scheduled",
            dbos::ScheduledWorkflowInput {
                scheduled_time: start,
                context: Some(json!({ "manual": true })),
            },
            WorkflowOptions {
                workflow_id: Some("wf-delayed".to_string()),
                delay: Some(Duration::from_secs(30)),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(delayed.get_status().await?.status, WorkflowStatusType::Delayed);
    ctx.set_workflow_delay(
        delayed.workflow_id(),
        SetWorkflowDelayOptions { delay: Some(Duration::from_millis(1)), delay_until: None },
    )
    .await?;

    let forked = ctx
        .fork_workflow::<serde_json::Value>(ForkWorkflowInput {
            original_workflow_id: "wf-delayed".to_string(),
            forked_workflow_id: Some("wf-forked".to_string()),
            ..Default::default()
        })
        .await?;
    assert_eq!(forked.workflow_id(), "wf-forked");

    ctx.delete_schedule("nightly").await?;
    assert!(ctx.get_schedule("nightly").await?.is_none());

    ctx.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[cfg(feature = "admin")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_server_exposes_control_endpoints() -> dbos::Result<()> {
    let ctx = test_context("dbos-test-admin").await?;
    ctx.register_workflow("admin.echo", |_ctx, input: String| async move { Ok(input) }, WorkflowRegistrationOptions::default())
        .await?;
    ctx.launch().await?;

    let handle = ctx
        .run_workflow::<_, String>(
            "admin.echo",
            "hello".to_string(),
            WorkflowOptions {
                workflow_id: Some("wf-admin".to_string()),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(handle.get_result(Some(Duration::from_secs(2))).await?, "hello");

    let admin = dbos::start_admin_server(ctx.clone(), dbos::AdminServerConfig { host: "127.0.0.1".to_string(), port: 0 }).await?;

    let (status, health) = admin_request(admin.port(), "GET", "/healthz").await?;
    assert_eq!(status, 200);
    assert_eq!(health["status"], "ok");

    let (status, workflows) = admin_request(admin.port(), "GET", "/workflows?workflow_name=admin.echo").await?;
    assert_eq!(status, 200);
    assert_eq!(workflows["workflows"].as_array().map(Vec::len), Some(1));

    let (status, workflow) = admin_request(admin.port(), "GET", "/workflows/wf-admin").await?;
    assert_eq!(status, 200);
    assert_eq!(workflow["workflow"]["workflow_uuid"], "wf-admin");

    admin.shutdown(Duration::from_secs(1)).await?;
    ctx.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conductor_dispatches_management_requests() -> dbos::Result<()> {
    let ctx = test_context("dbos-test-conductor").await?;
    ctx.register_workflow(
        "conductor.double",
        |_ctx, input: i32| async move { Ok(input * 2) },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.launch().await?;

    let handle = ctx
        .run_workflow::<_, i32>(
            "conductor.double",
            10,
            WorkflowOptions {
                workflow_id: Some("wf-conductor".to_string()),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(handle.get_result(Some(Duration::from_secs(2))).await?, 20);

    let request = ConductorRequest {
        request_id: Some("req-1".to_string()),
        kind: ConductorMessageKind::GetWorkflow,
        payload: json!({ "workflow_id": "wf-conductor" }),
    };
    let response = handle_conductor_request(&ctx, request).await;
    assert!(response.ok);
    assert_eq!(response.request_id.as_deref(), Some("req-1"));
    assert_eq!(response.payload["workflow"]["workflow_uuid"], "wf-conductor");

    let encoded = encode_conductor_response(&response)?;
    assert!(encoded.contains("wf-conductor"));

    let export_response = handle_conductor_request(
        &ctx,
        ConductorRequest {
            request_id: Some("req-export".to_string()),
            kind: ConductorMessageKind::ExportWorkflow,
            payload: json!({ "workflow_id": "wf-conductor" }),
        },
    )
    .await;
    assert!(export_response.ok);

    let import_ctx = test_context("dbos-test-conductor-import").await?;
    let import_response = handle_conductor_request(
        &import_ctx,
        ConductorRequest {
            request_id: Some("req-import".to_string()),
            kind: ConductorMessageKind::ImportWorkflow,
            payload: export_response.payload.clone(),
        },
    )
    .await;
    assert!(import_response.ok);
    let imported = import_ctx.retrieve_workflow::<serde_json::Value>("wf-conductor").await;
    assert_eq!(imported.get_status().await?.status, WorkflowStatusType::Success);

    let decoded =
        decode_conductor_request(r#"{"request_id":"req-2","kind":"list_workflows","payload":{"workflow_name":"conductor.double"}}"#)?;
    let list_response = handle_conductor_request(&ctx, decoded).await;
    assert!(list_response.ok);
    assert_eq!(list_response.payload["workflows"].as_array().map(Vec::len), Some(1));

    let workflow_aggregates = handle_conductor_request(
        &ctx,
        ConductorRequest {
            request_id: Some("req-workflow-aggregates".to_string()),
            kind: ConductorMessageKind::GetWorkflowAggregates,
            payload: json!({ "workflow_ids": ["wf-conductor"], "group_by_status": true }),
        },
    )
    .await;
    assert!(workflow_aggregates.ok);
    assert_eq!(workflow_aggregates.payload["aggregates"].as_array().map(Vec::len), Some(1));
    assert_eq!(workflow_aggregates.payload["aggregates"][0]["bucket"]["status"], "Success");

    let step_aggregates = handle_conductor_request(
        &ctx,
        ConductorRequest {
            request_id: Some("req-step-aggregates".to_string()),
            kind: ConductorMessageKind::GetStepAggregates,
            payload: json!({ "workflow_ids": ["wf-conductor"], "group_by_function_name": true }),
        },
    )
    .await;
    assert!(step_aggregates.ok);
    assert_eq!(step_aggregates.payload["aggregates"].as_array().map(Vec::len), Some(0));

    ctx.shutdown(Duration::from_secs(1)).await;
    Ok(())
}
