use std::future::Future;
use std::sync::{Arc, OnceLock};

use crate::error::{DbosError, Result};

use std::time::Instant;

use fast_telemetry::{
    CounterSet, DynamicDistribution, DynamicDistributionSeries, DynamicGaugeI64, DynamicGaugeI64Series, PrometheusExport, Span,
    SpanCollector, SpanKind, SpanStatus,
};

const DEFAULT_SHARD_COUNT: usize = 16;
const OPERATION_LABEL: &str = "operation";
const OUTCOME_LABEL: &str = "outcome";

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DbosSpanAttribute {
    key: &'static str,
    value: String,
}

impl DbosSpanAttribute {
    pub(crate) fn new(key: &'static str, value: impl Into<String>) -> Self {
        Self { key, value: value.into() }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DbosOperation {
    Launch,
    RunWorkflow,
    ExecuteWorkflow,
    WorkflowResult,
    RunStep,
    QueueSupervisor,
    QueueDequeue,
    ScheduleReconcile,
    ScheduleTrigger,
    MessageSend,
    MessageReceive,
    EventSet,
    EventGet,
    StreamWrite,
    StreamRead,
}

impl DbosOperation {
    const COUNT: usize = 15;

    const fn index(self) -> usize {
        match self {
            Self::Launch => 0,
            Self::RunWorkflow => 1,
            Self::ExecuteWorkflow => 2,
            Self::WorkflowResult => 3,
            Self::RunStep => 4,
            Self::QueueSupervisor => 5,
            Self::QueueDequeue => 6,
            Self::ScheduleReconcile => 7,
            Self::ScheduleTrigger => 8,
            Self::MessageSend => 9,
            Self::MessageReceive => 10,
            Self::EventSet => 11,
            Self::EventGet => 12,
            Self::StreamWrite => 13,
            Self::StreamRead => 14,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Launch => "launch",
            Self::RunWorkflow => "workflow.run",
            Self::ExecuteWorkflow => "workflow.execute",
            Self::WorkflowResult => "workflow.result",
            Self::RunStep => "step.run",
            Self::QueueSupervisor => "queue.supervisor",
            Self::QueueDequeue => "queue.dequeue",
            Self::ScheduleReconcile => "schedule.reconcile",
            Self::ScheduleTrigger => "schedule.trigger",
            Self::MessageSend => "message.send",
            Self::MessageReceive => "message.receive",
            Self::EventSet => "event.set",
            Self::EventGet => "event.get",
            Self::StreamWrite => "stream.write",
            Self::StreamRead => "stream.read",
        }
    }
    const fn span_name(self) -> &'static str {
        match self {
            Self::Launch => "dbos.launch",
            Self::RunWorkflow => "dbos.workflow.run",
            Self::ExecuteWorkflow => "dbos.workflow.execute",
            Self::WorkflowResult => "dbos.workflow.result",
            Self::RunStep => "dbos.step.run",
            Self::QueueSupervisor => "dbos.queue.supervisor",
            Self::QueueDequeue => "dbos.queue.dequeue",
            Self::ScheduleReconcile => "dbos.schedule.reconcile",
            Self::ScheduleTrigger => "dbos.schedule.trigger",
            Self::MessageSend => "dbos.message.send",
            Self::MessageReceive => "dbos.message.receive",
            Self::EventSet => "dbos.event.set",
            Self::EventGet => "dbos.event.get",
            Self::StreamWrite => "dbos.stream.write",
            Self::StreamRead => "dbos.stream.read",
        }
    }
    const fn span_kind(self) -> SpanKindCompat {
        match self {
            Self::MessageSend | Self::StreamWrite => SpanKindCompat::Producer,
            Self::MessageReceive | Self::StreamRead => SpanKindCompat::Consumer,
            _ => SpanKindCompat::Internal,
        }
    }
}

const DBOS_OPERATIONS: [DbosOperation; DbosOperation::COUNT] = [
    DbosOperation::Launch,
    DbosOperation::RunWorkflow,
    DbosOperation::ExecuteWorkflow,
    DbosOperation::WorkflowResult,
    DbosOperation::RunStep,
    DbosOperation::QueueSupervisor,
    DbosOperation::QueueDequeue,
    DbosOperation::ScheduleReconcile,
    DbosOperation::ScheduleTrigger,
    DbosOperation::MessageSend,
    DbosOperation::MessageReceive,
    DbosOperation::EventSet,
    DbosOperation::EventGet,
    DbosOperation::StreamWrite,
    DbosOperation::StreamRead,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SpanKindCompat {
    Internal,
    Producer,
    Consumer,
}

impl From<SpanKindCompat> for SpanKind {
    fn from(value: SpanKindCompat) -> Self {
        match value {
            SpanKindCompat::Internal => Self::Internal,
            SpanKindCompat::Producer => Self::Producer,
            SpanKindCompat::Consumer => Self::Consumer,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DbosOperationOutcome {
    Success,
    Error,
    Cached,
    Cancelled,
    Dropped,
}

impl DbosOperationOutcome {
    const COUNT: usize = 5;

    const fn index(self) -> usize {
        match self {
            Self::Success => 0,
            Self::Error => 1,
            Self::Cached => 2,
            Self::Cancelled => 3,
            Self::Dropped => 4,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Cached => "cached",
            Self::Cancelled => "cancelled",
            Self::Dropped => "dropped",
        }
    }
}

const DBOS_OPERATION_OUTCOMES: [DbosOperationOutcome; DbosOperationOutcome::COUNT] = [
    DbosOperationOutcome::Success,
    DbosOperationOutcome::Error,
    DbosOperationOutcome::Cached,
    DbosOperationOutcome::Cancelled,
    DbosOperationOutcome::Dropped,
];

const STARTED_COUNTER_OFFSET: usize = 0;
const FINISHED_COUNTER_OFFSET: usize = STARTED_COUNTER_OFFSET + DbosOperation::COUNT;
const DBOS_OPERATION_COUNTER_COUNT: usize = FINISHED_COUNTER_OFFSET + (DbosOperation::COUNT * DbosOperationOutcome::COUNT);

const fn started_counter_index(operation: DbosOperation) -> usize {
    STARTED_COUNTER_OFFSET + operation.index()
}

const fn finished_counter_index(operation: DbosOperation, outcome: DbosOperationOutcome) -> usize {
    FINISHED_COUNTER_OFFSET + (operation.index() * DbosOperationOutcome::COUNT) + outcome.index()
}

fn operation_labels(operation: DbosOperation) -> [(&'static str, &'static str); 1] {
    [(OPERATION_LABEL, operation.as_str())]
}

fn operation_outcome_labels(operation: DbosOperation, outcome: DbosOperationOutcome) -> [(&'static str, &'static str); 2] {
    [(OPERATION_LABEL, operation.as_str()), (OUTCOME_LABEL, outcome.as_str())]
}

/// DBOS observability hook backed by `fast-telemetry`.
///
/// DBOS contexts install an enabled handle by default. Use
/// [`DbosObservability::disabled`] with [`crate::DbosConfig::with_observability`]
/// to opt out.
#[derive(Clone)]
pub struct DbosObservability {
    inner: Option<Arc<DbosObservabilityInner>>,
}

impl Default for DbosObservability {
    fn default() -> Self {
        Self::new()
    }
}

struct DbosObservabilityInner {
    metrics: DbosMetrics,
    span_collector: Arc<SpanCollector>,
}

struct DbosMetrics {
    operation_counters: CounterSet,
    operation_duration_us: DynamicDistribution,
    active_operations: DynamicGaugeI64,
    operation_duration_series: [[OnceLock<DynamicDistributionSeries>; DbosOperationOutcome::COUNT]; DbosOperation::COUNT],
    active_operation_series: [OnceLock<DynamicGaugeI64Series>; DbosOperation::COUNT],
}

impl DbosMetrics {
    fn new(shard_count: usize) -> Self {
        let operation_duration_us = DynamicDistribution::new(shard_count);
        let active_operations = DynamicGaugeI64::new(shard_count);
        Self {
            operation_counters: CounterSet::new(shard_count, DBOS_OPERATION_COUNTER_COUNT),
            operation_duration_us,
            active_operations,
            operation_duration_series: std::array::from_fn(|_| std::array::from_fn(|_| OnceLock::new())),
            active_operation_series: std::array::from_fn(|_| OnceLock::new()),
        }
    }

    fn inc_operation_started(&self, operation: DbosOperation) {
        self.operation_counters.inc(started_counter_index(operation));
    }

    fn inc_operation_finished(&self, operation: DbosOperation, outcome: DbosOperationOutcome) {
        self.operation_counters.inc(finished_counter_index(operation, outcome));
    }

    fn record_operation_duration(&self, operation: DbosOperation, outcome: DbosOperationOutcome, duration_us: u64) {
        self.operation_duration_series[operation.index()][outcome.index()]
            .get_or_init(|| self.operation_duration_us.series(&operation_outcome_labels(operation, outcome)))
            .record(duration_us);
    }

    fn add_active_operation(&self, operation: DbosOperation, value: i64) {
        self.active_operation_series[operation.index()]
            .get_or_init(|| self.active_operations.series(&operation_labels(operation)))
            .add(value);
    }

    fn operations_started_sum(&self) -> isize {
        DBOS_OPERATIONS.iter().map(|operation| self.operation_counters.sum(started_counter_index(*operation))).sum()
    }

    fn operations_finished_sum(&self) -> isize {
        DBOS_OPERATIONS
            .iter()
            .flat_map(|operation| DBOS_OPERATION_OUTCOMES.iter().map(move |outcome| (*operation, *outcome)))
            .map(|(operation, outcome)| self.operation_counters.sum(finished_counter_index(operation, outcome)))
            .sum()
    }

    fn operations_finished_with_outcome(&self, outcome: DbosOperationOutcome) -> isize {
        DBOS_OPERATIONS.iter().map(|operation| self.operation_counters.sum(finished_counter_index(*operation, outcome))).sum()
    }

    fn operation_duration_count_sum(&self) -> (u64, u64) {
        self.operation_duration_us.snapshot().into_iter().fold((0u64, 0u64), |(count_acc, sum_acc), (_, count, sum, _)| {
            (count_acc.saturating_add(count), sum_acc.saturating_add(sum))
        })
    }

    fn export_operations_started_prometheus(&self, output: &mut String) {
        const NAME: &str = "dbos_operations_started_total";
        export_counter_header(output, NAME, "Total DBOS operations started, labelled by operation.");
        for operation in DBOS_OPERATIONS {
            let value = self.operation_counters.sum(started_counter_index(operation));
            if value != 0 {
                push_operation_counter_sample(output, NAME, operation, value);
            }
        }
    }

    fn export_operations_finished_prometheus(&self, output: &mut String) {
        const NAME: &str = "dbos_operations_finished_total";
        export_counter_header(output, NAME, "Total DBOS operations finished, labelled by operation and outcome.");
        for operation in DBOS_OPERATIONS {
            for outcome in DBOS_OPERATION_OUTCOMES {
                let value = self.operation_counters.sum(finished_counter_index(operation, outcome));
                if value != 0 {
                    push_operation_outcome_counter_sample(output, NAME, operation, outcome, value);
                }
            }
        }
    }
}

impl std::fmt::Debug for DbosObservability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbosObservability").field("enabled", &self.is_enabled()).finish_non_exhaustive()
    }
}

impl DbosObservability {
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub(crate) async fn observe_result<T, Fut>(
        &self,
        operation: DbosOperation,
        attributes: Vec<DbosSpanAttribute>,
        future: Fut,
    ) -> Result<T>
    where
        Fut: Future<Output = Result<T>>,
    {
        let mut guard = self.start_operation(operation, attributes);
        let result = future.await;
        match &result {
            Ok(_) => guard.finish_success(),
            Err(error) => guard.finish_error(error),
        }
        result
    }

    pub(crate) fn start_operation(&self, operation: DbosOperation, attributes: Vec<DbosSpanAttribute>) -> DbosOperationGuard {
        {
            let Some(inner) = &self.inner else {
                return DbosOperationGuard::noop();
            };
            inner.metrics.inc_operation_started(operation);
            inner.metrics.add_active_operation(operation, 1);

            let mut span = inner.span_collector.start_span(operation.span_name(), operation.span_kind().into());
            span.enter();
            span.set_attribute("dbos.operation", operation.as_str());
            for attribute in attributes {
                span.set_attribute(attribute.key, attribute.value);
            }

            DbosOperationGuard {
                inner: Some(DbosOperationGuardInner {
                    metrics: Arc::clone(inner),
                    operation,
                    started_at: Instant::now(),
                    span: Some(span),
                    finished: false,
                }),
            }
        }
    }
}

impl DbosObservability {
    pub fn new() -> Self {
        Self::with_shard_count(DEFAULT_SHARD_COUNT)
    }

    pub fn with_shard_count(shard_count: usize) -> Self {
        let shard_count = shard_count.max(1);
        Self {
            inner: Some(Arc::new(DbosObservabilityInner {
                metrics: DbosMetrics::new(shard_count),
                span_collector: Arc::new(SpanCollector::new(shard_count, 8192)),
            })),
        }
    }

    pub fn span_collector(&self) -> Option<&Arc<SpanCollector>> {
        self.inner.as_ref().map(|inner| &inner.span_collector)
    }

    pub fn drain_spans(&self) -> Vec<fast_telemetry::CompletedSpan> {
        let mut spans = Vec::new();
        if let Some(inner) = &self.inner {
            inner.span_collector.flush_local();
            inner.span_collector.drain_into(&mut spans);
        }
        spans
    }

    pub fn snapshot(&self) -> DbosTelemetrySnapshot {
        let Some(inner) = &self.inner else {
            return DbosTelemetrySnapshot::default();
        };
        let duration = inner.metrics.operation_duration_count_sum();

        DbosTelemetrySnapshot {
            operations_started: inner.metrics.operations_started_sum(),
            operations_finished: inner.metrics.operations_finished_sum(),
            operations_failed: inner.metrics.operations_finished_with_outcome(DbosOperationOutcome::Error),
            operations_cached: inner.metrics.operations_finished_with_outcome(DbosOperationOutcome::Cached),
            active_operations: inner.metrics.active_operations.sum_all(),
            operation_duration_count: duration.0,
            operation_duration_sum_us: duration.1,
            spans_recorded: inner.span_collector.recorded_count(),
            spans_sampled_out: inner.span_collector.sampled_out_count(),
        }
    }

    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();
        let Some(inner) = &self.inner else {
            return output;
        };
        inner.metrics.export_operations_started_prometheus(&mut output);
        inner.metrics.export_operations_finished_prometheus(&mut output);
        inner.metrics.operation_duration_us.export_prometheus(
            &mut output,
            "dbos_operation_duration_us",
            "DBOS operation duration in microseconds, labelled by operation and outcome.",
        );
        inner.metrics.active_operations.export_prometheus(
            &mut output,
            "dbos_active_operations",
            "Current active DBOS operations, labelled by operation.",
        );
        output
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct DbosTelemetrySnapshot {
    pub operations_started: isize,
    pub operations_finished: isize,
    pub operations_failed: isize,
    pub operations_cached: isize,
    pub active_operations: i64,
    pub operation_duration_count: u64,
    pub operation_duration_sum_us: u64,
    pub spans_recorded: u64,
    pub spans_sampled_out: u64,
}

pub(crate) struct DbosOperationGuard {
    inner: Option<DbosOperationGuardInner>,
}

struct DbosOperationGuardInner {
    metrics: Arc<DbosObservabilityInner>,
    operation: DbosOperation,
    started_at: Instant,
    span: Option<Span>,
    finished: bool,
}

impl DbosOperationGuard {
    fn noop() -> Self {
        Self { inner: None }
    }

    pub(crate) fn finish_success(&mut self) {
        self.finish(DbosOperationOutcome::Success, None);
    }

    pub(crate) fn finish_cached(&mut self) {
        self.finish(DbosOperationOutcome::Cached, None);
    }

    pub(crate) fn finish_cancelled(&mut self) {
        self.finish(DbosOperationOutcome::Cancelled, None);
    }

    pub(crate) fn finish_error(&mut self, error: &DbosError) {
        self.finish(DbosOperationOutcome::Error, Some(error.to_string()));
    }

    fn finish(&mut self, outcome: DbosOperationOutcome, error: Option<String>) {
        {
            if let Some(inner) = &mut self.inner {
                if inner.finished {
                    return;
                }
                inner.finished = true;
                let duration_us = duration_us(inner.started_at);
                let outcome_str = outcome.as_str();
                inner.metrics.metrics.inc_operation_finished(inner.operation, outcome);
                inner.metrics.metrics.record_operation_duration(inner.operation, outcome, duration_us);
                inner.metrics.metrics.add_active_operation(inner.operation, -1);
                if let Some(span) = &mut inner.span {
                    span.set_attribute("dbos.outcome", outcome_str);
                    span.set_attribute("dbos.duration_us", duration_us as i64);
                    match outcome {
                        DbosOperationOutcome::Error => {
                            let message = error.unwrap_or_else(|| "DBOS operation failed".to_string());
                            span.set_attribute("error.message", message.clone());
                            span.set_status(SpanStatus::Error { message: message.into() });
                        }
                        DbosOperationOutcome::Success | DbosOperationOutcome::Cached => {
                            span.set_status(SpanStatus::Ok);
                        }
                        DbosOperationOutcome::Cancelled | DbosOperationOutcome::Dropped => {}
                    }
                }
            }
        }
    }
}

impl Drop for DbosOperationGuard {
    fn drop(&mut self) {
        self.finish(DbosOperationOutcome::Dropped, None);
    }
}

fn duration_us(started_at: Instant) -> u64 {
    let micros = started_at.elapsed().as_micros();
    u64::try_from(micros).unwrap_or(u64::MAX)
}

fn export_counter_header(output: &mut String, name: &str, help: &str) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push_str("\n# TYPE ");
    output.push_str(name);
    output.push_str(" counter\n");
}

fn push_operation_counter_sample(output: &mut String, name: &str, operation: DbosOperation, value: isize) {
    output.push_str(name);
    output.push_str("{operation=\"");
    output.push_str(operation.as_str());
    output.push_str("\"} ");
    output.push_str(&value.to_string());
    output.push('\n');
}

fn push_operation_outcome_counter_sample(
    output: &mut String,
    name: &str,
    operation: DbosOperation,
    outcome: DbosOperationOutcome,
    value: isize,
) {
    output.push_str(name);
    output.push_str("{operation=\"");
    output.push_str(operation.as_str());
    output.push_str("\",outcome=\"");
    output.push_str(outcome.as_str());
    output.push_str("\"} ");
    output.push_str(&value.to_string());
    output.push('\n');
}

pub(crate) fn log_dbos_launched(app_name: &str, app_version: &str, executor_id: &str) {
    tracing::info!(app_name, app_version, executor_id, "DBOS launched");
}

pub(crate) fn log_workflow_execution_failed(workflow_id: &str, error: &DbosError) {
    tracing::error!(%error, workflow_id, "workflow execution failed");
}

pub(crate) fn log_supervisor_iteration_failed(supervisor: &str, error: &DbosError) {
    tracing::warn!(%error, supervisor, "DBOS supervisor iteration failed");
}

pub(crate) fn log_dequeued_workflow(queue: &str, workflow_id: &str) {
    tracing::debug!(queue, workflow_id, "dequeued workflow");
}

pub(crate) fn log_invalid_schedule(schedule_name: &str, error: &dyn std::fmt::Display) {
    tracing::warn!(%error, schedule = schedule_name, "skipping invalid DBOS schedule");
}

pub(crate) fn log_transient_retry(operation: &str, error: &DbosError, resource_id: &str) {
    tracing::warn!(%error, operation, resource_id, "retrying DBOS operation after transient database error");
}

#[cfg(feature = "admin")]
pub(crate) fn log_admin_warning(message: &'static str, error: &DbosError) {
    tracing::warn!(%error, "{message}");
}

#[cfg(any(feature = "admin", feature = "postgres"))]
pub(crate) fn log_database_warning(message: &'static str, error: &dyn std::fmt::Display) {
    tracing::warn!(%error, "{message}");
}
