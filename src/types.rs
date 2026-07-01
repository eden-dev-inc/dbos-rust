use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Current execution state of a workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkflowStatusType {
    Pending,
    Enqueued,
    Delayed,
    Success,
    Error,
    Cancelled,
    MaxRecoveryAttemptsExceeded,
}

impl WorkflowStatusType {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Success | Self::Error | Self::Cancelled | Self::MaxRecoveryAttemptsExceeded)
    }
}

/// Stored workflow state and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStatus {
    pub workflow_uuid: String,
    pub status: WorkflowStatusType,
    pub name: String,
    pub authenticated_user: Option<String>,
    pub assumed_role: Option<String>,
    pub authenticated_roles: Vec<String>,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub executor_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub application_version: String,
    pub application_id: Option<String>,
    pub attempts: u32,
    pub queue_name: Option<String>,
    #[serde(with = "duration_millis_option")]
    pub timeout: Option<Duration>,
    pub deadline: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub deduplication_id: Option<String>,
    pub input: Option<Value>,
    pub priority: Option<i64>,
    pub queue_partition_key: Option<String>,
    pub forked_from: Option<String>,
    pub was_forked_from: bool,
    pub parent_workflow_id: Option<String>,
    pub class_name: Option<String>,
    pub config_name: Option<String>,
    pub serialization: String,
    pub delay_until: Option<DateTime<Utc>>,
}

impl WorkflowStatus {
    pub fn new(
        workflow_uuid: impl Into<String>,
        name: impl Into<String>,
        application_version: impl Into<String>,
        serialization: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            workflow_uuid: workflow_uuid.into(),
            status: WorkflowStatusType::Pending,
            name: name.into(),
            authenticated_user: None,
            assumed_role: None,
            authenticated_roles: Vec::new(),
            output: None,
            error: None,
            executor_id: None,
            created_at: now,
            updated_at: now,
            application_version: application_version.into(),
            application_id: None,
            attempts: 0,
            queue_name: None,
            timeout: None,
            deadline: None,
            started_at: None,
            completed_at: None,
            deduplication_id: None,
            input: None,
            priority: None,
            queue_partition_key: None,
            forked_from: None,
            was_forked_from: false,
            parent_workflow_id: None,
            class_name: None,
            config_name: None,
            serialization: serialization.into(),
            delay_until: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepInfo {
    pub workflow_uuid: String,
    pub step_id: i32,
    pub step_name: String,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub child_workflow_id: Option<String>,
    pub serialization: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRegistryEntry {
    pub fully_qualified_name: String,
    pub name: String,
    pub class_name: Option<String>,
    pub config_name: Option<String>,
    pub max_retries: Option<u32>,
    pub schedule: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimiter {
    pub limit: u32,
    #[serde(with = "duration_millis")]
    pub period: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueConflictResolution {
    UpdateIfLatestVersion,
    AlwaysUpdate,
    NeverUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowQueue {
    pub name: String,
    pub worker_concurrency: Option<u32>,
    pub global_concurrency: Option<u32>,
    pub priority_enabled: bool,
    pub rate_limit: Option<RateLimiter>,
    pub max_tasks_per_iteration: u32,
    pub partition_queue: bool,
    #[serde(with = "duration_millis")]
    pub polling_interval: Duration,
    #[serde(default = "default_queue_max_polling_interval", with = "duration_millis")]
    pub max_polling_interval: Duration,
    pub on_conflict: QueueConflictResolution,
}

impl WorkflowQueue {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            worker_concurrency: None,
            global_concurrency: None,
            priority_enabled: false,
            rate_limit: None,
            max_tasks_per_iteration: 100,
            partition_queue: false,
            polling_interval: Duration::from_secs(1),
            max_polling_interval: default_queue_max_polling_interval(),
            on_conflict: QueueConflictResolution::UpdateIfLatestVersion,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn global_concurrency(&self) -> Option<u32> {
        self.global_concurrency
    }

    pub fn worker_concurrency(&self) -> Option<u32> {
        self.worker_concurrency
    }

    pub fn rate_limit(&self) -> Option<&RateLimiter> {
        self.rate_limit.as_ref()
    }

    pub fn priority_enabled(&self) -> bool {
        self.priority_enabled
    }

    pub fn partition_queue(&self) -> bool {
        self.partition_queue
    }

    pub fn polling_interval(&self) -> Duration {
        self.polling_interval
    }

    pub fn max_polling_interval(&self) -> Duration {
        self.max_polling_interval
    }
}

fn default_queue_max_polling_interval() -> Duration {
    Duration::from_secs(30)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ScheduleStatus {
    Active,
    Paused,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowSchedule {
    pub schedule_id: String,
    pub schedule_name: String,
    pub workflow_name: String,
    pub workflow_class_name: Option<String>,
    pub schedule: String,
    pub status: ScheduleStatus,
    pub context: Option<Value>,
    pub last_fired_at: Option<DateTime<Utc>>,
    pub automatic_backfill: bool,
    pub cron_timezone: Option<String>,
    pub queue_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledWorkflowInput {
    pub scheduled_time: DateTime<Utc>,
    pub context: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateScheduleRequest {
    pub schedule_name: String,
    pub schedule: String,
    pub workflow_name: String,
    pub context: Option<Value>,
    pub automatic_backfill: bool,
    pub cron_timezone: Option<String>,
    pub queue_name: Option<String>,
    pub workflow_class_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct WorkflowOptions {
    pub workflow_id: Option<String>,
    pub queue_name: Option<String>,
    pub application_version: Option<String>,
    pub deduplication_id: Option<String>,
    pub priority: Option<i64>,
    pub timeout: Option<Duration>,
    pub delay: Option<Duration>,
    pub authenticated_user: Option<String>,
    pub assumed_role: Option<String>,
    pub authenticated_roles: Vec<String>,
    pub queue_partition_key: Option<String>,
    pub class_name: Option<String>,
    pub config_name: Option<String>,
    pub parent_workflow_id: Option<String>,
    pub max_retries: Option<u32>,
    pub portable: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeduplicationPolicy {
    #[default]
    Reject,
    ReturnExisting,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct EnqueueOptions {
    pub workflow_id: Option<String>,
    pub application_version: Option<String>,
    pub deduplication_id: Option<String>,
    pub deduplication_policy: DeduplicationPolicy,
    pub priority: Option<i64>,
    pub timeout: Option<Duration>,
    pub queue_partition_key: Option<String>,
    pub class_name: Option<String>,
    pub config_name: Option<String>,
    pub delay: Option<Duration>,
    pub authenticated_user: Option<String>,
    pub assumed_role: Option<String>,
    pub authenticated_roles: Vec<String>,
    pub portable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ResumeWorkflowOptions {
    pub queue_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DeleteWorkflowOptions {
    pub force: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct SetWorkflowDelayOptions {
    pub delay: Option<Duration>,
    pub delay_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ListWorkflowsOptions {
    pub workflow_ids: Vec<String>,
    pub authenticated_user: Option<String>,
    pub authenticated_users: Vec<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub status: Vec<WorkflowStatusType>,
    pub application_version: Option<String>,
    pub application_versions: Vec<String>,
    pub workflow_name: Option<String>,
    pub workflow_names: Vec<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub sort_desc: bool,
    pub workflow_id_prefix: Option<String>,
    pub workflow_id_prefixes: Vec<String>,
    pub load_input: bool,
    pub load_output: bool,
    pub queue_name: Option<String>,
    pub queue_names: Vec<String>,
    pub queues_only: bool,
    pub deduplication_id: Option<String>,
    pub deduplication_ids: Vec<String>,
    pub executor_ids: Vec<String>,
    pub forked_from: Vec<String>,
    pub parent_workflow_ids: Vec<String>,
    pub completed_after: Option<DateTime<Utc>>,
    pub completed_before: Option<DateTime<Utc>>,
    pub dequeued_after: Option<DateTime<Utc>>,
    pub dequeued_before: Option<DateTime<Utc>>,
    pub was_forked_from: Option<bool>,
    pub has_parent: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct GetResultOptions {
    #[serde(with = "duration_millis_option")]
    pub timeout: Option<Duration>,
    #[serde(with = "duration_millis")]
    pub polling_interval: Duration,
}

impl Default for GetResultOptions {
    fn default() -> Self {
        Self { timeout: None, polling_interval: Duration::from_millis(250) }
    }
}

impl GetResultOptions {
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_polling_interval(mut self, polling_interval: Duration) -> Self {
        if !polling_interval.is_zero() {
            self.polling_interval = polling_interval;
        }
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct GetWorkflowStepsOptions {
    pub load_output: Option<bool>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ReadStreamOptions {
    pub snapshot: bool,
    pub from_offset: i64,
}

impl ReadStreamOptions {
    pub fn snapshot_from_offset(from_offset: i64) -> Self {
        Self { snapshot: true, from_offset }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct SendOptions {
    pub portable: bool,
}

impl SendOptions {
    pub fn portable() -> Self {
        Self { portable: true }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct SetEventOptions {
    pub portable: bool,
}

impl SetEventOptions {
    pub fn portable() -> Self {
        Self { portable: true }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct WriteStreamOptions {
    pub portable: bool,
}

impl WriteStreamOptions {
    pub fn portable() -> Self {
        Self { portable: true }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ListSchedulesOptions {
    pub statuses: Vec<ScheduleStatus>,
    pub workflow_names: Vec<String>,
    pub schedule_name_prefixes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ListRegisteredWorkflowsOptions {
    pub scheduled_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ForkWorkflowInput {
    pub original_workflow_id: String,
    pub start_step: Option<u32>,
    pub forked_workflow_id: Option<String>,
    pub application_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowAggregateRow {
    pub bucket: BTreeMap<String, Value>,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepAggregateRow {
    pub bucket: BTreeMap<String, Value>,
    pub count: u64,
    pub max_duration_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct GetWorkflowAggregatesInput {
    pub workflow_ids: Vec<String>,
    pub workflow_id_prefix: Option<String>,
    pub workflow_id_prefixes: Vec<String>,
    pub workflow_names: Vec<String>,
    pub queue_names: Vec<String>,
    pub executor_ids: Vec<String>,
    pub application_versions: Vec<String>,
    pub status: Vec<WorkflowStatusType>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub completed_after: Option<DateTime<Utc>>,
    pub completed_before: Option<DateTime<Utc>>,
    #[serde(with = "duration_millis_option")]
    pub time_bucket_size: Option<Duration>,
    pub group_by_status: bool,
    pub group_by_workflow_name: bool,
    pub group_by_queue_name: bool,
    pub group_by_executor_id: bool,
    pub group_by_application_version: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct GetStepAggregatesInput {
    pub workflow_ids: Vec<String>,
    pub workflow_id_prefix: Option<String>,
    pub workflow_id_prefixes: Vec<String>,
    pub function_names: Vec<String>,
    pub statuses: Vec<String>,
    pub completed_after: Option<DateTime<Utc>>,
    pub completed_before: Option<DateTime<Utc>>,
    #[serde(with = "duration_millis_option")]
    pub time_bucket_size: Option<Duration>,
    pub group_by_function_name: bool,
    pub group_by_status: bool,
    pub select_count: bool,
    pub select_max_duration_ms: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ExportWorkflowOptions {
    pub include_children: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransactionOptions {
    pub isolation_level: Option<TransactionIsolationLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkflowExport {
    pub workflow: WorkflowStatus,
    pub steps: Vec<StepInfo>,
    pub events: Vec<WorkflowEvent>,
    pub messages: Vec<WorkflowMessage>,
    pub streams: Vec<StreamEntry>,
    pub children: Vec<WorkflowExport>,
}

impl Default for WorkflowExport {
    fn default() -> Self {
        Self {
            workflow: WorkflowStatus::new("", "", "local", crate::serialization::DBOS_JSON),
            steps: Vec::new(),
            events: Vec::new(),
            messages: Vec::new(),
            streams: Vec::new(),
            children: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PortableWorkflowArgs {
    #[serde(rename = "positionalArgs")]
    pub positional_args: Vec<Value>,
    #[serde(rename = "namedArgs")]
    pub named_args: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortableWorkflowError {
    pub name: String,
    pub message: String,
    pub code: Option<Value>,
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamValue<T> {
    pub value: Option<T>,
    pub closed: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEntry {
    pub workflow_uuid: String,
    pub key: String,
    pub offset: i64,
    pub value: Option<Value>,
    pub serialization: String,
    pub closed: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowMessage {
    pub destination_id: String,
    pub topic: String,
    pub message: Value,
    pub serialization: String,
    pub created_at: DateTime<Utc>,
    pub consumed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowEvent {
    pub workflow_uuid: String,
    pub key: String,
    pub value: Value,
    pub serialization: String,
    pub created_at: DateTime<Utc>,
}

pub(crate) mod duration_millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(value.as_millis().min(u128::from(u64::MAX)) as u64)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}

pub(crate) mod duration_millis_option {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = Option::<u64>::deserialize(deserializer)?;
        Ok(millis.map(Duration::from_millis))
    }
}
