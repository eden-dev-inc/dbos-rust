use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::conductor::{AlertHandler, ConductorConfig, ConductorTransport, connect_conductor};
use crate::error::{DbosError, DbosErrorCode, Result};
use crate::observability::{
    DbosObservability, DbosOperation, DbosSpanAttribute, log_dbos_launched, log_dequeued_workflow, log_invalid_schedule,
    log_supervisor_iteration_failed, log_transient_retry, log_workflow_execution_failed,
};
use crate::serialization::{
    CustomSerializer, DBOS_JSON, EncodedValue, PORTABLE_JSON, decode_stored_with_serializer, encode_json_value, encode_portable,
};
use crate::store::{MemoryStore, SystemDatabase, SystemDatabaseHandle, step_counts_by_name, workflow_counts_by_status};
use crate::types::{
    CreateScheduleRequest, DeleteWorkflowOptions, ExportWorkflowOptions, ForkWorkflowInput, GetResultOptions, GetStepAggregatesInput,
    GetWorkflowAggregatesInput, GetWorkflowStepsOptions, ListRegisteredWorkflowsOptions, ListSchedulesOptions, ListWorkflowsOptions,
    QueueConflictResolution, ReadStreamOptions, ResumeWorkflowOptions, ScheduleStatus, SendOptions, SetEventOptions,
    SetWorkflowDelayOptions, StepInfo, StreamEntry, StreamValue, TransactionIsolationLevel, TransactionOptions, VersionInfo,
    WorkflowAggregateRow, WorkflowEvent, WorkflowExport, WorkflowMessage, WorkflowOptions, WorkflowQueue, WorkflowRegistryEntry,
    WorkflowSchedule, WorkflowStatus, WorkflowStatusType, WriteStreamOptions,
};

type BoxWorkflowFuture = Pin<Box<dyn Future<Output = Result<Value>> + Send>>;
type StepAggregateBucket = (BTreeMap<String, Value>, u64, Option<i64>);

#[derive(Clone)]
pub struct DbosConfig {
    pub app_name: String,
    pub database_url: Option<String>,
    pub turso_path: Option<String>,
    pub database_schema: String,
    pub system_database: Option<SystemDatabaseHandle>,
    pub admin_server: bool,
    pub admin_server_port: u16,
    pub conductor_url: Option<String>,
    pub conductor_api_key: Option<String>,
    pub conductor_executor_metadata: Option<Value>,
    pub conductor_alert_handler: Option<AlertHandler>,
    pub conductor_transport: Option<Arc<dyn ConductorTransport>>,
    pub application_version: Option<String>,
    pub executor_id: Option<String>,
    pub application_id: Option<String>,
    pub enable_patching: bool,
    pub scheduler_polling_interval: Duration,
    pub serializer: Option<Arc<dyn CustomSerializer>>,
    pub observability: Option<DbosObservability>,
}

impl std::fmt::Debug for DbosConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbosConfig")
            .field("app_name", &self.app_name)
            .field("database_url", &self.database_url)
            .field("turso_path", &self.turso_path)
            .field("database_schema", &self.database_schema)
            .field("system_database", &self.system_database)
            .field("admin_server", &self.admin_server)
            .field("admin_server_port", &self.admin_server_port)
            .field("conductor_url", &self.conductor_url)
            .field("conductor_api_key", &self.conductor_api_key.as_ref().map(|_| "<redacted>"))
            .field("conductor_executor_metadata", &self.conductor_executor_metadata)
            .field("conductor_alert_handler", &self.conductor_alert_handler.as_ref().map(|_| "<handler>"))
            .field("conductor_transport", &self.conductor_transport.as_ref().map(|_| "<transport>"))
            .field("application_version", &self.application_version)
            .field("executor_id", &self.executor_id)
            .field("application_id", &self.application_id)
            .field("enable_patching", &self.enable_patching)
            .field("scheduler_polling_interval", &self.scheduler_polling_interval)
            .field("serializer", &self.serializer.as_ref().map(|serializer| serializer.name()))
            .field("observability", &self.observability)
            .finish()
    }
}

impl DbosConfig {
    pub fn new(app_name: impl Into<String>) -> Self {
        Self {
            app_name: app_name.into(),
            database_url: None,
            turso_path: None,
            database_schema: "dbos".to_string(),
            system_database: None,
            admin_server: false,
            admin_server_port: 3001,
            conductor_url: None,
            conductor_api_key: None,
            conductor_executor_metadata: None,
            conductor_alert_handler: None,
            conductor_transport: None,
            application_version: None,
            executor_id: None,
            application_id: None,
            enable_patching: false,
            scheduler_polling_interval: Duration::from_secs(30),
            serializer: None,
            observability: None,
        }
    }

    pub fn with_database_url(mut self, database_url: impl Into<String>) -> Self {
        self.database_url = Some(database_url.into());
        self
    }

    pub fn with_turso_path(mut self, turso_path: impl Into<String>) -> Self {
        self.turso_path = Some(turso_path.into());
        self
    }

    pub fn with_system_database(mut self, system_database: SystemDatabaseHandle) -> Self {
        self.system_database = Some(system_database);
        self
    }

    pub fn with_serializer(mut self, serializer: Arc<dyn CustomSerializer>) -> Self {
        self.serializer = Some(serializer);
        self
    }

    pub fn with_observability(mut self, observability: DbosObservability) -> Self {
        self.observability = Some(observability);
        self
    }

    pub fn with_conductor(mut self, url: impl Into<String>, api_key: impl Into<String>) -> Self {
        self.conductor_url = Some(url.into());
        self.conductor_api_key = Some(api_key.into());
        self
    }

    pub fn with_conductor_executor_metadata(mut self, metadata: impl Serialize) -> Result<Self> {
        self.conductor_executor_metadata = Some(serde_json::to_value(metadata)?);
        Ok(self)
    }

    pub fn with_conductor_alert_handler(mut self, alert_handler: AlertHandler) -> Self {
        self.conductor_alert_handler = Some(alert_handler);
        self
    }

    pub fn with_conductor_transport(mut self, transport: Arc<dyn ConductorTransport>) -> Self {
        self.conductor_transport = Some(transport);
        self
    }
}

struct WorkflowRunState {
    workflow_id: String,
    step_id: AtomicI32,
    authenticated_user: Option<String>,
    assumed_role: Option<String>,
    authenticated_roles: Vec<String>,
    portable: bool,
}

impl WorkflowRunState {
    fn next_step_id(&self) -> i32 {
        self.step_id.fetch_add(1, Ordering::SeqCst) + 1
    }
}

#[derive(Clone)]
pub struct DbosContext {
    inner: Arc<DbosInner>,
    run_state: Option<Arc<WorkflowRunState>>,
    context_values: Arc<BTreeMap<String, Value>>,
    deadline: Option<DateTime<Utc>>,
    cancelled: Arc<AtomicBool>,
    cancel_cause: Arc<RwLock<Option<String>>>,
}

struct DbosInner {
    config: DbosConfig,
    store: Arc<dyn SystemDatabase>,
    workflows: RwLock<HashMap<String, Arc<dyn RunnableWorkflow>>>,
    workflow_aliases: RwLock<HashMap<String, String>>,
    workflow_registry: RwLock<HashMap<String, WorkflowRegistryEntry>>,
    listened_queues: RwLock<HashSet<String>>,
    launched: AtomicBool,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    conductor_handle: Mutex<Option<crate::conductor::ConductorHandle>>,
    observability: DbosObservability,
}

#[derive(Clone, Debug)]
pub struct DbosCancelHandle {
    cancelled: Arc<AtomicBool>,
    cause: Arc<RwLock<Option<String>>>,
}

impl DbosCancelHandle {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub async fn cancel_with_cause(&self, cause: impl Into<String>) {
        *self.cause.write().await = Some(cause.into());
        self.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub async fn cause(&self) -> Option<String> {
        self.cause.read().await.clone()
    }
}

#[async_trait]
trait RunnableWorkflow: Send + Sync {
    async fn run(&self, ctx: DbosContext, input: Value) -> Result<Value>;
}

struct TypedWorkflow<I, O, F> {
    name: String,
    _class_name: Option<String>,
    _config_name: Option<String>,
    handler: F,
    _input: PhantomData<I>,
    _output: PhantomData<O>,
}

#[async_trait]
impl<I, O, F> RunnableWorkflow for TypedWorkflow<I, O, F>
where
    I: DeserializeOwned + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(DbosContext, I) -> BoxWorkflowFuture + Send + Sync + 'static,
{
    async fn run(&self, ctx: DbosContext, input: Value) -> Result<Value> {
        let input = serde_json::from_value::<I>(input).map_err(|err| {
            DbosError::with_source(
                DbosErrorCode::WorkflowUnexpectedType,
                format!("workflow {} received an unexpected input type", self.name),
                err,
            )
        })?;
        (self.handler)(ctx, input).await
    }
}

#[derive(Debug, Clone, Default)]
pub struct WorkflowRegistrationOptions {
    pub name: Option<String>,
    pub class_name: Option<String>,
    pub config_name: Option<String>,
    pub max_retries: Option<u32>,
    pub schedule: Option<String>,
}

pub type StepRetryPredicate = Arc<dyn Fn(&DbosError) -> bool + Send + Sync>;

#[derive(Clone)]
pub struct StepOptions {
    pub max_retries: u32,
    pub backoff_factor: f64,
    pub base_interval: Duration,
    pub max_interval: Duration,
    pub next_step_id: Option<i32>,
    pub retry_predicate: Option<StepRetryPredicate>,
}

impl Default for StepOptions {
    fn default() -> Self {
        Self {
            max_retries: 0,
            backoff_factor: 2.0,
            base_interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(5),
            next_step_id: None,
            retry_predicate: None,
        }
    }
}

impl StepOptions {
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn with_backoff_factor(mut self, backoff_factor: f64) -> Self {
        if backoff_factor.is_finite() && backoff_factor > 0.0 {
            self.backoff_factor = backoff_factor;
        }
        self
    }

    pub fn with_base_interval(mut self, base_interval: Duration) -> Self {
        if !base_interval.is_zero() {
            self.base_interval = base_interval;
        }
        self
    }

    pub fn with_max_interval(mut self, max_interval: Duration) -> Self {
        if !max_interval.is_zero() {
            self.max_interval = max_interval;
        }
        self
    }

    pub fn with_next_step_id(mut self, next_step_id: i32) -> Self {
        self.next_step_id = Some(next_step_id);
        self
    }

    pub fn with_retry_predicate<F>(mut self, retry_predicate: F) -> Self
    where
        F: Fn(&DbosError) -> bool + Send + Sync + 'static,
    {
        self.retry_predicate = Some(Arc::new(retry_predicate));
        self
    }
}

impl DbosContext {
    pub async fn new(config: DbosConfig) -> Result<Self> {
        if config.app_name.is_empty() {
            return Err(DbosError::initialization("missing required app_name"));
        }

        let mut config = config;
        if config.database_schema.is_empty() {
            config.database_schema = "dbos".to_string();
        }
        if config.admin_server_port == 0 {
            config.admin_server_port = 3001;
        }
        if config.scheduler_polling_interval.is_zero() {
            config.scheduler_polling_interval = Duration::from_secs(30);
        }
        if config.application_version.is_none() {
            config.application_version =
                std::env::var("DBOS__APPVERSION").ok().filter(|value| !value.is_empty()).or_else(|| Some("local".to_string()));
        }
        if config.executor_id.is_none() {
            config.executor_id = std::env::var("DBOS__VMID").ok().filter(|value| !value.is_empty()).or_else(|| Some("local".to_string()));
        }
        if config.application_id.is_none() {
            config.application_id = std::env::var("DBOS__APPID").ok().filter(|value| !value.is_empty());
        }

        let store = build_store(&config).await?;
        let observability = config.observability.clone().unwrap_or_default();
        Ok(Self {
            inner: Arc::new(DbosInner {
                config,
                store,
                workflows: RwLock::new(HashMap::new()),
                workflow_aliases: RwLock::new(HashMap::new()),
                workflow_registry: RwLock::new(HashMap::new()),
                listened_queues: RwLock::new(HashSet::new()),
                launched: AtomicBool::new(false),
                tasks: Mutex::new(Vec::new()),
                conductor_handle: Mutex::new(None),
                observability,
            }),
            run_state: None,
            context_values: Arc::new(BTreeMap::new()),
            deadline: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            cancel_cause: Arc::new(RwLock::new(None)),
        })
    }

    pub fn app_name(&self) -> &str {
        &self.inner.config.app_name
    }

    pub fn application_version(&self) -> &str {
        self.inner.config.application_version.as_deref().unwrap_or("local")
    }

    pub fn executor_id(&self) -> &str {
        self.inner.config.executor_id.as_deref().unwrap_or("local")
    }

    pub fn application_id(&self) -> Option<&str> {
        self.inner.config.application_id.as_deref()
    }

    pub fn conductor_executor_metadata(&self) -> Option<&Value> {
        self.inner.config.conductor_executor_metadata.as_ref()
    }

    pub fn observability(&self) -> &DbosObservability {
        &self.inner.observability
    }

    pub fn with_value(&self, key: impl Into<String>, value: impl Serialize) -> Result<Self> {
        let mut values = self.context_values.as_ref().clone();
        values.insert(key.into(), serde_json::to_value(value)?);
        Ok(Self {
            inner: Arc::clone(&self.inner),
            run_state: self.run_state.clone(),
            context_values: Arc::new(values),
            deadline: self.deadline,
            cancelled: Arc::clone(&self.cancelled),
            cancel_cause: Arc::clone(&self.cancel_cause),
        })
    }

    pub fn value<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.context_values.get(key).cloned().map(serde_json::from_value).transpose().map_err(DbosError::from)
    }

    pub fn deadline(&self) -> Option<DateTime<Utc>> {
        self.deadline
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub fn with_cancel(&self) -> (Self, DbosCancelHandle) {
        let cancelled = Arc::new(AtomicBool::new(false));
        let cause = Arc::new(RwLock::new(None));
        (
            Self {
                inner: Arc::clone(&self.inner),
                run_state: self.run_state.clone(),
                context_values: Arc::clone(&self.context_values),
                deadline: self.deadline,
                cancelled: Arc::clone(&cancelled),
                cancel_cause: Arc::clone(&cause),
            },
            DbosCancelHandle { cancelled, cause },
        )
    }

    pub fn with_cancel_cause(&self) -> (Self, DbosCancelHandle) {
        self.with_cancel()
    }

    pub async fn cancel_cause(&self) -> Option<String> {
        self.cancel_cause.read().await.clone()
    }

    pub fn without_cancel(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            run_state: self.run_state.clone(),
            context_values: Arc::clone(&self.context_values),
            deadline: self.deadline,
            cancelled: Arc::new(AtomicBool::new(false)),
            cancel_cause: Arc::new(RwLock::new(None)),
        }
    }

    pub fn with_timeout(&self, timeout: Duration) -> (Self, DbosCancelHandle) {
        let (mut ctx, handle) = self.with_cancel();
        ctx.deadline = chrono::Duration::from_std(timeout).ok().map(|duration| Utc::now() + duration);
        (ctx, handle)
    }

    pub async fn launch(&self) -> Result<()> {
        let observability = self.inner.observability.clone();
        let conductor_config = self.conductor_config()?;
        let result = observability
            .observe_result(
                DbosOperation::Launch,
                vec![
                    DbosSpanAttribute::new("dbos.app_name", self.app_name()),
                    DbosSpanAttribute::new("dbos.app_version", self.application_version()),
                    DbosSpanAttribute::new("dbos.executor_id", self.executor_id()),
                ],
                async {
                    if self.inner.launched.swap(true, Ordering::SeqCst) {
                        return Err(DbosError::initialization("DBOS is already launched"));
                    }
                    let launch_result = async {
                        self.initialize_system_database().await?;

                        let queue_ctx = self.clone();
                        let queue_task = tokio::spawn(async move {
                            queue_ctx.queue_supervisor().await;
                        });
                        let schedule_ctx = self.clone();
                        let schedule_task = tokio::spawn(async move {
                            schedule_ctx.schedule_supervisor().await;
                        });
                        self.inner.tasks.lock().await.extend([queue_task, schedule_task]);
                        if let Some(config) = conductor_config {
                            let handle = connect_conductor(self.clone(), config).await?;
                            *self.inner.conductor_handle.lock().await = Some(handle);
                        }
                        self.recover_pending_workflows(&[self.executor_id().to_string()]).await?;
                        Ok(())
                    }
                    .await;
                    if launch_result.is_err() {
                        self.shutdown(Duration::from_secs(1)).await;
                    }
                    launch_result
                },
            )
            .await;
        if result.is_ok() {
            log_dbos_launched(self.app_name(), self.application_version(), self.executor_id());
        }
        result
    }

    pub(crate) async fn initialize_system_database(&self) -> Result<()> {
        self.inner.store.migrate().await?;
        let now = Utc::now();
        self.inner
            .store
            .create_application_version(VersionInfo {
                name: self.application_version().to_string(),
                created_at: now,
                updated_at: now,
            })
            .await
    }

    pub async fn shutdown(&self, timeout: Duration) {
        self.inner.launched.store(false, Ordering::SeqCst);
        if let Some(handle) = self.inner.conductor_handle.lock().await.take() {
            let _ = handle.shutdown(timeout).await;
        }
        let mut tasks = self.inner.tasks.lock().await;
        let handles = std::mem::take(&mut *tasks);
        drop(tasks);
        for handle in handles {
            handle.abort();
            let _ = tokio::time::timeout(timeout, handle).await;
        }
    }

    fn conductor_config(&self) -> Result<Option<ConductorConfig>> {
        let url = self.inner.config.conductor_url.as_ref().filter(|value| !value.is_empty());
        let api_key = self.inner.config.conductor_api_key.as_ref().filter(|value| !value.is_empty());
        match (url, api_key) {
            (Some(url), Some(api_key)) => Ok(Some(ConductorConfig {
                url: url.clone(),
                api_key: api_key.clone(),
                app_name: self.app_name().to_string(),
                executor_metadata: self.inner.config.conductor_executor_metadata.clone(),
                alert_handler: self.inner.config.conductor_alert_handler.clone(),
                transport: self.inner.config.conductor_transport.clone(),
                reconnect_interval: Duration::from_secs(5),
            })),
            (None, None) => Ok(None),
            _ => Err(DbosError::invalid_argument("conductor URL and API key must be configured together")),
        }
    }

    pub async fn register_workflow<I, O, F, Fut>(
        &self,
        name: impl Into<String>,
        handler: F,
        options: WorkflowRegistrationOptions,
    ) -> Result<()>
    where
        I: DeserializeOwned + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        F: Fn(DbosContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        if self.inner.launched.load(Ordering::SeqCst) {
            return Err(DbosError::new(
                DbosErrorCode::ConflictingRegistration,
                "cannot register workflow after DBOS has launched",
            ));
        }
        let fqn = name.into();
        let public_name = options.name.clone().unwrap_or_else(|| fqn.clone());
        if let Some(schedule) = &options.schedule {
            validate_cron(schedule)?;
        }
        let wrapped = move |ctx: DbosContext, input: I| -> BoxWorkflowFuture {
            let fut = handler(ctx, input);
            Box::pin(async move {
                let output = fut.await?;
                serde_json::to_value(output).map_err(DbosError::from)
            })
        };
        let workflow = TypedWorkflow::<I, O, _> {
            name: public_name.clone(),
            _class_name: options.class_name.clone(),
            _config_name: options.config_name.clone(),
            handler: wrapped,
            _input: PhantomData,
            _output: PhantomData,
        };

        let mut workflows = self.inner.workflows.write().await;
        if workflows.contains_key(&fqn) {
            return Err(DbosError::new(DbosErrorCode::ConflictingRegistration, format!("{fqn} is already registered")));
        }
        workflows.insert(fqn.clone(), Arc::new(workflow));
        drop(workflows);

        self.inner.workflow_registry.write().await.insert(
            fqn.clone(),
            WorkflowRegistryEntry {
                fully_qualified_name: fqn.clone(),
                name: public_name.clone(),
                class_name: options.class_name.clone(),
                config_name: options.config_name.clone(),
                max_retries: options.max_retries,
                schedule: options.schedule.clone(),
            },
        );

        let mut aliases = self.inner.workflow_aliases.write().await;
        let alias = if let Some(config_name) = options.config_name.filter(|name| !name.is_empty()) {
            format!("{public_name}/{config_name}")
        } else {
            public_name
        };
        if aliases.insert(alias.clone(), fqn.clone()).is_some() {
            return Err(DbosError::new(DbosErrorCode::ConflictingRegistration, format!("{alias} is already registered")));
        }
        aliases.entry(fqn.clone()).or_insert(fqn);
        Ok(())
    }

    fn serializer(&self) -> Option<&dyn CustomSerializer> {
        self.inner.config.serializer.as_deref()
    }

    fn encode_value(&self, value: &Value, portable: bool) -> Result<EncodedValue> {
        if portable {
            return encode_portable(value);
        }
        if let Some(serializer) = self.serializer() {
            return serializer.encode_value(value);
        }
        encode_json_value(value)
    }

    fn encode_serializable<T: Serialize>(&self, value: &T, portable: bool) -> Result<EncodedValue> {
        let value = serde_json::to_value(value)?;
        self.encode_value(&value, portable)
    }

    fn encoded_to_stored_value(encoded: EncodedValue) -> Option<Value> {
        encoded.data.map(Value::String)
    }

    fn decode_encoded<T: DeserializeOwned>(&self, encoded: &EncodedValue) -> Result<T> {
        decode_stored_with_serializer(encoded, self.serializer())
    }

    fn decode_stored_value<T: DeserializeOwned>(&self, value: Option<Value>, serialization: &str) -> Result<T> {
        match value {
            Some(Value::String(data)) => self.decode_encoded(&EncodedValue { data: Some(data), serialization: serialization.to_string() }),
            Some(Value::Null) | None => self.decode_encoded(&EncodedValue { data: None, serialization: serialization.to_string() }),
            Some(raw) if serialization.is_empty() || serialization == DBOS_JSON || serialization == PORTABLE_JSON => {
                serde_json::from_value(raw).map_err(DbosError::from)
            }
            Some(raw) => self.decode_encoded(&EncodedValue {
                data: Some(raw.to_string()),
                serialization: serialization.to_string(),
            }),
        }
    }

    pub async fn run_workflow<I, O>(&self, name: impl Into<String>, input: I, options: WorkflowOptions) -> Result<WorkflowHandle<O>>
    where
        I: Serialize + Send,
        O: DeserializeOwned + Send + 'static,
    {
        let input = serde_json::to_value(input)?;
        let handle = self.run_workflow_value(name.into(), input, options).await?;
        Ok(handle.cast())
    }

    pub async fn run_workflow_value(&self, name: String, input: Value, options: WorkflowOptions) -> Result<WorkflowHandle<Value>> {
        let workflow_id = options.workflow_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
        let observability = self.inner.observability.clone();
        observability
            .observe_result(
                DbosOperation::RunWorkflow,
                vec![
                    DbosSpanAttribute::new("dbos.workflow_id", workflow_id.clone()),
                    DbosSpanAttribute::new("dbos.workflow_name", name.clone()),
                ],
                async {
                    let now = Utc::now();
                    let portable = options.portable || self.run_state.as_ref().is_some_and(|state| state.portable);
                    let encoded_input = self.encode_value(&input, portable)?;
                    let mut status = WorkflowStatus::new(
                        workflow_id.clone(),
                        name.clone(),
                        options.application_version.clone().unwrap_or_else(|| self.application_version().to_string()),
                        encoded_input.serialization.clone(),
                    );
                    status.input = Self::encoded_to_stored_value(encoded_input);
                    status.queue_name = options.queue_name.clone();
                    status.deduplication_id = options.deduplication_id.clone();
                    status.priority = options.priority;
                    status.timeout = options.timeout;
                    status.deadline =
                        options.timeout.map(|timeout| chrono::Duration::from_std(timeout).map_or(now, |duration| now + duration));
                    status.delay_until =
                        options.delay.map(|delay| chrono::Duration::from_std(delay).map_or(now, |duration| now + duration));
                    status.queue_partition_key = options.queue_partition_key.clone();
                    status.authenticated_user = options
                        .authenticated_user
                        .clone()
                        .or_else(|| self.run_state.as_ref().and_then(|state| state.authenticated_user.clone()));
                    status.assumed_role =
                        options.assumed_role.clone().or_else(|| self.run_state.as_ref().and_then(|state| state.assumed_role.clone()));
                    status.authenticated_roles = if options.authenticated_roles.is_empty() {
                        self.run_state.as_ref().map(|state| state.authenticated_roles.clone()).unwrap_or_default()
                    } else {
                        options.authenticated_roles.clone()
                    };
                    status.class_name = options.class_name.clone();
                    status.config_name = options.config_name.clone();
                    status.parent_workflow_id =
                        options.parent_workflow_id.clone().or_else(|| self.run_state.as_ref().map(|state| state.workflow_id.clone()));
                    if status.delay_until.is_some() {
                        status.status = WorkflowStatusType::Delayed;
                    } else if status.queue_name.is_some() {
                        status.status = WorkflowStatusType::Enqueued;
                    }
                    self.inner.store.insert_workflow(status).await?;

                    if options.queue_name.is_none() && options.delay.is_none() {
                        self.spawn_workflow_execution(workflow_id.clone()).await;
                    }

                    Ok(WorkflowHandle::new(self.clone(), workflow_id))
                },
            )
            .await
    }

    pub async fn retrieve_workflow<O>(&self, workflow_id: impl Into<String>) -> WorkflowHandle<O>
    where
        O: DeserializeOwned + Send + 'static,
    {
        WorkflowHandle::new(self.clone(), workflow_id.into())
    }

    pub async fn cancel_workflow(&self, workflow_id: &str) -> Result<()> {
        let mut workflow =
            self.inner.store.get_workflow(workflow_id).await?.ok_or_else(|| DbosError::non_existent_workflow(workflow_id))?;
        if !workflow.status.is_terminal() {
            workflow.status = WorkflowStatusType::Cancelled;
            workflow.completed_at = Some(Utc::now());
            workflow.updated_at = Utc::now();
            self.inner.store.save_workflow(workflow).await?;
        }
        Ok(())
    }

    pub async fn cancel_workflows(&self, workflow_ids: &[String]) -> Result<()> {
        for workflow_id in workflow_ids {
            self.cancel_workflow(workflow_id).await?;
        }
        Ok(())
    }

    pub async fn set_workflow_delay(&self, workflow_id: &str, options: SetWorkflowDelayOptions) -> Result<()> {
        let mut workflow =
            self.inner.store.get_workflow(workflow_id).await?.ok_or_else(|| DbosError::non_existent_workflow(workflow_id))?;
        if workflow.status != WorkflowStatusType::Delayed {
            return Err(DbosError::invalid_argument("workflow delay can only be updated for DELAYED workflows"));
        }
        let delay_until = if let Some(delay_until) = options.delay_until {
            delay_until
        } else if let Some(delay) = options.delay {
            Utc::now()
                + chrono::Duration::from_std(delay).map_err(|err| DbosError::invalid_argument(format!("invalid delay duration: {err}")))?
        } else {
            Utc::now()
        };
        workflow.delay_until = Some(delay_until);
        workflow.updated_at = Utc::now();
        self.inner.store.save_workflow(workflow).await
    }

    pub async fn resume_workflow<O>(&self, workflow_id: &str, options: ResumeWorkflowOptions) -> Result<WorkflowHandle<O>>
    where
        O: DeserializeOwned + Send + 'static,
    {
        let mut workflow =
            self.inner.store.get_workflow(workflow_id).await?.ok_or_else(|| DbosError::non_existent_workflow(workflow_id))?;
        let should_spawn = options.queue_name.is_none();
        workflow.status = if let Some(queue_name) = options.queue_name {
            workflow.queue_name = Some(queue_name);
            WorkflowStatusType::Enqueued
        } else {
            WorkflowStatusType::Pending
        };
        workflow.completed_at = None;
        workflow.error = None;
        workflow.updated_at = Utc::now();
        self.inner.store.save_workflow(workflow).await?;
        if should_spawn {
            self.spawn_workflow_execution(workflow_id.to_string()).await;
        }
        Ok(WorkflowHandle::new(self.clone(), workflow_id.to_string()))
    }

    pub async fn resume_workflows<O>(&self, workflow_ids: &[String], options: ResumeWorkflowOptions) -> Result<Vec<WorkflowHandle<O>>>
    where
        O: DeserializeOwned + Send + 'static,
    {
        let mut handles = Vec::with_capacity(workflow_ids.len());
        for workflow_id in workflow_ids {
            handles.push(self.resume_workflow(workflow_id, options.clone()).await?);
        }
        Ok(handles)
    }

    pub async fn delete_workflows(&self, workflow_ids: &[String], options: DeleteWorkflowOptions) -> Result<()> {
        self.inner.store.delete_workflows(workflow_ids, &options).await
    }

    pub async fn fork_workflow<O>(&self, input: ForkWorkflowInput) -> Result<WorkflowHandle<O>>
    where
        O: DeserializeOwned + Send + 'static,
    {
        let original = self
            .inner
            .store
            .get_workflow(&input.original_workflow_id)
            .await?
            .ok_or_else(|| DbosError::non_existent_workflow(&input.original_workflow_id))?;
        let mut fork = original;
        fork.workflow_uuid = input.forked_workflow_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        fork.forked_from = Some(input.original_workflow_id);
        fork.was_forked_from = true;
        fork.status = WorkflowStatusType::Pending;
        fork.application_version = input.application_version.unwrap_or_else(|| self.application_version().to_string());
        fork.created_at = Utc::now();
        fork.updated_at = fork.created_at;
        fork.completed_at = None;
        fork.error = None;
        fork.output = None;
        self.inner.store.insert_workflow(fork.clone()).await?;
        self.spawn_workflow_execution(fork.workflow_uuid.clone()).await;
        Ok(WorkflowHandle::new(self.clone(), fork.workflow_uuid))
    }

    pub async fn list_workflows(&self, options: ListWorkflowsOptions) -> Result<Vec<WorkflowStatus>> {
        self.inner.store.list_workflows(&options).await
    }

    pub async fn list_registered_workflows(&self, options: ListRegisteredWorkflowsOptions) -> Result<Vec<WorkflowRegistryEntry>> {
        let mut entries = self.inner.workflow_registry.read().await.values().cloned().collect::<Vec<_>>();
        if options.scheduled_only {
            entries.retain(|entry| entry.schedule.is_some());
        }
        entries.sort_by(|left, right| left.fully_qualified_name.cmp(&right.fully_qualified_name));
        Ok(entries)
    }

    pub async fn list_registered_queues(&self) -> Result<Vec<WorkflowQueue>> {
        self.list_queues().await
    }

    pub async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        self.get_workflow_steps_with_options(workflow_id, GetWorkflowStepsOptions::default()).await
    }

    pub async fn get_workflow_steps_with_options(&self, workflow_id: &str, options: GetWorkflowStepsOptions) -> Result<Vec<StepInfo>> {
        let mut steps = self.inner.store.list_steps(workflow_id).await?;
        steps.sort_by_key(|step| step.step_id);
        let offset = options.offset.unwrap_or(0).min(steps.len());
        if offset > 0 {
            steps.drain(0..offset);
        }
        if let Some(limit) = options.limit
            && steps.len() > limit
        {
            steps.truncate(limit);
        }
        if options.load_output == Some(false) {
            for step in &mut steps {
                step.output = None;
            }
        }
        Ok(steps)
    }

    pub async fn export_workflow(&self, workflow_id: &str) -> Result<WorkflowExport> {
        self.export_workflow_with_options(workflow_id, ExportWorkflowOptions::default()).await
    }

    pub async fn export_workflow_with_options(&self, workflow_id: &str, options: ExportWorkflowOptions) -> Result<WorkflowExport> {
        self.export_workflow_inner(workflow_id.to_string(), options).await
    }

    fn export_workflow_inner(
        &self,
        workflow_id: String,
        options: ExportWorkflowOptions,
    ) -> Pin<Box<dyn Future<Output = Result<WorkflowExport>> + Send + '_>> {
        Box::pin(async move {
            let workflow =
                self.inner.store.get_workflow(&workflow_id).await?.ok_or_else(|| DbosError::non_existent_workflow(&workflow_id))?;
            let steps = self.get_workflow_steps(&workflow_id).await?;
            let events = self.inner.store.list_events(&workflow_id).await?;
            let messages = self.inner.store.list_messages(&workflow_id).await?;
            let streams = self.inner.store.list_streams(&workflow_id).await?;
            let mut children = Vec::new();
            if options.include_children {
                let child_workflows = self
                    .list_workflows(ListWorkflowsOptions {
                        parent_workflow_ids: vec![workflow_id.to_string()],
                        load_input: true,
                        load_output: true,
                        ..Default::default()
                    })
                    .await?;
                for child in child_workflows {
                    children.push(self.export_workflow_inner(child.workflow_uuid, options.clone()).await?);
                }
            }
            Ok(WorkflowExport { workflow, steps, events, messages, streams, children })
        })
    }

    pub async fn import_workflow(&self, export: WorkflowExport) -> Result<()> {
        self.import_workflow_inner(export).await
    }

    fn import_workflow_inner(&self, export: WorkflowExport) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            let workflow_id = export.workflow.workflow_uuid.clone();
            self.inner.store.insert_workflow(export.workflow).await?;
            for step in export.steps {
                if step.workflow_uuid != workflow_id {
                    return Err(DbosError::invalid_argument(format!(
                        "imported step {} belongs to workflow {}, expected {}",
                        step.step_id, step.workflow_uuid, workflow_id
                    )));
                }
                self.inner.store.record_step(step).await?;
            }
            for event in export.events {
                if event.workflow_uuid != workflow_id {
                    return Err(DbosError::invalid_argument(format!(
                        "imported event {} belongs to workflow {}, expected {}",
                        event.key, event.workflow_uuid, workflow_id
                    )));
                }
                self.inner.store.set_event(event).await?;
            }
            for stream in export.streams {
                if stream.workflow_uuid != workflow_id {
                    return Err(DbosError::invalid_argument(format!(
                        "imported stream {} belongs to workflow {}, expected {}",
                        stream.key, stream.workflow_uuid, workflow_id
                    )));
                }
                self.inner.store.write_stream(stream).await?;
            }
            for message in export.messages {
                if message.destination_id != workflow_id {
                    return Err(DbosError::invalid_argument(format!(
                        "imported message for workflow {}, expected {}",
                        message.destination_id, workflow_id
                    )));
                }
                self.inner.store.send_message(message).await?;
            }
            for child in export.children {
                self.import_workflow_inner(child).await?;
            }
            Ok(())
        })
    }

    pub async fn get_workflow_aggregates(&self, options: ListWorkflowsOptions) -> Result<Vec<WorkflowAggregateRow>> {
        let workflows = self.list_workflows(options).await?;
        Ok(workflow_counts_by_status(&workflows))
    }

    pub async fn get_workflow_aggregates_with_input(&self, input: GetWorkflowAggregatesInput) -> Result<Vec<WorkflowAggregateRow>> {
        let workflows = self
            .list_workflows(ListWorkflowsOptions {
                workflow_ids: input.workflow_ids,
                workflow_id_prefix: input.workflow_id_prefix,
                workflow_id_prefixes: input.workflow_id_prefixes,
                workflow_names: input.workflow_names,
                queue_names: input.queue_names,
                executor_ids: input.executor_ids,
                application_versions: input.application_versions,
                status: input.status,
                start_time: input.start_time,
                end_time: input.end_time,
                completed_after: input.completed_after,
                completed_before: input.completed_before,
                load_input: false,
                load_output: false,
                ..Default::default()
            })
            .await?;
        let group_any = input.group_by_status
            || input.group_by_workflow_name
            || input.group_by_queue_name
            || input.group_by_executor_id
            || input.group_by_application_version
            || input.time_bucket_size.is_some();
        let mut counts: BTreeMap<String, (BTreeMap<String, Value>, u64)> = BTreeMap::new();
        for workflow in workflows {
            let mut bucket = BTreeMap::new();
            if !group_any || input.group_by_status {
                bucket.insert("status".to_string(), Value::String(format!("{:?}", workflow.status)));
            }
            if input.group_by_workflow_name {
                bucket.insert("workflow_name".to_string(), Value::String(workflow.name));
            }
            if input.group_by_queue_name {
                bucket.insert("queue_name".to_string(), workflow.queue_name.map(Value::String).unwrap_or(Value::Null));
            }
            if input.group_by_executor_id {
                bucket.insert("executor_id".to_string(), workflow.executor_id.map(Value::String).unwrap_or(Value::Null));
            }
            if input.group_by_application_version {
                bucket.insert("application_version".to_string(), Value::String(workflow.application_version));
            }
            if let Some(size) = input.time_bucket_size {
                bucket.insert("created_at_bucket".to_string(), time_bucket_value(workflow.created_at, size)?);
            }
            let key = serde_json::to_string(&bucket)?;
            let entry = counts.entry(key).or_insert((bucket, 0));
            entry.1 += 1;
        }
        Ok(counts.into_values().map(|(bucket, count)| WorkflowAggregateRow { bucket, count }).collect())
    }

    pub async fn get_step_aggregates(&self, workflow_id: &str) -> Result<Vec<crate::types::StepAggregateRow>> {
        let steps = self.get_workflow_steps(workflow_id).await?;
        Ok(step_counts_by_name(&steps))
    }

    pub async fn get_step_aggregates_with_input(&self, input: GetStepAggregatesInput) -> Result<Vec<crate::types::StepAggregateRow>> {
        let workflow_id_prefix = input.workflow_id_prefix.clone();
        let workflow_id_prefixes = input.workflow_id_prefixes.clone();
        let workflows = self
            .list_workflows(ListWorkflowsOptions {
                workflow_ids: input.workflow_ids.clone(),
                workflow_id_prefix,
                workflow_id_prefixes,
                load_input: false,
                load_output: false,
                ..Default::default()
            })
            .await?;
        let mut aggregates: BTreeMap<String, StepAggregateBucket> = BTreeMap::new();
        let group_any = input.group_by_function_name || input.group_by_status || input.time_bucket_size.is_some();
        for workflow in workflows {
            for step in self.get_workflow_steps(&workflow.workflow_uuid).await? {
                if !input.function_names.is_empty() && !input.function_names.contains(&step.step_name) {
                    continue;
                }
                let step_status = if step.error.is_some() { "ERROR" } else { "SUCCESS" };
                if !input.statuses.is_empty() && !input.statuses.iter().any(|status| status.eq_ignore_ascii_case(step_status)) {
                    continue;
                }
                if let Some(completed_after) = input.completed_after
                    && step.completed_at < completed_after
                {
                    continue;
                }
                if let Some(completed_before) = input.completed_before
                    && step.completed_at > completed_before
                {
                    continue;
                }
                let mut bucket = BTreeMap::new();
                if !group_any || input.group_by_function_name {
                    bucket.insert("function_name".to_string(), Value::String(step.step_name.clone()));
                }
                if input.group_by_status {
                    bucket.insert("status".to_string(), Value::String(step_status.to_string()));
                }
                if let Some(size) = input.time_bucket_size {
                    bucket.insert("completed_at_bucket".to_string(), time_bucket_value(step.completed_at, size)?);
                }
                let key = serde_json::to_string(&bucket)?;
                let duration = step.completed_at.signed_duration_since(step.started_at).num_milliseconds();
                let entry = aggregates.entry(key).or_insert((bucket, 0, None));
                entry.1 += 1;
                if input.select_max_duration_ms || !input.select_count {
                    entry.2 = Some(entry.2.map_or(duration, |current| current.max(duration)));
                }
            }
        }
        Ok(aggregates
            .into_values()
            .map(|(bucket, count, max_duration_ms)| crate::types::StepAggregateRow { bucket, count, max_duration_ms })
            .collect())
    }

    pub async fn run_as_step<T, F, Fut>(&self, step_name: impl Into<String>, operation: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: FnOnce(DbosContext) -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send,
    {
        let step_name = step_name.into();
        let Some(run_state) = &self.run_state else {
            return self
                .inner
                .observability
                .clone()
                .observe_result(
                    DbosOperation::RunStep,
                    vec![DbosSpanAttribute::new("dbos.step_name", step_name.clone())],
                    operation(self.clone()),
                )
                .await;
        };
        let step_id = run_state.next_step_id();
        let mut operation_guard = self.inner.observability.start_operation(
            DbosOperation::RunStep,
            vec![
                DbosSpanAttribute::new("dbos.workflow_id", run_state.workflow_id.clone()),
                DbosSpanAttribute::new("dbos.step_name", step_name.clone()),
                DbosSpanAttribute::new("dbos.step_id", step_id.to_string()),
            ],
        );
        let recorded_step = match self.inner.store.get_step(&run_state.workflow_id, step_id).await {
            Ok(recorded) => recorded,
            Err(error) => {
                operation_guard.finish_error(&error);
                return Err(error);
            }
        };
        if let Some(recorded) = recorded_step {
            if recorded.step_name != step_name {
                let error = DbosError::unexpected_step(&run_state.workflow_id, step_id, step_name, recorded.step_name);
                operation_guard.finish_error(&error);
                return Err(error);
            }
            let decoded = self.decode_stored_value(recorded.output, &recorded.serialization);
            match &decoded {
                Ok(_) => operation_guard.finish_cached(),
                Err(error) => operation_guard.finish_error(error),
            }
            return decoded;
        }
        let started_at = Utc::now();
        let result = operation(self.clone()).await;
        let completed_at = Utc::now();
        match result {
            Ok(value) => {
                let encoded = self.encode_serializable(&value, run_state.portable)?;
                let output = Self::encoded_to_stored_value(encoded.clone());
                match self
                    .inner
                    .store
                    .record_step(StepInfo {
                        workflow_uuid: run_state.workflow_id.clone(),
                        step_id,
                        step_name,
                        output,
                        error: None,
                        child_workflow_id: None,
                        serialization: encoded.serialization,
                        started_at,
                        completed_at,
                    })
                    .await
                {
                    Ok(()) => operation_guard.finish_success(),
                    Err(error) => {
                        operation_guard.finish_error(&error);
                        return Err(error);
                    }
                };
                Ok(value)
            }
            Err(error) => {
                let record_result = self
                    .inner
                    .store
                    .record_step(StepInfo {
                        workflow_uuid: run_state.workflow_id.clone(),
                        step_id,
                        step_name: step_name.clone(),
                        output: None,
                        error: Some(error.to_string()),
                        child_workflow_id: None,
                        serialization: DBOS_JSON.to_string(),
                        started_at,
                        completed_at,
                    })
                    .await;
                if let Err(record_error) = record_result {
                    operation_guard.finish_error(&record_error);
                    return Err(record_error);
                }
                let step_error = DbosError::step_execution(&run_state.workflow_id, step_name, error.to_string());
                operation_guard.finish_error(&step_error);
                Err(step_error)
            }
        }
    }

    pub async fn run_as_step_with_options<T, F, Fut>(&self, step_name: impl Into<String>, options: StepOptions, operation: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: Fn(DbosContext) -> Fut + Send + Sync,
        Fut: Future<Output = Result<T>> + Send,
    {
        let step_name = step_name.into();
        let Some(run_state) = &self.run_state else {
            return self
                .inner
                .observability
                .clone()
                .observe_result(
                    DbosOperation::RunStep,
                    vec![DbosSpanAttribute::new("dbos.step_name", step_name.clone())],
                    self.retry_step_operation(options, operation),
                )
                .await;
        };
        let step_id = options.next_step_id.unwrap_or_else(|| run_state.next_step_id());
        let mut operation_guard = self.inner.observability.start_operation(
            DbosOperation::RunStep,
            vec![
                DbosSpanAttribute::new("dbos.workflow_id", run_state.workflow_id.clone()),
                DbosSpanAttribute::new("dbos.step_name", step_name.clone()),
                DbosSpanAttribute::new("dbos.step_id", step_id.to_string()),
            ],
        );
        let recorded_step = match self.inner.store.get_step(&run_state.workflow_id, step_id).await {
            Ok(recorded) => recorded,
            Err(error) => {
                operation_guard.finish_error(&error);
                return Err(error);
            }
        };
        if let Some(recorded) = recorded_step {
            if recorded.step_name != step_name {
                let error = DbosError::unexpected_step(&run_state.workflow_id, step_id, step_name, recorded.step_name);
                operation_guard.finish_error(&error);
                return Err(error);
            }
            let decoded = self.decode_stored_value(recorded.output, &recorded.serialization);
            match &decoded {
                Ok(_) => operation_guard.finish_cached(),
                Err(error) => operation_guard.finish_error(error),
            }
            return decoded;
        }
        let started_at = Utc::now();
        let result = self.retry_step_operation(options, operation).await;
        let completed_at = Utc::now();
        match result {
            Ok(value) => {
                let encoded = self.encode_serializable(&value, run_state.portable)?;
                let output = Self::encoded_to_stored_value(encoded.clone());
                match self
                    .inner
                    .store
                    .record_step(StepInfo {
                        workflow_uuid: run_state.workflow_id.clone(),
                        step_id,
                        step_name,
                        output,
                        error: None,
                        child_workflow_id: None,
                        serialization: encoded.serialization,
                        started_at,
                        completed_at,
                    })
                    .await
                {
                    Ok(()) => operation_guard.finish_success(),
                    Err(error) => {
                        operation_guard.finish_error(&error);
                        return Err(error);
                    }
                };
                Ok(value)
            }
            Err(error) => {
                let record_result = self
                    .inner
                    .store
                    .record_step(StepInfo {
                        workflow_uuid: run_state.workflow_id.clone(),
                        step_id,
                        step_name: step_name.clone(),
                        output: None,
                        error: Some(error.to_string()),
                        serialization: DBOS_JSON.to_string(),
                        started_at,
                        completed_at,
                        child_workflow_id: None,
                    })
                    .await;
                if let Err(record_error) = record_result {
                    operation_guard.finish_error(&record_error);
                    return Err(record_error);
                }
                let step_error = DbosError::step_execution(&run_state.workflow_id, step_name, error.to_string());
                operation_guard.finish_error(&step_error);
                Err(step_error)
            }
        }
    }

    async fn retry_step_operation<T, F, Fut>(&self, options: StepOptions, operation: F) -> Result<T>
    where
        F: Fn(DbosContext) -> Fut + Send + Sync,
        Fut: Future<Output = Result<T>> + Send,
    {
        let mut retries = 0;
        loop {
            match operation(self.clone()).await {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let should_retry = retries < options.max_retries
                        && options.retry_predicate.as_ref().is_none_or(|retry_predicate| retry_predicate(&error));
                    if !should_retry {
                        return Err(error);
                    }
                    retries += 1;
                    tokio::time::sleep(step_retry_delay(&options, retries)).await;
                }
            }
        }
    }

    pub async fn run_as_transaction<T, F, Fut>(&self, step_name: impl Into<String>, operation: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: FnOnce(DbosTransaction) -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send,
    {
        self.run_as_transaction_with_options(step_name, TransactionOptions::default(), operation).await
    }

    pub async fn run_as_transaction_with_options<T, F, Fut>(
        &self,
        step_name: impl Into<String>,
        options: TransactionOptions,
        operation: F,
    ) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: FnOnce(DbosTransaction) -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send,
    {
        self.run_as_step(step_name, |ctx| async move {
            operation(DbosTransaction { ctx, isolation_level: options.isolation_level }).await
        })
        .await
    }

    pub async fn sleep(&self, duration: Duration) -> Result<Duration> {
        let millis = self
            .run_as_step("DBOS.sleep", move |_ctx| async move {
                tokio::time::sleep(duration).await;
                let millis = u64::try_from(duration.as_millis())
                    .map_err(|_| DbosError::invalid_argument("sleep duration is too large to checkpoint"))?;
                Ok(millis)
            })
            .await?;
        Ok(Duration::from_millis(millis))
    }

    pub async fn spawn_step<T, F, Fut>(&self, step_name: impl Into<String>, operation: F) -> Result<JoinHandle<Result<T>>>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: FnOnce(DbosContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let ctx = self.clone();
        let step_name = step_name.into();
        Ok(tokio::spawn(async move { ctx.run_as_step(step_name, operation).await }))
    }

    pub async fn select_step<T>(&self, handles: Vec<JoinHandle<Result<T>>>) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        self.run_as_step("DBOS.select", |_ctx| async move {
            let mut pending = handles.into_iter().collect::<FuturesUnordered<_>>();
            let Some(joined) = pending.next().await else {
                return Err(DbosError::invalid_argument("select_step requires at least one handle"));
            };
            match joined {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(DbosError::workflow_execution("select", format!("step task join failed: {error}"))),
            }
        })
        .await
    }

    pub fn current_workflow_id(&self) -> Result<String> {
        self.run_state
            .as_ref()
            .map(|state| state.workflow_id.clone())
            .ok_or_else(|| DbosError::invalid_argument("current workflow ID is only available inside workflows"))
    }

    pub fn current_step_id(&self) -> Result<i32> {
        self.run_state
            .as_ref()
            .map(|state| state.step_id.load(Ordering::SeqCst))
            .ok_or_else(|| DbosError::invalid_argument("current step ID is only available inside workflows"))
    }

    pub async fn register_queue(&self, queue: WorkflowQueue) -> Result<WorkflowQueue> {
        validate_queue(&queue)?;
        if queue.on_conflict == QueueConflictResolution::NeverUpdate && self.inner.store.get_queue(&queue.name).await?.is_some() {
            return self
                .inner
                .store
                .get_queue(&queue.name)
                .await?
                .ok_or_else(|| DbosError::database("queue disappeared during registration"));
        }
        self.inner.store.upsert_queue(queue.clone()).await?;
        Ok(queue)
    }

    pub async fn retrieve_queue(&self, name: &str) -> Result<Option<WorkflowQueue>> {
        self.inner.store.get_queue(name).await
    }

    pub async fn list_queues(&self) -> Result<Vec<WorkflowQueue>> {
        self.inner.store.list_queues().await
    }

    pub async fn delete_queue(&self, name: &str) -> Result<()> {
        self.inner.store.delete_queue(name).await
    }

    pub async fn update_queue(&self, queue: WorkflowQueue) -> Result<WorkflowQueue> {
        validate_queue(&queue)?;
        if self.inner.store.get_queue(&queue.name).await?.is_none() {
            return Err(DbosError::invalid_argument(format!("queue {} is not registered", queue.name)));
        }
        self.inner.store.upsert_queue(queue.clone()).await?;
        Ok(queue)
    }

    async fn update_queue_config<F>(&self, name: &str, mutate: F) -> Result<WorkflowQueue>
    where
        F: FnOnce(&mut WorkflowQueue),
    {
        let mut queue = self
            .inner
            .store
            .get_queue(name)
            .await?
            .ok_or_else(|| DbosError::invalid_argument(format!("queue {name} is not registered")))?;
        mutate(&mut queue);
        self.update_queue(queue).await
    }

    pub async fn listen_queues(&self, queues: &[WorkflowQueue]) {
        let mut listened = self.inner.listened_queues.write().await;
        for queue in queues {
            listened.insert(queue.name.clone());
        }
    }

    pub async fn send<T: Serialize>(&self, destination_id: &str, message: T, topic: &str) -> Result<()> {
        self.send_with_options(destination_id, message, topic, SendOptions::default()).await
    }

    pub async fn send_with_options<T: Serialize>(&self, destination_id: &str, message: T, topic: &str, options: SendOptions) -> Result<()> {
        self.inner
            .observability
            .clone()
            .observe_result(
                DbosOperation::MessageSend,
                vec![
                    DbosSpanAttribute::new("dbos.destination_id", destination_id),
                    DbosSpanAttribute::new("dbos.topic", topic),
                ],
                async {
                    let encoded = self.encode_serializable(&message, options.portable)?;
                    let value = encoded.data.map(Value::String).unwrap_or(Value::Null);
                    self.inner
                        .store
                        .send_message(WorkflowMessage {
                            destination_id: destination_id.to_string(),
                            topic: topic.to_string(),
                            message: value,
                            serialization: encoded.serialization,
                            created_at: Utc::now(),
                            consumed: false,
                        })
                        .await
                },
            )
            .await
    }

    pub async fn recv<T: DeserializeOwned>(&self, topic: &str, timeout: Duration) -> Result<T> {
        let workflow_id = self.current_workflow_id()?;
        self.inner
            .observability
            .clone()
            .observe_result(
                DbosOperation::MessageReceive,
                vec![
                    DbosSpanAttribute::new("dbos.workflow_id", workflow_id.clone()),
                    DbosSpanAttribute::new("dbos.topic", topic),
                ],
                async {
                    let deadline = tokio::time::Instant::now() + timeout;
                    loop {
                        match self.inner.store.recv_message(&workflow_id, topic).await {
                            Ok(Some(message)) => {
                                return self.decode_stored_value(Some(message.message), &message.serialization);
                            }
                            Ok(None) => {}
                            Err(error) if is_transient_database_error(&error) => {
                                log_transient_retry("recv", &error, &workflow_id);
                            }
                            Err(error) => return Err(error),
                        }
                        if tokio::time::Instant::now() >= deadline {
                            return Err(DbosError::timeout(format!("timed out receiving topic {topic}")));
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
            )
            .await
    }

    pub async fn set_event<T: Serialize>(&self, key: &str, value: T) -> Result<()> {
        self.set_event_with_options(key, value, SetEventOptions::default()).await
    }

    pub async fn set_event_with_options<T: Serialize>(&self, key: &str, value: T, options: SetEventOptions) -> Result<()> {
        let workflow_id = self.current_workflow_id()?;
        self.inner
            .observability
            .clone()
            .observe_result(
                DbosOperation::EventSet,
                vec![
                    DbosSpanAttribute::new("dbos.workflow_id", workflow_id.clone()),
                    DbosSpanAttribute::new("dbos.event_key", key),
                ],
                async {
                    let encoded = self.encode_serializable(&value, options.portable)?;
                    self.inner
                        .store
                        .set_event(WorkflowEvent {
                            workflow_uuid: workflow_id,
                            key: key.to_string(),
                            value: encoded.data.map(Value::String).unwrap_or(Value::Null),
                            serialization: encoded.serialization,
                            created_at: Utc::now(),
                        })
                        .await
                },
            )
            .await
    }

    pub async fn get_event<T: DeserializeOwned>(&self, target_workflow_id: &str, key: &str, timeout: Duration) -> Result<T> {
        self.inner
            .observability
            .clone()
            .observe_result(
                DbosOperation::EventGet,
                vec![
                    DbosSpanAttribute::new("dbos.workflow_id", target_workflow_id),
                    DbosSpanAttribute::new("dbos.event_key", key),
                ],
                async {
                    let deadline = tokio::time::Instant::now() + timeout;
                    loop {
                        match self.inner.store.get_event(target_workflow_id, key).await {
                            Ok(Some(event)) => {
                                return self.decode_stored_value(Some(event.value), &event.serialization);
                            }
                            Ok(None) => {}
                            Err(error) if is_transient_database_error(&error) => {
                                log_transient_retry("get_event", &error, target_workflow_id);
                            }
                            Err(error) => return Err(error),
                        }
                        if tokio::time::Instant::now() >= deadline {
                            return Err(DbosError::timeout(format!("timed out getting event {key} from workflow {target_workflow_id}")));
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
            )
            .await
    }

    pub async fn write_stream<T: Serialize>(&self, key: &str, value: T) -> Result<()> {
        self.write_stream_with_options(key, value, WriteStreamOptions::default()).await
    }

    pub async fn write_stream_with_options<T: Serialize>(&self, key: &str, value: T, options: WriteStreamOptions) -> Result<()> {
        let workflow_id = self.current_workflow_id()?;
        self.inner
            .observability
            .clone()
            .observe_result(
                DbosOperation::StreamWrite,
                vec![
                    DbosSpanAttribute::new("dbos.workflow_id", workflow_id.clone()),
                    DbosSpanAttribute::new("dbos.stream_key", key),
                ],
                async {
                    let existing = self.inner.store.read_stream(&workflow_id, key).await?;
                    let offset = i64::try_from(existing.len()).map_err(|_| DbosError::invalid_argument("stream offset overflow"))?;
                    let encoded = self.encode_serializable(&value, options.portable)?;
                    self.inner
                        .store
                        .write_stream(StreamEntry {
                            workflow_uuid: workflow_id,
                            key: key.to_string(),
                            offset,
                            value: encoded.data.map(Value::String),
                            serialization: encoded.serialization,
                            closed: false,
                            created_at: Utc::now(),
                        })
                        .await
                },
            )
            .await
    }

    pub async fn close_stream(&self, key: &str) -> Result<()> {
        let workflow_id = self.current_workflow_id()?;
        self.inner.store.close_stream(&workflow_id, key).await
    }

    pub async fn read_stream<T: DeserializeOwned>(&self, workflow_id: &str, key: &str) -> Result<(Vec<T>, bool)> {
        self.read_stream_with_options(workflow_id, key, ReadStreamOptions::default()).await
    }

    pub async fn read_stream_with_options<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
        options: ReadStreamOptions,
    ) -> Result<(Vec<T>, bool)> {
        self.inner
            .observability
            .clone()
            .observe_result(
                DbosOperation::StreamRead,
                vec![
                    DbosSpanAttribute::new("dbos.workflow_id", workflow_id),
                    DbosSpanAttribute::new("dbos.stream_key", key),
                ],
                async {
                    if options.from_offset < 0 {
                        return Err(DbosError::invalid_argument("stream from_offset must be non-negative"));
                    }
                    let entries = self.inner.store.read_stream(workflow_id, key).await?;
                    let closed = entries.iter().any(|entry| entry.closed);
                    let mut values = Vec::new();
                    for entry in entries.into_iter().filter(|entry| entry.offset >= options.from_offset && !entry.closed) {
                        values.push(self.decode_stored_value(entry.value, &entry.serialization)?);
                    }
                    Ok((values, closed))
                },
            )
            .await
    }

    pub async fn read_stream_async<T: DeserializeOwned + Send + 'static>(
        &self,
        workflow_id: String,
        key: String,
    ) -> Result<tokio::sync::mpsc::Receiver<StreamValue<T>>> {
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        let ctx = self.clone();
        tokio::spawn(async move {
            let mut sent = 0usize;
            loop {
                match ctx.read_stream::<T>(&workflow_id, &key).await {
                    Ok((values, closed)) => {
                        for value in values.into_iter().skip(sent) {
                            sent += 1;
                            if sender.send(StreamValue { value: Some(value), closed: false, error: None }).await.is_err() {
                                return;
                            }
                        }
                        if closed {
                            let _ = sender.send(StreamValue { value: None, closed: true, error: None }).await;
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(StreamValue { value: None, closed: false, error: Some(error.to_string()) }).await;
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
        Ok(receiver)
    }

    pub async fn create_schedule(&self, request: CreateScheduleRequest) -> Result<()> {
        validate_cron(&request.schedule)?;
        let schedule = WorkflowSchedule {
            schedule_id: Uuid::new_v4().to_string(),
            schedule_name: request.schedule_name,
            workflow_name: request.workflow_name,
            workflow_class_name: request.workflow_class_name,
            schedule: request.schedule,
            status: ScheduleStatus::Active,
            context: request.context,
            last_fired_at: None,
            automatic_backfill: request.automatic_backfill,
            cron_timezone: request.cron_timezone,
            queue_name: request.queue_name,
        };
        self.inner.store.upsert_schedule(schedule).await
    }

    pub async fn apply_schedules(&self, schedules: Vec<CreateScheduleRequest>) -> Result<()> {
        for schedule in schedules {
            self.create_schedule(schedule).await?;
        }
        Ok(())
    }

    pub async fn get_schedule(&self, schedule_name: &str) -> Result<Option<WorkflowSchedule>> {
        self.inner.store.get_schedule(schedule_name).await
    }

    pub async fn list_schedules(&self, options: ListSchedulesOptions) -> Result<Vec<WorkflowSchedule>> {
        self.inner.store.list_schedules(&options).await
    }

    pub async fn pause_schedule(&self, schedule_name: &str) -> Result<()> {
        self.update_schedule_status(schedule_name, ScheduleStatus::Paused).await
    }

    pub async fn resume_schedule(&self, schedule_name: &str) -> Result<()> {
        self.update_schedule_status(schedule_name, ScheduleStatus::Active).await
    }

    pub async fn delete_schedule(&self, schedule_name: &str) -> Result<()> {
        self.inner.store.delete_schedule(schedule_name).await
    }

    pub async fn backfill_schedule(&self, schedule_name: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<String>> {
        let schedule = self
            .get_schedule(schedule_name)
            .await?
            .ok_or_else(|| DbosError::invalid_argument(format!("schedule not found: {schedule_name}")))?;
        let cron = schedule
            .schedule
            .parse::<cron::Schedule>()
            .map_err(|err| DbosError::invalid_argument(format!("invalid cron schedule: {err}")))?;
        let mut ids = Vec::new();
        for scheduled_time in cron.after(&start).take_while(|time| *time <= end) {
            let id = self.enqueue_scheduled_workflow(&schedule, scheduled_time).await?;
            ids.push(id);
        }
        Ok(ids)
    }

    pub async fn trigger_schedule(&self, schedule_name: &str) -> Result<WorkflowHandle<Value>> {
        let schedule = self
            .get_schedule(schedule_name)
            .await?
            .ok_or_else(|| DbosError::invalid_argument(format!("schedule not found: {schedule_name}")))?;
        let id = self.enqueue_scheduled_workflow(&schedule, Utc::now()).await?;
        Ok(WorkflowHandle::new(self.clone(), id))
    }

    pub async fn list_application_versions(&self) -> Result<Vec<VersionInfo>> {
        self.inner.store.list_application_versions().await
    }

    pub async fn get_latest_application_version(&self) -> Result<Option<VersionInfo>> {
        Ok(self.inner.store.list_application_versions().await?.into_iter().next())
    }

    pub async fn set_latest_application_version(&self, version_name: &str) -> Result<()> {
        self.inner.store.set_latest_application_version(version_name).await
    }

    pub async fn patch(&self, patch_name: &str) -> Result<bool> {
        if !self.inner.config.enable_patching {
            return Err(DbosError::new(DbosErrorCode::PatchingNotEnabled, "patching system is not enabled"));
        }
        Ok(self.inner.store.get_patch(patch_name).await?.unwrap_or(false))
    }

    pub async fn deprecate_patch(&self, patch_name: &str) -> Result<()> {
        if !self.inner.config.enable_patching {
            return Err(DbosError::new(DbosErrorCode::PatchingNotEnabled, "patching system is not enabled"));
        }
        self.inner.store.set_patch(patch_name, false).await
    }

    async fn spawn_workflow_execution(&self, workflow_id: String) {
        let ctx = self.clone();
        let handle = tokio::spawn(async move {
            if let Err(error) = ctx.execute_workflow(&workflow_id).await {
                log_workflow_execution_failed(&workflow_id, &error);
            }
        });
        self.inner.tasks.lock().await.push(handle);
    }

    async fn execute_workflow(&self, workflow_id: &str) -> Result<()> {
        let mut operation_guard = self
            .inner
            .observability
            .start_operation(DbosOperation::ExecuteWorkflow, vec![DbosSpanAttribute::new("dbos.workflow_id", workflow_id)]);
        let mut workflow = match self
            .inner
            .store
            .get_workflow(workflow_id)
            .await
            .and_then(|workflow| workflow.ok_or_else(|| DbosError::non_existent_workflow(workflow_id)))
        {
            Ok(workflow) => workflow,
            Err(error) => {
                operation_guard.finish_error(&error);
                return Err(error);
            }
        };
        if workflow.status.is_terminal() {
            if workflow.status == WorkflowStatusType::Cancelled {
                operation_guard.finish_cancelled();
            } else {
                operation_guard.finish_success();
            }
            return Ok(());
        }
        let name = workflow.name.clone();
        let executor = match self.resolve_workflow(&name, workflow.config_name.as_deref()).await {
            Ok(executor) => executor,
            Err(error) => {
                operation_guard.finish_error(&error);
                return Err(error);
            }
        };
        workflow.status = WorkflowStatusType::Pending;
        workflow.executor_id = Some(self.executor_id().to_string());
        workflow.started_at.get_or_insert_with(Utc::now);
        workflow.attempts = workflow.attempts.saturating_add(1);
        workflow.updated_at = Utc::now();
        if let Err(error) = self.inner.store.save_workflow(workflow.clone()).await {
            operation_guard.finish_error(&error);
            return Err(error);
        }

        let child_ctx = self.with_run_state(WorkflowRunState {
            workflow_id: workflow.workflow_uuid.clone(),
            step_id: AtomicI32::new(0),
            authenticated_user: workflow.authenticated_user.clone(),
            assumed_role: workflow.assumed_role.clone(),
            authenticated_roles: workflow.authenticated_roles.clone(),
            portable: workflow.serialization == PORTABLE_JSON,
        });
        let input = match self.decode_stored_value(workflow.input.clone(), &workflow.serialization) {
            Ok(input) => input,
            Err(error) => {
                operation_guard.finish_error(&error);
                return Err(error);
            }
        };
        let result = executor.run(child_ctx, input).await;
        let workflow_succeeded = result.is_ok();
        let mut latest = match self
            .inner
            .store
            .get_workflow(workflow_id)
            .await
            .and_then(|workflow| workflow.ok_or_else(|| DbosError::non_existent_workflow(workflow_id)))
        {
            Ok(latest) => latest,
            Err(error) => {
                operation_guard.finish_error(&error);
                return Err(error);
            }
        };
        if latest.status == WorkflowStatusType::Cancelled {
            operation_guard.finish_cancelled();
            return Ok(());
        }
        latest.updated_at = Utc::now();
        latest.completed_at = Some(latest.updated_at);
        match result {
            Ok(value) => {
                let encoded = match self.encode_value(&value, latest.serialization == PORTABLE_JSON) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        operation_guard.finish_error(&error);
                        return Err(error);
                    }
                };
                latest.status = WorkflowStatusType::Success;
                latest.serialization = encoded.serialization.clone();
                latest.output = Self::encoded_to_stored_value(encoded);
                latest.error = None;
            }
            Err(error) => {
                latest.status = WorkflowStatusType::Error;
                latest.error = Some(error.to_string());
                operation_guard.finish_error(&DbosError::workflow_execution(workflow_id, error.to_string()));
            }
        }
        let save_result = self.inner.store.save_workflow(latest).await;
        match &save_result {
            Ok(()) if workflow_succeeded => operation_guard.finish_success(),
            Ok(()) => {}
            Err(error) => operation_guard.finish_error(error),
        }
        save_result
    }

    fn with_run_state(&self, run_state: WorkflowRunState) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            run_state: Some(Arc::new(run_state)),
            context_values: Arc::clone(&self.context_values),
            deadline: self.deadline,
            cancelled: Arc::clone(&self.cancelled),
            cancel_cause: Arc::clone(&self.cancel_cause),
        }
    }

    async fn resolve_workflow(&self, name: &str, config_name: Option<&str>) -> Result<Arc<dyn RunnableWorkflow>> {
        let lookup = if let Some(config_name) = config_name.filter(|value| !value.is_empty()) {
            format!("{name}/{config_name}")
        } else {
            name.to_string()
        };
        let aliases = self.inner.workflow_aliases.read().await;
        let fqn = aliases.get(&lookup).cloned().unwrap_or(lookup);
        drop(aliases);
        self.inner.workflows.read().await.get(&fqn).cloned().ok_or_else(|| DbosError::non_existent_workflow(name))
    }

    pub async fn recover_pending_workflows(&self, executor_ids: &[String]) -> Result<Vec<WorkflowHandle<Value>>> {
        let workflows = self
            .list_workflows(ListWorkflowsOptions {
                status: vec![WorkflowStatusType::Pending],
                load_input: true,
                ..Default::default()
            })
            .await?;
        let mut handles = Vec::new();
        for workflow in workflows {
            if workflow.executor_id.as_ref().is_some_and(|executor_id| executor_ids.contains(executor_id)) {
                self.spawn_workflow_execution(workflow.workflow_uuid.clone()).await;
                handles.push(WorkflowHandle::new(self.clone(), workflow.workflow_uuid));
            }
        }
        Ok(handles)
    }

    async fn queue_supervisor(&self) {
        while self.inner.launched.load(Ordering::SeqCst) {
            let result = self
                .inner
                .observability
                .clone()
                .observe_result(DbosOperation::QueueSupervisor, Vec::new(), self.transition_and_run_queued_workflows())
                .await;
            if let Err(error) = result {
                log_supervisor_iteration_failed("queue", &error);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn transition_and_run_queued_workflows(&self) -> Result<()> {
        let now = Utc::now();
        let delayed = self
            .list_workflows(ListWorkflowsOptions {
                status: vec![WorkflowStatusType::Delayed],
                load_input: true,
                ..Default::default()
            })
            .await?;
        for mut workflow in delayed {
            if workflow.delay_until.is_some_and(|delay_until| delay_until <= now) {
                workflow.status = WorkflowStatusType::Enqueued;
                workflow.updated_at = now;
                self.inner.store.save_workflow(workflow).await?;
            }
        }

        let listened = self.inner.listened_queues.read().await.clone();
        let queues = self.list_queues().await?;
        for queue in queues {
            if !listened.is_empty() && !listened.contains(&queue.name) {
                continue;
            }
            let workflows = self
                .list_workflows(ListWorkflowsOptions {
                    status: vec![WorkflowStatusType::Enqueued],
                    queue_name: Some(queue.name.clone()),
                    load_input: true,
                    sort_desc: false,
                    limit: Some(queue.max_tasks_per_iteration as usize),
                    ..Default::default()
                })
                .await?;
            for workflow in workflows {
                let mut operation_guard = self.inner.observability.start_operation(
                    DbosOperation::QueueDequeue,
                    vec![
                        DbosSpanAttribute::new("dbos.queue_name", queue.name.clone()),
                        DbosSpanAttribute::new("dbos.workflow_id", workflow.workflow_uuid.clone()),
                    ],
                );
                log_dequeued_workflow(&queue.name, &workflow.workflow_uuid);
                self.spawn_workflow_execution(workflow.workflow_uuid).await;
                operation_guard.finish_success();
            }
        }
        Ok(())
    }

    async fn schedule_supervisor(&self) {
        while self.inner.launched.load(Ordering::SeqCst) {
            let result = self
                .inner
                .observability
                .clone()
                .observe_result(DbosOperation::ScheduleReconcile, Vec::new(), self.reconcile_schedules())
                .await;
            if let Err(error) = result {
                log_supervisor_iteration_failed("schedule", &error);
            }
            tokio::time::sleep(self.inner.config.scheduler_polling_interval).await;
        }
    }

    async fn reconcile_schedules(&self) -> Result<()> {
        let schedules = self.list_schedules(ListSchedulesOptions { statuses: vec![ScheduleStatus::Active], ..Default::default() }).await?;
        let now = Utc::now();
        for mut schedule in schedules {
            let cron = match schedule.schedule.parse::<cron::Schedule>() {
                Ok(cron) => cron,
                Err(error) => {
                    log_invalid_schedule(&schedule.schedule_name, &error);
                    continue;
                }
            };
            let last = schedule.last_fired_at.unwrap_or_else(|| now - chrono::Duration::seconds(1));
            let should_fire = cron.after(&last).next().is_some_and(|next| next <= now);
            if should_fire {
                let delay_ms = rand::random::<u64>() % 1_000;
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                self.enqueue_scheduled_workflow(&schedule, now).await?;
                schedule.last_fired_at = Some(now);
                self.inner.store.upsert_schedule(schedule).await?;
            }
        }
        Ok(())
    }

    async fn enqueue_scheduled_workflow(&self, schedule: &WorkflowSchedule, scheduled_time: DateTime<Utc>) -> Result<String> {
        self.inner
            .observability
            .clone()
            .observe_result(
                DbosOperation::ScheduleTrigger,
                vec![
                    DbosSpanAttribute::new("dbos.schedule_name", schedule.schedule_name.clone()),
                    DbosSpanAttribute::new("dbos.workflow_name", schedule.workflow_name.clone()),
                ],
                async {
                    let workflow_id = format!("sched-{}-{}", schedule.schedule_name, scheduled_time.to_rfc3339());
                    let input =
                        serde_json::to_value(crate::types::ScheduledWorkflowInput { scheduled_time, context: schedule.context.clone() })?;
                    let options = WorkflowOptions {
                        workflow_id: Some(workflow_id.clone()),
                        queue_name: schedule.queue_name.clone().or_else(|| Some("_dbos_internal_queue".to_string())),
                        application_version: Some(self.application_version().to_string()),
                        class_name: schedule.workflow_class_name.clone(),
                        ..Default::default()
                    };
                    let _ = self.run_workflow_value(schedule.workflow_name.clone(), input, options).await?;
                    Ok(workflow_id)
                },
            )
            .await
    }

    async fn update_schedule_status(&self, schedule_name: &str, status: ScheduleStatus) -> Result<()> {
        let mut schedule = self
            .inner
            .store
            .get_schedule(schedule_name)
            .await?
            .ok_or_else(|| DbosError::invalid_argument(format!("schedule not found: {schedule_name}")))?;
        schedule.status = status;
        self.inner.store.upsert_schedule(schedule).await
    }
}

#[derive(Clone)]
pub struct DbosTransaction {
    ctx: DbosContext,
    isolation_level: Option<TransactionIsolationLevel>,
}

impl DbosTransaction {
    pub fn context(&self) -> &DbosContext {
        &self.ctx
    }

    pub fn isolation_level(&self) -> Option<TransactionIsolationLevel> {
        self.isolation_level
    }
}

pub struct WorkflowHandle<T> {
    ctx: DbosContext,
    workflow_id: String,
    _result: PhantomData<T>,
}

impl<T> Clone for WorkflowHandle<T> {
    fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
            workflow_id: self.workflow_id.clone(),
            _result: PhantomData,
        }
    }
}

impl<T> WorkflowHandle<T> {
    fn new(ctx: DbosContext, workflow_id: String) -> Self {
        Self { ctx, workflow_id, _result: PhantomData }
    }

    fn cast<O>(self) -> WorkflowHandle<O> {
        WorkflowHandle {
            ctx: self.ctx,
            workflow_id: self.workflow_id,
            _result: PhantomData,
        }
    }

    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    pub async fn get_status(&self) -> Result<WorkflowStatus> {
        self.ctx
            .inner
            .store
            .get_workflow(&self.workflow_id)
            .await?
            .ok_or_else(|| DbosError::non_existent_workflow(&self.workflow_id))
    }
}

impl<T> WorkflowHandle<T>
where
    T: DeserializeOwned + Send + 'static,
{
    pub async fn get_result(&self, timeout: Option<Duration>) -> Result<T> {
        self.get_result_with_options(GetResultOptions { timeout, ..Default::default() }).await
    }

    pub async fn get_result_with_options(&self, mut options: GetResultOptions) -> Result<T> {
        self.ctx
            .inner
            .observability
            .clone()
            .observe_result(
                DbosOperation::WorkflowResult,
                vec![DbosSpanAttribute::new("dbos.workflow_id", self.workflow_id.clone())],
                async {
                    if options.polling_interval.is_zero() {
                        options.polling_interval = GetResultOptions::default().polling_interval;
                    }
                    let started = tokio::time::Instant::now();
                    loop {
                        let status = match self.get_status().await {
                            Ok(status) => status,
                            Err(error) if is_transient_database_error(&error) => {
                                log_transient_retry("workflow_result", &error, &self.workflow_id);
                                if let Some(timeout) = options.timeout
                                    && started.elapsed() >= timeout
                                {
                                    return Err(DbosError::timeout(format!("workflow result timeout after {timeout:?}")));
                                }
                                tokio::time::sleep(options.polling_interval).await;
                                continue;
                            }
                            Err(error) => return Err(error),
                        };
                        match status.status {
                            WorkflowStatusType::Success => {
                                return self.ctx.decode_stored_value(status.output, &status.serialization);
                            }
                            WorkflowStatusType::Error => {
                                return Err(DbosError::workflow_execution(
                                    &self.workflow_id,
                                    status.error.unwrap_or_else(|| "workflow failed".to_string()),
                                ));
                            }
                            WorkflowStatusType::Cancelled => {
                                return Err(DbosError::new(
                                    DbosErrorCode::WorkflowCancelled,
                                    format!("workflow {} was cancelled", self.workflow_id),
                                ));
                            }
                            WorkflowStatusType::MaxRecoveryAttemptsExceeded => {
                                return Err(DbosError::new(
                                    DbosErrorCode::MaxStepRetriesExceeded,
                                    format!("workflow {} exceeded max recovery attempts", self.workflow_id),
                                ));
                            }
                            WorkflowStatusType::Pending | WorkflowStatusType::Enqueued | WorkflowStatusType::Delayed => {}
                        }
                        if let Some(timeout) = options.timeout
                            && started.elapsed() >= timeout
                        {
                            return Err(DbosError::timeout(format!("workflow result timeout after {timeout:?}")));
                        }
                        tokio::time::sleep(options.polling_interval).await;
                    }
                },
            )
            .await
    }
}

impl WorkflowQueue {
    pub async fn set_global_concurrency(&mut self, ctx: &DbosContext, value: Option<u32>) -> Result<()> {
        let name = self.name.clone();
        *self = ctx.update_queue_config(&name, |queue| queue.global_concurrency = value).await?;
        Ok(())
    }

    pub async fn set_worker_concurrency(&mut self, ctx: &DbosContext, value: Option<u32>) -> Result<()> {
        let name = self.name.clone();
        *self = ctx.update_queue_config(&name, |queue| queue.worker_concurrency = value).await?;
        Ok(())
    }

    pub async fn set_rate_limit(&mut self, ctx: &DbosContext, value: Option<crate::types::RateLimiter>) -> Result<()> {
        let name = self.name.clone();
        *self = ctx.update_queue_config(&name, |queue| queue.rate_limit = value).await?;
        Ok(())
    }

    pub async fn set_priority_enabled(&mut self, ctx: &DbosContext, value: bool) -> Result<()> {
        let name = self.name.clone();
        *self = ctx.update_queue_config(&name, |queue| queue.priority_enabled = value).await?;
        Ok(())
    }

    pub async fn set_partition_queue(&mut self, ctx: &DbosContext, value: bool) -> Result<()> {
        let name = self.name.clone();
        *self = ctx.update_queue_config(&name, |queue| queue.partition_queue = value).await?;
        Ok(())
    }

    pub async fn set_polling_interval(&mut self, ctx: &DbosContext, value: Duration) -> Result<()> {
        let name = self.name.clone();
        *self = ctx.update_queue_config(&name, |queue| queue.polling_interval = value).await?;
        Ok(())
    }

    pub async fn set_max_polling_interval(&mut self, ctx: &DbosContext, value: Duration) -> Result<()> {
        let name = self.name.clone();
        *self = ctx.update_queue_config(&name, |queue| queue.max_polling_interval = value).await?;
        Ok(())
    }
}

pub async fn new_dbos_context(config: DbosConfig) -> Result<DbosContext> {
    DbosContext::new(config).await
}

pub async fn launch(ctx: &DbosContext) -> Result<()> {
    ctx.launch().await
}

pub async fn shutdown(ctx: &DbosContext, timeout: Duration) {
    ctx.shutdown(timeout).await;
}

pub async fn run_as_step<T, F, Fut>(ctx: &DbosContext, step_name: impl Into<String>, operation: F) -> Result<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    F: FnOnce(DbosContext) -> Fut + Send,
    Fut: Future<Output = Result<T>> + Send,
{
    ctx.run_as_step(step_name, operation).await
}

pub async fn run_as_step_with_options<T, F, Fut>(
    ctx: &DbosContext,
    step_name: impl Into<String>,
    options: StepOptions,
    operation: F,
) -> Result<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    F: Fn(DbosContext) -> Fut + Send + Sync,
    Fut: Future<Output = Result<T>> + Send,
{
    ctx.run_as_step_with_options(step_name, options, operation).await
}

pub async fn sleep(ctx: &DbosContext, duration: Duration) -> Result<Duration> {
    ctx.sleep(duration).await
}

async fn build_store(config: &DbosConfig) -> Result<Arc<dyn SystemDatabase>> {
    if let Some(system_database) = &config.system_database {
        return Ok(system_database.clone().into_arc());
    }
    if let Some(turso_path) = &config.turso_path {
        #[cfg(feature = "turso")]
        {
            return crate::store::TursoStore::connect(turso_path).await;
        }
        #[cfg(not(feature = "turso"))]
        {
            let _ = turso_path;
            return Err(DbosError::unsupported("turso_path requires the turso feature"));
        }
    }
    if let Some(database_url) = &config.database_url {
        if let Some(turso_path) = database_url.strip_prefix("turso://") {
            #[cfg(feature = "turso")]
            {
                return crate::store::TursoStore::connect(turso_path).await;
            }
            #[cfg(not(feature = "turso"))]
            {
                let _ = turso_path;
                return Err(DbosError::unsupported("turso database_url requires the turso feature"));
            }
        }
        #[cfg(feature = "postgres")]
        {
            return crate::store::PostgresStore::connect(database_url, &config.database_schema).await;
        }
        #[cfg(not(feature = "postgres"))]
        {
            return Err(DbosError::unsupported("database_url requires the postgres feature"));
        }
    }
    Ok(MemoryStore::shared())
}

fn validate_queue(queue: &WorkflowQueue) -> Result<()> {
    if queue.name.is_empty() {
        return Err(DbosError::invalid_argument("queue name is required"));
    }
    if let (Some(worker), Some(global)) = (queue.worker_concurrency, queue.global_concurrency)
        && worker > global
    {
        return Err(DbosError::invalid_argument(
            "global concurrency must be greater than or equal to worker concurrency",
        ));
    }
    if let Some(rate_limit) = &queue.rate_limit
        && (rate_limit.limit == 0 || rate_limit.period.is_zero())
    {
        return Err(DbosError::invalid_argument("rate limiter limit and period must be positive"));
    }
    if queue.polling_interval.is_zero() {
        return Err(DbosError::invalid_argument("queue polling interval must be positive"));
    }
    if queue.max_polling_interval.is_zero() {
        return Err(DbosError::invalid_argument("queue max polling interval must be positive"));
    }
    if queue.max_polling_interval < queue.polling_interval {
        return Err(DbosError::invalid_argument(
            "queue max polling interval must be greater than or equal to the polling interval",
        ));
    }
    Ok(())
}

fn step_retry_delay(options: &StepOptions, retry_number: u32) -> Duration {
    let base_interval = if options.base_interval.is_zero() {
        StepOptions::default().base_interval
    } else {
        options.base_interval
    };
    let max_interval = if options.max_interval.is_zero() {
        StepOptions::default().max_interval
    } else {
        options.max_interval
    };
    let backoff_factor = if options.backoff_factor.is_finite() && options.backoff_factor > 0.0 {
        options.backoff_factor
    } else {
        StepOptions::default().backoff_factor
    };
    let exponent = retry_number.saturating_sub(1) as i32;
    let factor = backoff_factor.powi(exponent);
    let scaled_secs = base_interval.as_secs_f64() * factor;
    if !scaled_secs.is_finite() || scaled_secs >= max_interval.as_secs_f64() {
        return max_interval;
    }
    Duration::from_secs_f64(scaled_secs)
}

fn time_bucket_value(timestamp: DateTime<Utc>, size: Duration) -> Result<Value> {
    if size.is_zero() {
        return Err(DbosError::invalid_argument("time bucket size must be positive"));
    }
    let size_ms = i64::try_from(size.as_millis()).map_err(|_| DbosError::invalid_argument("time bucket size is too large"))?;
    if size_ms == 0 {
        return Err(DbosError::invalid_argument("time bucket size must be at least one millisecond"));
    }
    let timestamp_ms = timestamp.timestamp_millis();
    let bucket_ms = timestamp_ms - timestamp_ms.rem_euclid(size_ms);
    let value = DateTime::<Utc>::from_timestamp_millis(bucket_ms)
        .map(|bucket| Value::String(bucket.to_rfc3339()))
        .unwrap_or_else(|| Value::Number(bucket_ms.into()));
    Ok(value)
}

fn validate_cron(spec: &str) -> Result<()> {
    spec.parse::<cron::Schedule>()
        .map(|_| ())
        .map_err(|err| DbosError::invalid_argument(format!("invalid cron schedule {spec:?}: {err}")))
}

fn is_transient_database_error(error: &DbosError) -> bool {
    error.code == DbosErrorCode::Database
}
