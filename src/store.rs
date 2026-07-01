use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
#[cfg(feature = "postgres")]
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
#[cfg(any(feature = "postgres", feature = "turso"))]
use serde::Serialize;
#[cfg(feature = "postgres")]
use serde::de::DeserializeOwned;
use serde_json::Value;
#[cfg(feature = "postgres")]
use tokio::sync::Mutex;
use tokio::sync::RwLock;

use crate::error::{DbosError, Result};
#[cfg(feature = "postgres")]
use crate::observability::log_database_warning;
use crate::types::{
    DeleteWorkflowOptions, ListSchedulesOptions, ListWorkflowsOptions, StepInfo, StreamEntry, VersionInfo, WorkflowEvent, WorkflowMessage,
    WorkflowQueue, WorkflowSchedule, WorkflowStatus,
};

#[async_trait]
pub trait SystemDatabase: Send + Sync {
    async fn migrate(&self) -> Result<()>;
    async fn insert_workflow(&self, workflow: WorkflowStatus) -> Result<()>;
    async fn save_workflow(&self, workflow: WorkflowStatus) -> Result<()>;
    async fn get_workflow(&self, workflow_id: &str) -> Result<Option<WorkflowStatus>>;
    async fn list_workflows(&self, options: &ListWorkflowsOptions) -> Result<Vec<WorkflowStatus>>;
    async fn delete_workflows(&self, workflow_ids: &[String], options: &DeleteWorkflowOptions) -> Result<()>;

    async fn record_step(&self, step: StepInfo) -> Result<()>;
    async fn get_step(&self, workflow_id: &str, step_id: i32) -> Result<Option<StepInfo>>;
    async fn list_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>>;

    async fn upsert_queue(&self, queue: WorkflowQueue) -> Result<()>;
    async fn get_queue(&self, name: &str) -> Result<Option<WorkflowQueue>>;
    async fn list_queues(&self) -> Result<Vec<WorkflowQueue>>;
    async fn delete_queue(&self, name: &str) -> Result<()>;

    async fn upsert_schedule(&self, schedule: WorkflowSchedule) -> Result<()>;
    async fn get_schedule(&self, name: &str) -> Result<Option<WorkflowSchedule>>;
    async fn list_schedules(&self, options: &ListSchedulesOptions) -> Result<Vec<WorkflowSchedule>>;
    async fn delete_schedule(&self, name: &str) -> Result<()>;

    async fn send_message(&self, message: WorkflowMessage) -> Result<()>;
    async fn recv_message(&self, destination_id: &str, topic: &str) -> Result<Option<WorkflowMessage>>;
    async fn list_messages(&self, workflow_id: &str) -> Result<Vec<WorkflowMessage>>;

    async fn set_event(&self, event: WorkflowEvent) -> Result<()>;
    async fn get_event(&self, workflow_id: &str, key: &str) -> Result<Option<WorkflowEvent>>;
    async fn list_events(&self, workflow_id: &str) -> Result<Vec<WorkflowEvent>>;

    async fn write_stream(&self, entry: StreamEntry) -> Result<()>;
    async fn read_stream(&self, workflow_id: &str, key: &str) -> Result<Vec<StreamEntry>>;
    async fn list_streams(&self, workflow_id: &str) -> Result<Vec<StreamEntry>>;
    async fn close_stream(&self, workflow_id: &str, key: &str) -> Result<()>;

    async fn create_application_version(&self, version: VersionInfo) -> Result<()>;
    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>>;
    async fn set_latest_application_version(&self, name: &str) -> Result<()>;

    async fn set_patch(&self, patch_name: &str, active: bool) -> Result<()>;
    async fn get_patch(&self, patch_name: &str) -> Result<Option<bool>>;
}

#[derive(Clone)]
pub struct SystemDatabaseHandle {
    store: Arc<dyn SystemDatabase>,
}

impl SystemDatabaseHandle {
    pub fn from_arc(store: Arc<dyn SystemDatabase>) -> Self {
        Self { store }
    }

    pub fn memory() -> Self {
        Self { store: MemoryStore::shared() }
    }

    #[cfg(feature = "postgres")]
    pub async fn postgres(database_url: &str, schema: &str) -> Result<Self> {
        Ok(Self { store: PostgresStore::connect(database_url, schema).await? })
    }

    #[cfg(feature = "turso")]
    pub async fn turso(path: &str) -> Result<Self> {
        Ok(Self { store: TursoStore::connect(path).await? })
    }

    pub(crate) fn into_arc(self) -> Arc<dyn SystemDatabase> {
        self.store
    }
}

impl std::fmt::Debug for SystemDatabaseHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemDatabaseHandle").finish_non_exhaustive()
    }
}

#[derive(Default)]
struct MemoryData {
    workflows: HashMap<String, WorkflowStatus>,
    steps: HashMap<(String, i32), StepInfo>,
    queues: HashMap<String, WorkflowQueue>,
    schedules: HashMap<String, WorkflowSchedule>,
    messages: Vec<WorkflowMessage>,
    events: HashMap<(String, String), WorkflowEvent>,
    streams: HashMap<(String, String), Vec<StreamEntry>>,
    application_versions: HashMap<String, VersionInfo>,
    patches: HashMap<String, bool>,
}

#[derive(Default)]
pub struct MemoryStore {
    data: RwLock<MemoryData>,
}

impl MemoryStore {
    pub fn shared() -> Arc<dyn SystemDatabase> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl SystemDatabase for MemoryStore {
    async fn migrate(&self) -> Result<()> {
        Ok(())
    }

    async fn insert_workflow(&self, workflow: WorkflowStatus) -> Result<()> {
        let mut data = self.data.write().await;
        if let Some(existing) = data.workflows.get(&workflow.workflow_uuid) {
            if existing.name != workflow.name || existing.input != workflow.input {
                return Err(DbosError::new(
                    crate::error::DbosErrorCode::ConflictingWorkflow,
                    format!("conflicting workflow invocation with the same ID ({})", workflow.workflow_uuid),
                ));
            }
            return Ok(());
        }
        if let (Some(queue), Some(dedup)) = (&workflow.queue_name, &workflow.deduplication_id) {
            let duplicate = data.workflows.values().any(|item| {
                item.queue_name.as_ref() == Some(queue) && item.deduplication_id.as_ref() == Some(dedup) && !item.status.is_terminal()
            });
            if duplicate {
                let mut err = DbosError::new(
                    crate::error::DbosErrorCode::QueueDeduplicated,
                    format!("workflow was deduplicated in queue {queue}"),
                );
                err.queue_name = Some(queue.clone());
                err.deduplication_id = Some(dedup.clone());
                return Err(err);
            }
        }
        data.workflows.insert(workflow.workflow_uuid.clone(), workflow);
        Ok(())
    }

    async fn save_workflow(&self, workflow: WorkflowStatus) -> Result<()> {
        self.data.write().await.workflows.insert(workflow.workflow_uuid.clone(), workflow);
        Ok(())
    }

    async fn get_workflow(&self, workflow_id: &str) -> Result<Option<WorkflowStatus>> {
        Ok(self.data.read().await.workflows.get(workflow_id).cloned())
    }

    async fn list_workflows(&self, options: &ListWorkflowsOptions) -> Result<Vec<WorkflowStatus>> {
        let data = self.data.read().await;
        let mut rows = data.workflows.values().filter(|workflow| workflow_matches(workflow, options)).cloned().collect::<Vec<_>>();
        sort_and_page_workflows(&mut rows, options);
        apply_workflow_load_options(&mut rows, options);
        Ok(rows)
    }

    async fn delete_workflows(&self, workflow_ids: &[String], _options: &DeleteWorkflowOptions) -> Result<()> {
        let id_set = workflow_ids.iter().collect::<HashSet<_>>();
        let mut data = self.data.write().await;
        for workflow_id in workflow_ids {
            data.workflows.remove(workflow_id);
        }
        data.steps.retain(|(workflow_id, _), _| !id_set.contains(workflow_id));
        data.events.retain(|(workflow_id, _), _| !id_set.contains(workflow_id));
        data.streams.retain(|(workflow_id, _), _| !id_set.contains(workflow_id));
        data.messages.retain(|message| !id_set.contains(&message.destination_id));
        Ok(())
    }

    async fn record_step(&self, step: StepInfo) -> Result<()> {
        self.data.write().await.steps.insert((step.workflow_uuid.clone(), step.step_id), step);
        Ok(())
    }

    async fn get_step(&self, workflow_id: &str, step_id: i32) -> Result<Option<StepInfo>> {
        Ok(self.data.read().await.steps.get(&(workflow_id.to_string(), step_id)).cloned())
    }

    async fn list_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        let mut steps = self.data.read().await.steps.values().filter(|step| step.workflow_uuid == workflow_id).cloned().collect::<Vec<_>>();
        steps.sort_by_key(|step| step.step_id);
        Ok(steps)
    }

    async fn upsert_queue(&self, queue: WorkflowQueue) -> Result<()> {
        self.data.write().await.queues.insert(queue.name.clone(), queue);
        Ok(())
    }

    async fn get_queue(&self, name: &str) -> Result<Option<WorkflowQueue>> {
        Ok(self.data.read().await.queues.get(name).cloned())
    }

    async fn list_queues(&self) -> Result<Vec<WorkflowQueue>> {
        Ok(self.data.read().await.queues.values().cloned().collect())
    }

    async fn delete_queue(&self, name: &str) -> Result<()> {
        self.data.write().await.queues.remove(name);
        Ok(())
    }

    async fn upsert_schedule(&self, schedule: WorkflowSchedule) -> Result<()> {
        self.data.write().await.schedules.insert(schedule.schedule_name.clone(), schedule);
        Ok(())
    }

    async fn get_schedule(&self, name: &str) -> Result<Option<WorkflowSchedule>> {
        Ok(self.data.read().await.schedules.get(name).cloned())
    }

    async fn list_schedules(&self, options: &ListSchedulesOptions) -> Result<Vec<WorkflowSchedule>> {
        Ok(self.data.read().await.schedules.values().filter(|schedule| schedule_matches(schedule, options)).cloned().collect())
    }

    async fn delete_schedule(&self, name: &str) -> Result<()> {
        self.data.write().await.schedules.remove(name);
        Ok(())
    }

    async fn send_message(&self, message: WorkflowMessage) -> Result<()> {
        self.data.write().await.messages.push(message);
        Ok(())
    }

    async fn recv_message(&self, destination_id: &str, topic: &str) -> Result<Option<WorkflowMessage>> {
        let mut data = self.data.write().await;
        let Some(message) = data
            .messages
            .iter_mut()
            .find(|message| !message.consumed && message.destination_id == destination_id && message.topic == topic)
        else {
            return Ok(None);
        };
        message.consumed = true;
        Ok(Some(message.clone()))
    }

    async fn list_messages(&self, workflow_id: &str) -> Result<Vec<WorkflowMessage>> {
        let mut messages = self
            .data
            .read()
            .await
            .messages
            .iter()
            .filter(|message| message.destination_id == workflow_id)
            .cloned()
            .collect::<Vec<_>>();
        messages.sort_by_key(|message| message.created_at);
        Ok(messages)
    }

    async fn set_event(&self, event: WorkflowEvent) -> Result<()> {
        self.data.write().await.events.insert((event.workflow_uuid.clone(), event.key.clone()), event);
        Ok(())
    }

    async fn get_event(&self, workflow_id: &str, key: &str) -> Result<Option<WorkflowEvent>> {
        Ok(self.data.read().await.events.get(&(workflow_id.to_string(), key.to_string())).cloned())
    }

    async fn list_events(&self, workflow_id: &str) -> Result<Vec<WorkflowEvent>> {
        let mut events =
            self.data.read().await.events.values().filter(|event| event.workflow_uuid == workflow_id).cloned().collect::<Vec<_>>();
        events.sort_by_key(|event| event.created_at);
        Ok(events)
    }

    async fn write_stream(&self, entry: StreamEntry) -> Result<()> {
        self.data.write().await.streams.entry((entry.workflow_uuid.clone(), entry.key.clone())).or_default().push(entry);
        Ok(())
    }

    async fn read_stream(&self, workflow_id: &str, key: &str) -> Result<Vec<StreamEntry>> {
        Ok(self.data.read().await.streams.get(&(workflow_id.to_string(), key.to_string())).cloned().unwrap_or_default())
    }

    async fn list_streams(&self, workflow_id: &str) -> Result<Vec<StreamEntry>> {
        let mut streams = self
            .data
            .read()
            .await
            .streams
            .values()
            .flatten()
            .filter(|entry| entry.workflow_uuid == workflow_id)
            .cloned()
            .collect::<Vec<_>>();
        streams.sort_by(|left, right| left.key.cmp(&right.key).then(left.offset.cmp(&right.offset)));
        Ok(streams)
    }

    async fn close_stream(&self, workflow_id: &str, key: &str) -> Result<()> {
        let mut data = self.data.write().await;
        let entries = data.streams.entry((workflow_id.to_string(), key.to_string())).or_default();
        let offset = i64::try_from(entries.len()).map_err(|_| DbosError::invalid_argument("stream offset overflow"))?;
        entries.push(StreamEntry {
            workflow_uuid: workflow_id.to_string(),
            key: key.to_string(),
            offset,
            value: None,
            serialization: crate::serialization::DBOS_JSON.to_string(),
            closed: true,
            created_at: Utc::now(),
        });
        Ok(())
    }

    async fn create_application_version(&self, version: VersionInfo) -> Result<()> {
        self.data.write().await.application_versions.entry(version.name.clone()).or_insert(version);
        Ok(())
    }

    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>> {
        let mut versions = self.data.read().await.application_versions.values().cloned().collect::<Vec<_>>();
        versions.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        Ok(versions)
    }

    async fn set_latest_application_version(&self, name: &str) -> Result<()> {
        let mut data = self.data.write().await;
        let now = Utc::now();
        let version = data.application_versions.entry(name.to_string()).or_insert(VersionInfo {
            name: name.to_string(),
            created_at: now,
            updated_at: now,
        });
        version.updated_at = now;
        Ok(())
    }

    async fn set_patch(&self, patch_name: &str, active: bool) -> Result<()> {
        self.data.write().await.patches.insert(patch_name.to_string(), active);
        Ok(())
    }

    async fn get_patch(&self, patch_name: &str) -> Result<Option<bool>> {
        Ok(self.data.read().await.patches.get(patch_name).copied())
    }
}

fn workflow_matches(workflow: &WorkflowStatus, options: &ListWorkflowsOptions) -> bool {
    if !options.workflow_ids.is_empty() && !options.workflow_ids.contains(&workflow.workflow_uuid) {
        return false;
    }
    if !matches_optional_string_filter(
        workflow.authenticated_user.as_deref(),
        options.authenticated_user.as_deref(),
        &options.authenticated_users,
    ) {
        return false;
    }
    if let Some(start) = options.start_time
        && workflow.created_at < start
    {
        return false;
    }
    if let Some(end) = options.end_time
        && workflow.created_at > end
    {
        return false;
    }
    if !options.status.is_empty() && !options.status.contains(&workflow.status) {
        return false;
    }
    if !matches_string_filter(&workflow.application_version, options.application_version.as_deref(), &options.application_versions) {
        return false;
    }
    if !matches_string_filter(&workflow.name, options.workflow_name.as_deref(), &options.workflow_names) {
        return false;
    }
    if !matches_prefix_filter(&workflow.workflow_uuid, options.workflow_id_prefix.as_deref(), &options.workflow_id_prefixes) {
        return false;
    }
    if !matches_optional_string_filter(workflow.queue_name.as_deref(), options.queue_name.as_deref(), &options.queue_names) {
        return false;
    }
    if options.queues_only && workflow.queue_name.is_none() {
        return false;
    }
    if !matches_optional_string_filter(
        workflow.deduplication_id.as_deref(),
        options.deduplication_id.as_deref(),
        &options.deduplication_ids,
    ) {
        return false;
    }
    if !matches_optional_string_filter(workflow.executor_id.as_deref(), None, &options.executor_ids) {
        return false;
    }
    if !matches_optional_string_filter(workflow.forked_from.as_deref(), None, &options.forked_from) {
        return false;
    }
    if !matches_optional_string_filter(workflow.parent_workflow_id.as_deref(), None, &options.parent_workflow_ids) {
        return false;
    }
    if let Some(completed_after) = options.completed_after
        && workflow.completed_at.is_none_or(|completed_at| completed_at < completed_after)
    {
        return false;
    }
    if let Some(completed_before) = options.completed_before
        && workflow.completed_at.is_none_or(|completed_at| completed_at > completed_before)
    {
        return false;
    }
    if let Some(dequeued_after) = options.dequeued_after
        && workflow.started_at.is_none_or(|started_at| started_at < dequeued_after)
    {
        return false;
    }
    if let Some(dequeued_before) = options.dequeued_before
        && workflow.started_at.is_none_or(|started_at| started_at > dequeued_before)
    {
        return false;
    }
    if let Some(was_forked_from) = options.was_forked_from
        && workflow.was_forked_from != was_forked_from
    {
        return false;
    }
    if let Some(has_parent) = options.has_parent
        && workflow.parent_workflow_id.is_some() != has_parent
    {
        return false;
    }
    true
}

fn matches_string_filter(value: &str, single: Option<&str>, many: &[String]) -> bool {
    single.is_none() && many.is_empty() || single.is_some_and(|single| value == single) || many.iter().any(|candidate| candidate == value)
}

fn matches_optional_string_filter(value: Option<&str>, single: Option<&str>, many: &[String]) -> bool {
    if single.is_none() && many.is_empty() {
        return true;
    }
    let Some(value) = value else {
        return false;
    };
    matches_string_filter(value, single, many)
}

fn matches_prefix_filter(value: &str, single: Option<&str>, many: &[String]) -> bool {
    single.is_none() && many.is_empty()
        || single.is_some_and(|prefix| value.starts_with(prefix))
        || many.iter().any(|prefix| value.starts_with(prefix))
}

fn sort_and_page_workflows(rows: &mut Vec<WorkflowStatus>, options: &ListWorkflowsOptions) {
    if options.sort_desc {
        rows.sort_by(|left, right| right.created_at.cmp(&left.created_at));
    } else {
        rows.sort_by(|left, right| left.created_at.cmp(&right.created_at));
    }

    let offset = options.offset.unwrap_or(0).min(rows.len());
    if offset > 0 {
        rows.drain(0..offset);
    }
    if let Some(limit) = options.limit
        && rows.len() > limit
    {
        rows.truncate(limit);
    }
}

fn apply_workflow_load_options(rows: &mut [WorkflowStatus], options: &ListWorkflowsOptions) {
    for workflow in rows {
        if !options.load_input {
            workflow.input = None;
        }
        if !options.load_output {
            workflow.output = None;
            workflow.error = None;
        }
    }
}

fn schedule_matches(schedule: &WorkflowSchedule, options: &ListSchedulesOptions) -> bool {
    if !options.statuses.is_empty() && !options.statuses.contains(&schedule.status) {
        return false;
    }
    if !options.workflow_names.is_empty() && !options.workflow_names.contains(&schedule.workflow_name) {
        return false;
    }
    if !options.schedule_name_prefixes.is_empty()
        && !options.schedule_name_prefixes.iter().any(|prefix| schedule.schedule_name.starts_with(prefix))
    {
        return false;
    }
    true
}

#[cfg(feature = "postgres")]
pub(crate) struct PostgresStore {
    database_url: String,
    client: Mutex<Option<tokio_postgres::Client>>,
    reconnect_timeout: Duration,
    schema: String,
}

#[cfg(feature = "postgres")]
impl PostgresStore {
    pub(crate) async fn connect(database_url: &str, schema: &str) -> Result<Arc<dyn SystemDatabase>> {
        validate_schema_name(schema)?;
        let store = Arc::new(Self {
            database_url: database_url.to_string(),
            client: Mutex::new(None),
            reconnect_timeout: reconnect_timeout(),
            schema: schema.to_string(),
        });
        {
            let mut client = store.client.lock().await;
            store.ensure_connected_locked(&mut client).await?;
        }
        Ok(store)
    }

    fn state_table(&self) -> String {
        format!("{}.dbos_state", self.schema)
    }

    fn migrations_table(&self) -> String {
        format!("{}.dbos_migrations", self.schema)
    }

    async fn open_client(&self) -> Result<tokio_postgres::Client> {
        let (client, connection) = tokio_postgres::connect(&self.database_url, tokio_postgres::NoTls).await.map_err(DbosError::from)?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                log_database_warning("dbos postgres connection task exited", &error);
            }
        });
        Ok(client)
    }

    async fn ensure_connected_locked(&self, client: &mut Option<tokio_postgres::Client>) -> Result<()> {
        if client.is_some() {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + self.reconnect_timeout;
        let mut delay = Duration::from_millis(50);
        loop {
            match self.open_client().await {
                Ok(new_client) => {
                    *client = Some(new_client);
                    return Ok(());
                }
                Err(error) if tokio::time::Instant::now() < deadline => {
                    log_database_warning("retrying DBOS postgres connection", &error);
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_millis(500));
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn put<T: Serialize + Send + Sync>(&self, kind: &str, id: &str, value: &T) -> Result<()> {
        let payload = serde_json::to_value(value)?;
        let query = format!(
            "INSERT INTO {} (kind, id, payload, updated_at) VALUES ($1, $2, $3, now()) \
             ON CONFLICT (kind, id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = now()",
            self.state_table()
        );
        for attempt in 0..2 {
            let mut client = self.client.lock().await;
            self.ensure_connected_locked(&mut client).await?;
            let Some(client_ref) = client.as_ref() else {
                return Err(DbosError::database("postgres client was not initialized"));
            };
            match client_ref.execute(&query, &[&kind, &id, &payload]).await {
                Ok(_) => return Ok(()),
                Err(error) if attempt == 0 => {
                    log_database_warning("retrying DBOS postgres put after connection failure", &error);
                    *client = None;
                }
                Err(error) => return Err(DbosError::from(error)),
            }
        }
        Err(DbosError::database("postgres put retry loop exited unexpectedly"))
    }

    async fn get<T: DeserializeOwned>(&self, kind: &str, id: &str) -> Result<Option<T>> {
        let query = format!("SELECT payload FROM {} WHERE kind = $1 AND id = $2", self.state_table());
        let row = {
            let mut row = None;
            for attempt in 0..2 {
                let mut client = self.client.lock().await;
                self.ensure_connected_locked(&mut client).await?;
                let Some(client_ref) = client.as_ref() else {
                    return Err(DbosError::database("postgres client was not initialized"));
                };
                match client_ref.query_opt(&query, &[&kind, &id]).await {
                    Ok(result) => {
                        row = result;
                        break;
                    }
                    Err(error) if attempt == 0 => {
                        log_database_warning("retrying DBOS postgres get after connection failure", &error);
                        *client = None;
                    }
                    Err(error) => return Err(DbosError::from(error)),
                }
            }
            row
        };
        let Some(row) = row else {
            return Ok(None);
        };
        let payload: Value = row.get(0);
        serde_json::from_value(payload).map(Some).map_err(DbosError::from)
    }

    async fn list<T: DeserializeOwned>(&self, kind: &str) -> Result<Vec<T>> {
        let query = format!("SELECT payload FROM {} WHERE kind = $1", self.state_table());
        let rows = {
            let mut rows = None;
            for attempt in 0..2 {
                let mut client = self.client.lock().await;
                self.ensure_connected_locked(&mut client).await?;
                let Some(client_ref) = client.as_ref() else {
                    return Err(DbosError::database("postgres client was not initialized"));
                };
                match client_ref.query(&query, &[&kind]).await {
                    Ok(result) => {
                        rows = Some(result);
                        break;
                    }
                    Err(error) if attempt == 0 => {
                        log_database_warning("retrying DBOS postgres list after connection failure", &error);
                        *client = None;
                    }
                    Err(error) => return Err(DbosError::from(error)),
                }
            }
            rows.ok_or_else(|| DbosError::database("postgres list retry loop exited unexpectedly"))?
        };
        rows.into_iter()
            .map(|row| {
                let payload: Value = row.get(0);
                serde_json::from_value(payload).map_err(DbosError::from)
            })
            .collect()
    }

    async fn delete(&self, kind: &str, id: &str) -> Result<()> {
        let query = format!("DELETE FROM {} WHERE kind = $1 AND id = $2", self.state_table());
        for attempt in 0..2 {
            let mut client = self.client.lock().await;
            self.ensure_connected_locked(&mut client).await?;
            let Some(client_ref) = client.as_ref() else {
                return Err(DbosError::database("postgres client was not initialized"));
            };
            match client_ref.execute(&query, &[&kind, &id]).await {
                Ok(_) => return Ok(()),
                Err(error) if attempt == 0 => {
                    log_database_warning("retrying DBOS postgres delete after connection failure", &error);
                    *client = None;
                }
                Err(error) => return Err(DbosError::from(error)),
            }
        }
        Err(DbosError::database("postgres delete retry loop exited unexpectedly"))
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl SystemDatabase for PostgresStore {
    async fn migrate(&self) -> Result<()> {
        let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema);
        let create_migrations = format!(
            "CREATE TABLE IF NOT EXISTS {} (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
            self.migrations_table()
        );
        let create_state = format!(
            "CREATE TABLE IF NOT EXISTS {} (kind TEXT NOT NULL, id TEXT NOT NULL, payload JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (kind, id))",
            self.state_table()
        );
        let create_index = format!(
            "CREATE INDEX IF NOT EXISTS dbos_state_kind_updated_idx ON {} (kind, updated_at)",
            self.state_table()
        );
        let insert_migration = format!("INSERT INTO {} (version) VALUES ($1) ON CONFLICT (version) DO NOTHING", self.migrations_table());
        for attempt in 0..2 {
            let mut client = self.client.lock().await;
            self.ensure_connected_locked(&mut client).await?;
            let Some(client_ref) = client.as_ref() else {
                return Err(DbosError::database("postgres client was not initialized"));
            };
            let result = async {
                client_ref.batch_execute(&create_schema).await?;
                client_ref.batch_execute(&create_migrations).await?;
                client_ref.batch_execute(&create_state).await?;
                client_ref.batch_execute(&create_index).await?;
                client_ref.execute(&insert_migration, &[&"rust-0001-json-state-store"]).await?;
                Ok::<(), tokio_postgres::Error>(())
            }
            .await;
            match result {
                Ok(()) => return Ok(()),
                Err(error) if attempt == 0 => {
                    log_database_warning("retrying DBOS postgres migration after connection failure", &error);
                    *client = None;
                }
                Err(error) => return Err(DbosError::from(error)),
            }
        }
        Err(DbosError::database("postgres migration retry loop exited unexpectedly"))
    }

    async fn insert_workflow(&self, workflow: WorkflowStatus) -> Result<()> {
        if self.get_workflow(&workflow.workflow_uuid).await?.is_some() {
            return Err(DbosError::new(
                crate::error::DbosErrorCode::ConflictingWorkflow,
                format!("conflicting workflow invocation with the same ID ({})", workflow.workflow_uuid),
            ));
        }
        self.put("workflow", &workflow.workflow_uuid, &workflow).await
    }

    async fn save_workflow(&self, workflow: WorkflowStatus) -> Result<()> {
        self.put("workflow", &workflow.workflow_uuid, &workflow).await
    }

    async fn get_workflow(&self, workflow_id: &str) -> Result<Option<WorkflowStatus>> {
        self.get("workflow", workflow_id).await
    }

    async fn list_workflows(&self, options: &ListWorkflowsOptions) -> Result<Vec<WorkflowStatus>> {
        let mut rows = self
            .list::<WorkflowStatus>("workflow")
            .await?
            .into_iter()
            .filter(|workflow| workflow_matches(workflow, options))
            .collect::<Vec<_>>();
        sort_and_page_workflows(&mut rows, options);
        apply_workflow_load_options(&mut rows, options);
        Ok(rows)
    }

    async fn delete_workflows(&self, workflow_ids: &[String], _options: &DeleteWorkflowOptions) -> Result<()> {
        for workflow_id in workflow_ids {
            self.delete("workflow", workflow_id).await?;
            let steps = self.list_steps(workflow_id).await?;
            for step in steps {
                self.delete("step", &step_key(workflow_id, step.step_id)).await?;
            }
            for event in self.list_events(workflow_id).await? {
                self.delete("event", &event_key(workflow_id, &event.key)).await?;
            }
            for entry in self.list_streams(workflow_id).await? {
                self.delete("stream", &stream_key(workflow_id, &entry.key, entry.offset)).await?;
            }
            for message in self.list_messages(workflow_id).await? {
                self.delete("message", &message_key(&message)).await?;
            }
        }
        Ok(())
    }

    async fn record_step(&self, step: StepInfo) -> Result<()> {
        self.put("step", &step_key(&step.workflow_uuid, step.step_id), &step).await
    }

    async fn get_step(&self, workflow_id: &str, step_id: i32) -> Result<Option<StepInfo>> {
        self.get("step", &step_key(workflow_id, step_id)).await
    }

    async fn list_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        let mut steps =
            self.list::<StepInfo>("step").await?.into_iter().filter(|step| step.workflow_uuid == workflow_id).collect::<Vec<_>>();
        steps.sort_by_key(|step| step.step_id);
        Ok(steps)
    }

    async fn upsert_queue(&self, queue: WorkflowQueue) -> Result<()> {
        self.put("queue", &queue.name, &queue).await
    }

    async fn get_queue(&self, name: &str) -> Result<Option<WorkflowQueue>> {
        self.get("queue", name).await
    }

    async fn list_queues(&self) -> Result<Vec<WorkflowQueue>> {
        self.list("queue").await
    }

    async fn delete_queue(&self, name: &str) -> Result<()> {
        self.delete("queue", name).await
    }

    async fn upsert_schedule(&self, schedule: WorkflowSchedule) -> Result<()> {
        self.put("schedule", &schedule.schedule_name, &schedule).await
    }

    async fn get_schedule(&self, name: &str) -> Result<Option<WorkflowSchedule>> {
        self.get("schedule", name).await
    }

    async fn list_schedules(&self, options: &ListSchedulesOptions) -> Result<Vec<WorkflowSchedule>> {
        Ok(self
            .list::<WorkflowSchedule>("schedule")
            .await?
            .into_iter()
            .filter(|schedule| schedule_matches(schedule, options))
            .collect())
    }

    async fn delete_schedule(&self, name: &str) -> Result<()> {
        self.delete("schedule", name).await
    }

    async fn send_message(&self, message: WorkflowMessage) -> Result<()> {
        let id = message_key(&message);
        self.put("message", &id, &message).await
    }

    async fn recv_message(&self, destination_id: &str, topic: &str) -> Result<Option<WorkflowMessage>> {
        let mut messages = self
            .list::<WorkflowMessage>("message")
            .await?
            .into_iter()
            .filter(|message| !message.consumed && message.destination_id == destination_id && message.topic == topic)
            .collect::<Vec<_>>();
        messages.sort_by_key(|message| message.created_at);
        let Some(mut message) = messages.into_iter().next() else {
            return Ok(None);
        };
        message.consumed = true;
        let id = message_key(&message);
        self.put("message", &id, &message).await?;
        Ok(Some(message))
    }

    async fn list_messages(&self, workflow_id: &str) -> Result<Vec<WorkflowMessage>> {
        let mut messages = self
            .list::<WorkflowMessage>("message")
            .await?
            .into_iter()
            .filter(|message| message.destination_id == workflow_id)
            .collect::<Vec<_>>();
        messages.sort_by_key(|message| message.created_at);
        Ok(messages)
    }

    async fn set_event(&self, event: WorkflowEvent) -> Result<()> {
        self.put("event", &event_key(&event.workflow_uuid, &event.key), &event).await
    }

    async fn get_event(&self, workflow_id: &str, key: &str) -> Result<Option<WorkflowEvent>> {
        self.get("event", &event_key(workflow_id, key)).await
    }

    async fn list_events(&self, workflow_id: &str) -> Result<Vec<WorkflowEvent>> {
        let mut events = self
            .list::<WorkflowEvent>("event")
            .await?
            .into_iter()
            .filter(|event| event.workflow_uuid == workflow_id)
            .collect::<Vec<_>>();
        events.sort_by_key(|event| event.created_at);
        Ok(events)
    }

    async fn write_stream(&self, entry: StreamEntry) -> Result<()> {
        self.put("stream", &stream_key(&entry.workflow_uuid, &entry.key, entry.offset), &entry).await
    }

    async fn read_stream(&self, workflow_id: &str, key: &str) -> Result<Vec<StreamEntry>> {
        let mut entries = self
            .list::<StreamEntry>("stream")
            .await?
            .into_iter()
            .filter(|entry| entry.workflow_uuid == workflow_id && entry.key == key)
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.offset);
        Ok(entries)
    }

    async fn list_streams(&self, workflow_id: &str) -> Result<Vec<StreamEntry>> {
        let mut entries = self
            .list::<StreamEntry>("stream")
            .await?
            .into_iter()
            .filter(|entry| entry.workflow_uuid == workflow_id)
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.key.cmp(&right.key).then(left.offset.cmp(&right.offset)));
        Ok(entries)
    }

    async fn close_stream(&self, workflow_id: &str, key: &str) -> Result<()> {
        let offset = i64::try_from(self.read_stream(workflow_id, key).await?.len())
            .map_err(|_| DbosError::invalid_argument("stream offset overflow"))?;
        self.write_stream(StreamEntry {
            workflow_uuid: workflow_id.to_string(),
            key: key.to_string(),
            offset,
            value: None,
            serialization: crate::serialization::DBOS_JSON.to_string(),
            closed: true,
            created_at: Utc::now(),
        })
        .await
    }

    async fn create_application_version(&self, version: VersionInfo) -> Result<()> {
        if self.get::<VersionInfo>("application_version", &version.name).await?.is_none() {
            self.put("application_version", &version.name, &version).await?;
        }
        Ok(())
    }

    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>> {
        let mut versions = self.list::<VersionInfo>("application_version").await?;
        versions.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        Ok(versions)
    }

    async fn set_latest_application_version(&self, name: &str) -> Result<()> {
        let now = Utc::now();
        let mut version = self.get::<VersionInfo>("application_version", name).await?.unwrap_or(VersionInfo {
            name: name.to_string(),
            created_at: now,
            updated_at: now,
        });
        version.updated_at = now;
        self.put("application_version", name, &version).await
    }

    async fn set_patch(&self, patch_name: &str, active: bool) -> Result<()> {
        self.put("patch", patch_name, &PatchState { active }).await
    }

    async fn get_patch(&self, patch_name: &str) -> Result<Option<bool>> {
        Ok(self.get::<PatchState>("patch", patch_name).await?.map(|state| state.active))
    }
}

#[cfg(feature = "turso")]
pub(crate) struct TursoStore {
    connection: Mutex<turso::Connection>,
}

#[cfg(feature = "turso")]
impl TursoStore {
    pub(crate) async fn connect(path: &str) -> Result<Arc<dyn SystemDatabase>> {
        if path.is_empty() {
            return Err(DbosError::invalid_argument("turso database path is required"));
        }
        let database = turso::Builder::new_local(path).build().await.map_err(DbosError::from)?;
        let connection = database.connect().map_err(DbosError::from)?;
        Ok(Arc::new(Self { connection: Mutex::new(connection) }))
    }

    async fn put<T: Serialize + Send + Sync>(&self, kind: &str, id: &str, value: &T) -> Result<()> {
        let payload = serde_json::to_string(value)?;
        let connection = self.connection.lock().await;
        connection
            .execute(
                "INSERT INTO dbos_state (kind, id, payload, updated_at) VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP) \
                 ON CONFLICT (kind, id) DO UPDATE SET payload = excluded.payload, updated_at = CURRENT_TIMESTAMP",
                turso::params![kind, id, payload],
            )
            .await
            .map_err(DbosError::from)?;
        Ok(())
    }

    async fn get<T: DeserializeOwned>(&self, kind: &str, id: &str) -> Result<Option<T>> {
        let connection = self.connection.lock().await;
        let mut rows = connection
            .query("SELECT payload FROM dbos_state WHERE kind = ?1 AND id = ?2", turso::params![kind, id])
            .await
            .map_err(DbosError::from)?;
        let Some(row) = rows.next().await.map_err(DbosError::from)? else {
            return Ok(None);
        };
        let payload = row.get::<String>(0).map_err(DbosError::from)?;
        serde_json::from_str(&payload).map(Some).map_err(DbosError::from)
    }

    async fn list<T: DeserializeOwned>(&self, kind: &str) -> Result<Vec<T>> {
        let connection = self.connection.lock().await;
        let mut rows = connection
            .query("SELECT payload FROM dbos_state WHERE kind = ?1", turso::params![kind])
            .await
            .map_err(DbosError::from)?;
        let mut values = Vec::new();
        while let Some(row) = rows.next().await.map_err(DbosError::from)? {
            let payload = row.get::<String>(0).map_err(DbosError::from)?;
            values.push(serde_json::from_str(&payload).map_err(DbosError::from)?);
        }
        Ok(values)
    }

    async fn delete(&self, kind: &str, id: &str) -> Result<()> {
        let connection = self.connection.lock().await;
        connection
            .execute("DELETE FROM dbos_state WHERE kind = ?1 AND id = ?2", turso::params![kind, id])
            .await
            .map_err(DbosError::from)?;
        Ok(())
    }
}

#[cfg(feature = "turso")]
#[async_trait]
impl SystemDatabase for TursoStore {
    async fn migrate(&self) -> Result<()> {
        let connection = self.connection.lock().await;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS dbos_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
                 CREATE TABLE IF NOT EXISTS dbos_state (
                    kind TEXT NOT NULL,
                    id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (kind, id)
                 );
                 CREATE INDEX IF NOT EXISTS dbos_state_kind_updated_idx ON dbos_state (kind, updated_at);",
            )
            .await
            .map_err(DbosError::from)?;
        connection
            .execute(
                "INSERT OR IGNORE INTO dbos_migrations (version) VALUES (?1)",
                turso::params!["rust-0001-json-state-store"],
            )
            .await
            .map_err(DbosError::from)?;
        Ok(())
    }

    async fn insert_workflow(&self, workflow: WorkflowStatus) -> Result<()> {
        if self.get_workflow(&workflow.workflow_uuid).await?.is_some() {
            return Err(DbosError::new(
                crate::error::DbosErrorCode::ConflictingWorkflow,
                format!("conflicting workflow invocation with the same ID ({})", workflow.workflow_uuid),
            ));
        }
        self.put("workflow", &workflow.workflow_uuid, &workflow).await
    }

    async fn save_workflow(&self, workflow: WorkflowStatus) -> Result<()> {
        self.put("workflow", &workflow.workflow_uuid, &workflow).await
    }

    async fn get_workflow(&self, workflow_id: &str) -> Result<Option<WorkflowStatus>> {
        self.get("workflow", workflow_id).await
    }

    async fn list_workflows(&self, options: &ListWorkflowsOptions) -> Result<Vec<WorkflowStatus>> {
        let mut rows = self
            .list::<WorkflowStatus>("workflow")
            .await?
            .into_iter()
            .filter(|workflow| workflow_matches(workflow, options))
            .collect::<Vec<_>>();
        sort_and_page_workflows(&mut rows, options);
        apply_workflow_load_options(&mut rows, options);
        Ok(rows)
    }

    async fn delete_workflows(&self, workflow_ids: &[String], _options: &DeleteWorkflowOptions) -> Result<()> {
        for workflow_id in workflow_ids {
            self.delete("workflow", workflow_id).await?;
            let steps = self.list_steps(workflow_id).await?;
            for step in steps {
                self.delete("step", &step_key(workflow_id, step.step_id)).await?;
            }
            for event in self.list_events(workflow_id).await? {
                self.delete("event", &event_key(workflow_id, &event.key)).await?;
            }
            for entry in self.list_streams(workflow_id).await? {
                self.delete("stream", &stream_key(workflow_id, &entry.key, entry.offset)).await?;
            }
            for message in self.list_messages(workflow_id).await? {
                self.delete("message", &message_key(&message)).await?;
            }
        }
        Ok(())
    }

    async fn record_step(&self, step: StepInfo) -> Result<()> {
        self.put("step", &step_key(&step.workflow_uuid, step.step_id), &step).await
    }

    async fn get_step(&self, workflow_id: &str, step_id: i32) -> Result<Option<StepInfo>> {
        self.get("step", &step_key(workflow_id, step_id)).await
    }

    async fn list_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        let mut steps =
            self.list::<StepInfo>("step").await?.into_iter().filter(|step| step.workflow_uuid == workflow_id).collect::<Vec<_>>();
        steps.sort_by_key(|step| step.step_id);
        Ok(steps)
    }

    async fn upsert_queue(&self, queue: WorkflowQueue) -> Result<()> {
        self.put("queue", &queue.name, &queue).await
    }

    async fn get_queue(&self, name: &str) -> Result<Option<WorkflowQueue>> {
        self.get("queue", name).await
    }

    async fn list_queues(&self) -> Result<Vec<WorkflowQueue>> {
        self.list("queue").await
    }

    async fn delete_queue(&self, name: &str) -> Result<()> {
        self.delete("queue", name).await
    }

    async fn upsert_schedule(&self, schedule: WorkflowSchedule) -> Result<()> {
        self.put("schedule", &schedule.schedule_name, &schedule).await
    }

    async fn get_schedule(&self, name: &str) -> Result<Option<WorkflowSchedule>> {
        self.get("schedule", name).await
    }

    async fn list_schedules(&self, options: &ListSchedulesOptions) -> Result<Vec<WorkflowSchedule>> {
        Ok(self
            .list::<WorkflowSchedule>("schedule")
            .await?
            .into_iter()
            .filter(|schedule| schedule_matches(schedule, options))
            .collect())
    }

    async fn delete_schedule(&self, name: &str) -> Result<()> {
        self.delete("schedule", name).await
    }

    async fn send_message(&self, message: WorkflowMessage) -> Result<()> {
        let id = message_key(&message);
        self.put("message", &id, &message).await
    }

    async fn recv_message(&self, destination_id: &str, topic: &str) -> Result<Option<WorkflowMessage>> {
        let mut messages = self
            .list::<WorkflowMessage>("message")
            .await?
            .into_iter()
            .filter(|message| !message.consumed && message.destination_id == destination_id && message.topic == topic)
            .collect::<Vec<_>>();
        messages.sort_by_key(|message| message.created_at);
        let Some(mut message) = messages.into_iter().next() else {
            return Ok(None);
        };
        message.consumed = true;
        let id = message_key(&message);
        self.put("message", &id, &message).await?;
        Ok(Some(message))
    }

    async fn list_messages(&self, workflow_id: &str) -> Result<Vec<WorkflowMessage>> {
        let mut messages = self
            .list::<WorkflowMessage>("message")
            .await?
            .into_iter()
            .filter(|message| message.destination_id == workflow_id)
            .collect::<Vec<_>>();
        messages.sort_by_key(|message| message.created_at);
        Ok(messages)
    }

    async fn set_event(&self, event: WorkflowEvent) -> Result<()> {
        self.put("event", &event_key(&event.workflow_uuid, &event.key), &event).await
    }

    async fn get_event(&self, workflow_id: &str, key: &str) -> Result<Option<WorkflowEvent>> {
        self.get("event", &event_key(workflow_id, key)).await
    }

    async fn list_events(&self, workflow_id: &str) -> Result<Vec<WorkflowEvent>> {
        let mut events = self
            .list::<WorkflowEvent>("event")
            .await?
            .into_iter()
            .filter(|event| event.workflow_uuid == workflow_id)
            .collect::<Vec<_>>();
        events.sort_by_key(|event| event.created_at);
        Ok(events)
    }

    async fn write_stream(&self, entry: StreamEntry) -> Result<()> {
        self.put("stream", &stream_key(&entry.workflow_uuid, &entry.key, entry.offset), &entry).await
    }

    async fn read_stream(&self, workflow_id: &str, key: &str) -> Result<Vec<StreamEntry>> {
        let mut entries = self
            .list::<StreamEntry>("stream")
            .await?
            .into_iter()
            .filter(|entry| entry.workflow_uuid == workflow_id && entry.key == key)
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.offset);
        Ok(entries)
    }

    async fn list_streams(&self, workflow_id: &str) -> Result<Vec<StreamEntry>> {
        let mut entries = self
            .list::<StreamEntry>("stream")
            .await?
            .into_iter()
            .filter(|entry| entry.workflow_uuid == workflow_id)
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.key.cmp(&right.key).then(left.offset.cmp(&right.offset)));
        Ok(entries)
    }

    async fn close_stream(&self, workflow_id: &str, key: &str) -> Result<()> {
        let offset = i64::try_from(self.read_stream(workflow_id, key).await?.len())
            .map_err(|_| DbosError::invalid_argument("stream offset overflow"))?;
        self.write_stream(StreamEntry {
            workflow_uuid: workflow_id.to_string(),
            key: key.to_string(),
            offset,
            value: None,
            serialization: crate::serialization::DBOS_JSON.to_string(),
            closed: true,
            created_at: Utc::now(),
        })
        .await
    }

    async fn create_application_version(&self, version: VersionInfo) -> Result<()> {
        if self.get::<VersionInfo>("application_version", &version.name).await?.is_none() {
            self.put("application_version", &version.name, &version).await?;
        }
        Ok(())
    }

    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>> {
        let mut versions = self.list::<VersionInfo>("application_version").await?;
        versions.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        Ok(versions)
    }

    async fn set_latest_application_version(&self, name: &str) -> Result<()> {
        let now = Utc::now();
        let mut version = self.get::<VersionInfo>("application_version", name).await?.unwrap_or(VersionInfo {
            name: name.to_string(),
            created_at: now,
            updated_at: now,
        });
        version.updated_at = now;
        self.put("application_version", name, &version).await
    }

    async fn set_patch(&self, patch_name: &str, active: bool) -> Result<()> {
        self.put("patch", patch_name, &PatchState { active }).await
    }

    async fn get_patch(&self, patch_name: &str) -> Result<Option<bool>> {
        Ok(self.get::<PatchState>("patch", patch_name).await?.map(|state| state.active))
    }
}

#[cfg(any(feature = "postgres", feature = "turso"))]
#[derive(Debug, Serialize, serde::Deserialize)]
struct PatchState {
    active: bool,
}

#[cfg(any(feature = "postgres", feature = "turso"))]
fn step_key(workflow_id: &str, step_id: i32) -> String {
    format!("{workflow_id}:{step_id}")
}

#[cfg(any(feature = "postgres", feature = "turso"))]
fn event_key(workflow_id: &str, key: &str) -> String {
    format!("{workflow_id}:{key}")
}

#[cfg(any(feature = "postgres", feature = "turso"))]
fn stream_key(workflow_id: &str, key: &str, offset: i64) -> String {
    format!("{workflow_id}:{key}:{offset}")
}

#[cfg(any(feature = "postgres", feature = "turso"))]
fn message_key(message: &WorkflowMessage) -> String {
    let nanos = message.created_at.timestamp_nanos_opt().map_or(0, |value| value);
    format!("{}:{}:{}", message.destination_id, message.topic, nanos)
}

#[cfg(feature = "postgres")]
fn reconnect_timeout() -> Duration {
    std::env::var("DBOS_POSTGRES_RECONNECT_TIMEOUT_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(5))
}

#[cfg(feature = "postgres")]
fn validate_schema_name(schema: &str) -> Result<()> {
    if schema.is_empty() || !schema.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(DbosError::invalid_argument(format!("invalid postgres schema name {schema:?}")));
    }
    Ok(())
}

pub(crate) fn workflow_counts_by_status(rows: &[WorkflowStatus]) -> Vec<crate::types::WorkflowAggregateRow> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for row in rows {
        let key = format!("{:?}", row.status);
        *counts.entry(key).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(status, count)| {
            let mut bucket = BTreeMap::new();
            bucket.insert("status".to_string(), Value::String(status));
            crate::types::WorkflowAggregateRow { bucket, count }
        })
        .collect()
}

pub(crate) fn step_counts_by_name(rows: &[StepInfo]) -> Vec<crate::types::StepAggregateRow> {
    let mut counts: BTreeMap<String, (u64, Option<i64>)> = BTreeMap::new();
    for row in rows {
        let duration = row.completed_at.signed_duration_since(row.started_at).num_milliseconds();
        let entry = counts.entry(row.step_name.clone()).or_insert((0, None));
        entry.0 += 1;
        entry.1 = Some(entry.1.map_or(duration, |current| current.max(duration)));
    }
    counts
        .into_iter()
        .map(|(name, (count, max_duration_ms))| {
            let mut bucket = BTreeMap::new();
            bucket.insert("function_name".to_string(), Value::String(name));
            crate::types::StepAggregateRow { bucket, count, max_duration_ms }
        })
        .collect()
}
