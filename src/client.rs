use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::context::{DbosConfig, DbosContext, WorkflowHandle};
use crate::error::Result;
use crate::serialization::CustomSerializer;
use crate::store::SystemDatabaseHandle;
use crate::types::{
    CreateScheduleRequest, DeleteWorkflowOptions, EnqueueOptions, ExportWorkflowOptions, ForkWorkflowInput, GetStepAggregatesInput,
    GetWorkflowAggregatesInput, GetWorkflowStepsOptions, ListRegisteredWorkflowsOptions, ListSchedulesOptions, ListWorkflowsOptions,
    ReadStreamOptions, ResumeWorkflowOptions, SendOptions, SetWorkflowDelayOptions, StepAggregateRow, StepInfo, StreamValue, VersionInfo,
    WorkflowAggregateRow, WorkflowExport, WorkflowOptions, WorkflowQueue, WorkflowRegistryEntry, WorkflowSchedule, WorkflowStatus,
};

#[derive(Clone)]
pub struct ClientConfig {
    pub database_url: Option<String>,
    pub turso_path: Option<String>,
    pub database_schema: String,
    pub system_database: Option<SystemDatabaseHandle>,
    pub serializer: Option<Arc<dyn CustomSerializer>>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            database_url: None,
            turso_path: None,
            database_schema: "dbos".to_string(),
            system_database: None,
            serializer: None,
        }
    }
}

impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig")
            .field("database_url", &self.database_url)
            .field("turso_path", &self.turso_path)
            .field("database_schema", &self.database_schema)
            .field("system_database", &self.system_database)
            .field("serializer", &self.serializer.as_ref().map(|serializer| serializer.name()))
            .finish()
    }
}

#[derive(Clone)]
pub struct DbosClient {
    ctx: DbosContext,
}

impl DbosClient {
    pub async fn new(config: ClientConfig) -> Result<Self> {
        let mut ctx_config = DbosConfig::new("dbos-client");
        ctx_config.database_url = config.database_url;
        ctx_config.turso_path = config.turso_path;
        ctx_config.database_schema = config.database_schema;
        ctx_config.system_database = config.system_database;
        ctx_config.serializer = config.serializer;
        let ctx = DbosContext::new(ctx_config).await?;
        ctx.initialize_system_database().await?;
        Ok(Self { ctx })
    }

    pub fn context(&self) -> &DbosContext {
        &self.ctx
    }

    pub async fn enqueue<I, O>(
        &self,
        queue_name: impl Into<String>,
        workflow_name: impl Into<String>,
        input: I,
        options: EnqueueOptions,
    ) -> Result<WorkflowHandle<O>>
    where
        I: Serialize + Send,
        O: DeserializeOwned + Send + 'static,
    {
        let workflow_options = WorkflowOptions {
            workflow_id: options.workflow_id,
            queue_name: Some(queue_name.into()),
            application_version: options.application_version,
            deduplication_id: options.deduplication_id,
            priority: options.priority,
            timeout: options.timeout,
            delay: options.delay,
            authenticated_user: options.authenticated_user,
            assumed_role: options.assumed_role,
            authenticated_roles: options.authenticated_roles,
            queue_partition_key: options.queue_partition_key,
            class_name: options.class_name,
            config_name: options.config_name,
            parent_workflow_id: None,
            max_retries: None,
            portable: options.portable,
        };
        self.ctx.run_workflow(workflow_name.into(), input, workflow_options).await
    }

    pub async fn list_workflows(&self, options: ListWorkflowsOptions) -> Result<Vec<WorkflowStatus>> {
        self.ctx.list_workflows(options).await
    }

    pub async fn list_registered_workflows(&self, options: ListRegisteredWorkflowsOptions) -> Result<Vec<WorkflowRegistryEntry>> {
        self.ctx.list_registered_workflows(options).await
    }

    pub async fn list_registered_queues(&self) -> Result<Vec<WorkflowQueue>> {
        self.ctx.list_registered_queues().await
    }

    pub async fn send<T: Serialize>(&self, destination_id: &str, message: T, topic: &str) -> Result<()> {
        self.ctx.send(destination_id, message, topic).await
    }

    pub async fn send_with_options<T: Serialize>(&self, destination_id: &str, message: T, topic: &str, options: SendOptions) -> Result<()> {
        self.ctx.send_with_options(destination_id, message, topic, options).await
    }

    pub async fn get_event<T: DeserializeOwned>(&self, target_workflow_id: &str, key: &str, timeout: Duration) -> Result<T> {
        self.ctx.get_event(target_workflow_id, key, timeout).await
    }

    pub async fn retrieve_workflow<O>(&self, workflow_id: impl Into<String>) -> WorkflowHandle<O>
    where
        O: DeserializeOwned + Send + 'static,
    {
        self.ctx.retrieve_workflow(workflow_id).await
    }

    pub async fn cancel_workflow(&self, workflow_id: &str) -> Result<()> {
        self.ctx.cancel_workflow(workflow_id).await
    }

    pub async fn cancel_workflows(&self, workflow_ids: &[String]) -> Result<()> {
        self.ctx.cancel_workflows(workflow_ids).await
    }

    pub async fn set_workflow_delay(&self, workflow_id: &str, options: SetWorkflowDelayOptions) -> Result<()> {
        self.ctx.set_workflow_delay(workflow_id, options).await
    }

    pub async fn delete_workflows(&self, workflow_ids: &[String], options: DeleteWorkflowOptions) -> Result<()> {
        self.ctx.delete_workflows(workflow_ids, options).await
    }

    pub async fn resume_workflow<O>(&self, workflow_id: &str, options: ResumeWorkflowOptions) -> Result<WorkflowHandle<O>>
    where
        O: DeserializeOwned + Send + 'static,
    {
        self.ctx.resume_workflow(workflow_id, options).await
    }

    pub async fn resume_workflows<O>(&self, workflow_ids: &[String], options: ResumeWorkflowOptions) -> Result<Vec<WorkflowHandle<O>>>
    where
        O: DeserializeOwned + Send + 'static,
    {
        self.ctx.resume_workflows(workflow_ids, options).await
    }

    pub async fn fork_workflow<O>(&self, input: ForkWorkflowInput) -> Result<WorkflowHandle<O>>
    where
        O: DeserializeOwned + Send + 'static,
    {
        self.ctx.fork_workflow(input).await
    }

    pub async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        self.ctx.get_workflow_steps(workflow_id).await
    }

    pub async fn get_workflow_steps_with_options(&self, workflow_id: &str, options: GetWorkflowStepsOptions) -> Result<Vec<StepInfo>> {
        self.ctx.get_workflow_steps_with_options(workflow_id, options).await
    }

    pub async fn export_workflow(&self, workflow_id: &str) -> Result<WorkflowExport> {
        self.ctx.export_workflow(workflow_id).await
    }

    pub async fn export_workflow_with_options(&self, workflow_id: &str, options: ExportWorkflowOptions) -> Result<WorkflowExport> {
        self.ctx.export_workflow_with_options(workflow_id, options).await
    }

    pub async fn import_workflow(&self, export: WorkflowExport) -> Result<()> {
        self.ctx.import_workflow(export).await
    }

    pub async fn read_stream<T: DeserializeOwned>(&self, workflow_id: &str, key: &str) -> Result<(Vec<T>, bool)> {
        self.ctx.read_stream(workflow_id, key).await
    }

    pub async fn read_stream_with_options<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
        options: ReadStreamOptions,
    ) -> Result<(Vec<T>, bool)> {
        self.ctx.read_stream_with_options(workflow_id, key, options).await
    }

    pub async fn read_stream_async<T: DeserializeOwned + Send + 'static>(
        &self,
        workflow_id: String,
        key: String,
    ) -> Result<tokio::sync::mpsc::Receiver<StreamValue<T>>> {
        self.ctx.read_stream_async(workflow_id, key).await
    }

    pub async fn create_schedule(&self, input: CreateScheduleRequest) -> Result<()> {
        self.ctx.create_schedule(input).await
    }

    pub async fn apply_schedules(&self, schedules: Vec<CreateScheduleRequest>) -> Result<()> {
        self.ctx.apply_schedules(schedules).await
    }

    pub async fn get_schedule(&self, schedule_name: &str) -> Result<Option<WorkflowSchedule>> {
        self.ctx.get_schedule(schedule_name).await
    }

    pub async fn list_schedules(&self, options: ListSchedulesOptions) -> Result<Vec<WorkflowSchedule>> {
        self.ctx.list_schedules(options).await
    }

    pub async fn pause_schedule(&self, schedule_name: &str) -> Result<()> {
        self.ctx.pause_schedule(schedule_name).await
    }

    pub async fn resume_schedule(&self, schedule_name: &str) -> Result<()> {
        self.ctx.resume_schedule(schedule_name).await
    }

    pub async fn delete_schedule(&self, schedule_name: &str) -> Result<()> {
        self.ctx.delete_schedule(schedule_name).await
    }

    pub async fn backfill_schedule(
        &self,
        schedule_name: &str,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<String>> {
        self.ctx.backfill_schedule(schedule_name, start, end).await
    }

    pub async fn trigger_schedule(&self, schedule_name: &str) -> Result<WorkflowHandle<Value>> {
        self.ctx.trigger_schedule(schedule_name).await
    }

    pub async fn register_queue(&self, queue: WorkflowQueue) -> Result<WorkflowQueue> {
        self.ctx.register_queue(queue).await
    }

    pub async fn retrieve_queue(&self, name: &str) -> Result<Option<WorkflowQueue>> {
        self.ctx.retrieve_queue(name).await
    }

    pub async fn list_queues(&self) -> Result<Vec<WorkflowQueue>> {
        self.ctx.list_queues().await
    }

    pub async fn delete_queue(&self, name: &str) -> Result<()> {
        self.ctx.delete_queue(name).await
    }

    pub async fn update_queue(&self, queue: WorkflowQueue) -> Result<WorkflowQueue> {
        self.ctx.update_queue(queue).await
    }

    pub async fn list_application_versions(&self) -> Result<Vec<VersionInfo>> {
        self.ctx.list_application_versions().await
    }

    pub async fn get_latest_application_version(&self) -> Result<Option<VersionInfo>> {
        self.ctx.get_latest_application_version().await
    }

    pub async fn set_latest_application_version(&self, version_name: &str) -> Result<()> {
        self.ctx.set_latest_application_version(version_name).await
    }

    pub async fn get_workflow_aggregates_with_input(&self, input: GetWorkflowAggregatesInput) -> Result<Vec<WorkflowAggregateRow>> {
        self.ctx.get_workflow_aggregates_with_input(input).await
    }

    pub async fn get_step_aggregates_with_input(&self, input: GetStepAggregatesInput) -> Result<Vec<StepAggregateRow>> {
        self.ctx.get_step_aggregates_with_input(input).await
    }

    pub async fn shutdown(&self, timeout: Duration) {
        self.ctx.shutdown(timeout).await;
    }
}

pub async fn new_client(config: ClientConfig) -> Result<DbosClient> {
    DbosClient::new(config).await
}

pub async fn enqueue<I, O>(
    client: &DbosClient,
    queue_name: impl Into<String>,
    workflow_name: impl Into<String>,
    input: I,
    options: EnqueueOptions,
) -> Result<WorkflowHandle<O>>
where
    I: Serialize + Send,
    O: DeserializeOwned + Send + 'static,
{
    client.enqueue(queue_name, workflow_name, input, options).await
}
