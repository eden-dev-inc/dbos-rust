use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
#[cfg(feature = "conductor")]
use futures::{Sink, SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::context::DbosContext;
use crate::error::{DbosError, DbosErrorCode, Result};
use crate::types::{
    DeleteWorkflowOptions, ExportWorkflowOptions, ForkWorkflowInput, GetStepAggregatesInput, GetWorkflowAggregatesInput,
    ListSchedulesOptions, ListWorkflowsOptions, ResumeWorkflowOptions, ScheduleStatus, WorkflowExport, WorkflowStatusType,
};

pub type AlertHandler = Arc<dyn Fn(String, String, BTreeMap<String, String>) + Send + Sync>;

#[async_trait]
pub trait ConductorTransport: Send + Sync {
    async fn run(&self, ctx: DbosContext, config: ConductorConfig, shutdown: watch::Receiver<bool>) -> Result<()>;
}

#[derive(Clone)]
pub struct ConductorConfig {
    pub url: String,
    pub api_key: String,
    pub app_name: String,
    pub executor_metadata: Option<Value>,
    pub alert_handler: Option<AlertHandler>,
    pub transport: Option<Arc<dyn ConductorTransport>>,
    pub reconnect_interval: Duration,
}

impl std::fmt::Debug for ConductorConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConductorConfig")
            .field("url", &self.url)
            .field("api_key", &"<redacted>")
            .field("app_name", &self.app_name)
            .field("executor_metadata", &self.executor_metadata)
            .field("alert_handler", &self.alert_handler.as_ref().map(|_| "<handler>"))
            .field("transport", &self.transport.as_ref().map(|_| "<transport>"))
            .field("reconnect_interval", &self.reconnect_interval)
            .finish()
    }
}

pub struct ConductorHandle {
    pub url: String,
    config: ConductorConfig,
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<Result<()>>>,
}

impl std::fmt::Debug for ConductorHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConductorHandle")
            .field("url", &self.url)
            .field("config", &self.config)
            .field("task", &self.task.as_ref().map(|_| "<task>"))
            .finish()
    }
}

impl ConductorHandle {
    pub fn config(&self) -> &ConductorConfig {
        &self.config
    }

    pub async fn shutdown(mut self, timeout: Duration) -> Result<()> {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take() {
            let mut task = task;
            tokio::select! {
                result = &mut task => match result {
                    Ok(result) => result,
                    Err(err) => Err(DbosError::with_source(
                        DbosErrorCode::Initialization,
                        "conductor task failed while shutting down",
                        err,
                    )),
                },
                _ = tokio::time::sleep(timeout) => {
                    task.abort();
                    let _ = task.await;
                    Ok(())
                }
            }
        } else {
            Ok(())
        }
    }
}

pub async fn connect_conductor(ctx: DbosContext, config: ConductorConfig) -> Result<ConductorHandle> {
    #[cfg(feature = "conductor")]
    {
        if config.url.is_empty() {
            return Err(DbosError::invalid_argument("conductor URL is required"));
        }
        if config.api_key.is_empty() {
            return Err(DbosError::invalid_argument("conductor API key is required"));
        }
        let (shutdown, shutdown_rx) = watch::channel(false);
        let transport = config.transport.clone().unwrap_or_else(default_conductor_transport);
        let task_config = config.clone();
        let task = tokio::spawn(async move { transport.run(ctx, task_config, shutdown_rx).await });
        Ok(ConductorHandle { url: config.url.clone(), config, shutdown, task: Some(task) })
    }
    #[cfg(not(feature = "conductor"))]
    {
        let _ = ctx;
        let _ = config;
        Err(DbosError::unsupported("conductor support requires the conductor feature"))
    }
}

#[cfg(feature = "conductor")]
fn default_conductor_transport() -> Arc<dyn ConductorTransport> {
    Arc::new(WebSocketConductorTransport)
}

#[cfg(feature = "conductor")]
struct WebSocketConductorTransport;

#[cfg(feature = "conductor")]
#[async_trait]
impl ConductorTransport for WebSocketConductorTransport {
    async fn run(&self, ctx: DbosContext, config: ConductorConfig, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            match run_websocket_conductor_session(&ctx, &config, shutdown.clone()).await {
                Ok(()) if *shutdown.borrow() => return Ok(()),
                Ok(()) => tracing::warn!("DBOS conductor websocket disconnected; reconnecting"),
                Err(error) => tracing::warn!(error = %error, "DBOS conductor websocket session failed; reconnecting"),
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    let _ = changed;
                    return Ok(());
                }
                _ = tokio::time::sleep(config.reconnect_interval.max(Duration::from_millis(100))) => {}
            }
        }
    }
}

#[cfg(feature = "conductor")]
async fn run_websocket_conductor_session(ctx: &DbosContext, config: &ConductorConfig, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    use tokio_tungstenite::tungstenite::Message;

    let websocket_url = conductor_websocket_url(config);
    let redacted_websocket_url = conductor_websocket_url_redacted(config);
    let (stream, _) = tokio_tungstenite::connect_async(&websocket_url).await.map_err(|err| {
        DbosError::with_source(
            DbosErrorCode::Initialization,
            format!("failed to connect DBOS conductor websocket at {redacted_websocket_url}"),
            err,
        )
    })?;
    let (mut write, mut read) = stream.split();
    let mut ping_interval = tokio::time::interval(Duration::from_secs(20));
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                let _ = changed;
                let _ = write.close().await;
                return Ok(());
            }
            _ = ping_interval.tick() => {
                write.send(Message::Ping(Vec::new().into())).await.map_err(|err| {
                    DbosError::with_source(DbosErrorCode::Initialization, "failed to send DBOS conductor websocket ping", err)
                })?;
            }
            message = read.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        process_websocket_conductor_message(ctx, config, text.as_str(), &mut write).await?;
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        let text = String::from_utf8(bytes.to_vec()).map_err(|err| {
                            DbosError::with_source(DbosErrorCode::Serialization, "DBOS conductor sent invalid UTF-8 binary payload", err)
                        })?;
                        process_websocket_conductor_message(ctx, config, &text, &mut write).await?;
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        write.send(Message::Pong(payload)).await.map_err(|err| {
                            DbosError::with_source(DbosErrorCode::Initialization, "failed to send DBOS conductor websocket pong", err)
                        })?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Err(err)) => {
                        return Err(DbosError::with_source(
                            DbosErrorCode::Initialization,
                            "failed to read DBOS conductor websocket message",
                            err,
                        ));
                    }
                }
            }
        }
    }
}

#[cfg(feature = "conductor")]
async fn process_websocket_conductor_message<S>(ctx: &DbosContext, config: &ConductorConfig, input: &str, write: &mut S) -> Result<()>
where
    S: Sink<tokio_tungstenite::tungstenite::Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let request = decode_conductor_wire_request(input)?;
    let response = handle_conductor_request_with_config(ctx, config, request).await;
    let response = encode_conductor_wire_response(&response)?;
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(response.into()))
        .await
        .map_err(|err| DbosError::with_source(DbosErrorCode::Initialization, "failed to send DBOS conductor response", err))
}

#[cfg(feature = "conductor")]
fn conductor_websocket_url(config: &ConductorConfig) -> String {
    conductor_websocket_url_with_key(config, &config.api_key)
}

#[cfg(feature = "conductor")]
fn conductor_websocket_url_redacted(config: &ConductorConfig) -> String {
    conductor_websocket_url_with_key(config, "REDACTED")
}

#[cfg(feature = "conductor")]
fn conductor_websocket_url_with_key(config: &ConductorConfig, api_key: &str) -> String {
    let base = config.url.trim_end_matches('/');
    let base = base
        .strip_prefix("http://")
        .map(|rest| format!("ws://{rest}"))
        .unwrap_or_else(|| base.strip_prefix("https://").map(|rest| format!("wss://{rest}")).unwrap_or_else(|| base.to_string()));
    let app_name = urlencoding::encode(&config.app_name);
    let api_key = urlencoding::encode(api_key);
    format!("{base}/websocket/{app_name}/{api_key}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConductorMessageKind {
    ExecutorInfo,
    Recovery,
    ExistPendingWorkflows,
    #[serde(rename = "cancel", alias = "cancel_workflow")]
    CancelWorkflow,
    #[serde(rename = "resume", alias = "resume_workflow")]
    ResumeWorkflow,
    ListWorkflows,
    ListQueuedWorkflows,
    ListSteps,
    GetWorkflow,
    ForkWorkflow,
    Retention,
    GetMetrics,
    ExportWorkflow,
    ImportWorkflow,
    #[serde(rename = "delete", alias = "delete_workflow")]
    DeleteWorkflow,
    Alert,
    ListSchedules,
    GetSchedule,
    PauseSchedule,
    ResumeSchedule,
    BackfillSchedule,
    TriggerSchedule,
    GetWorkflowEvents,
    GetWorkflowNotifications,
    GetWorkflowStreams,
    GetWorkflowAggregates,
    GetStepAggregates,
    ListApplicationVersions,
    SetLatestApplicationVersion,
    ListQueues,
    GetQueue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConductorRequest {
    pub request_id: Option<String>,
    #[serde(alias = "type")]
    pub kind: ConductorMessageKind,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConductorResponse {
    pub request_id: Option<String>,
    pub kind: ConductorMessageKind,
    pub ok: bool,
    pub payload: Value,
    pub error: Option<ConductorErrorPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConductorErrorPayload {
    pub code: DbosErrorCode,
    pub message: String,
}

pub async fn handle_conductor_request(ctx: &DbosContext, request: ConductorRequest) -> ConductorResponse {
    let request_id = request.request_id.clone();
    let kind = request.kind;
    match dispatch_conductor_request(ctx, request).await {
        Ok(payload) => ConductorResponse { request_id, kind, ok: true, payload, error: None },
        Err(error) => ConductorResponse {
            request_id,
            kind,
            ok: false,
            payload: Value::Null,
            error: Some(ConductorErrorPayload { code: error.code, message: error.message }),
        },
    }
}

pub async fn handle_conductor_request_with_config(
    ctx: &DbosContext,
    config: &ConductorConfig,
    request: ConductorRequest,
) -> ConductorResponse {
    let request_id = request.request_id.clone();
    let kind = request.kind;
    match dispatch_configured_conductor_request(ctx, config, request).await {
        Ok(payload) => ConductorResponse { request_id, kind, ok: true, payload, error: None },
        Err(error) => ConductorResponse {
            request_id,
            kind,
            ok: false,
            payload: Value::Null,
            error: Some(ConductorErrorPayload { code: error.code, message: error.message }),
        },
    }
}

async fn dispatch_configured_conductor_request(ctx: &DbosContext, config: &ConductorConfig, request: ConductorRequest) -> Result<Value> {
    if request.kind == ConductorMessageKind::Alert
        && let Some(alert_handler) = &config.alert_handler
    {
        let severity = string_field_optional(&request.payload, "severity").unwrap_or_else(|| "info".to_string());
        let message = string_field_optional(&request.payload, "message")
            .or_else(|| string_field_optional(&request.payload, "alert"))
            .unwrap_or_else(|| "DBOS conductor alert".to_string());
        let metadata = string_map_field(&request.payload, "metadata");
        alert_handler(severity, message, metadata);
    }
    dispatch_conductor_request(ctx, request).await
}

pub async fn dispatch_conductor_request(ctx: &DbosContext, request: ConductorRequest) -> Result<Value> {
    let payload = request.payload;
    match request.kind {
        ConductorMessageKind::ExecutorInfo => Ok(json!({
            "app_name": ctx.app_name(),
            "application_version": ctx.application_version(),
            "application_id": ctx.application_id(),
            "executor_id": ctx.executor_id(),
            "dbos_version": env!("CARGO_PKG_VERSION"),
            "language": "rust",
            "hostname": std::env::var("HOSTNAME").ok(),
            "executor_metadata": ctx.conductor_executor_metadata(),
        })),
        ConductorMessageKind::Recovery => {
            let executor_ids = string_vec_field(&payload, "executor_ids");
            let handles = ctx.recover_pending_workflows(&executor_ids).await?;
            let workflow_ids = handles.into_iter().map(|handle| handle.workflow_id().to_string()).collect::<Vec<_>>();
            Ok(json!({ "workflow_ids": workflow_ids }))
        }
        ConductorMessageKind::ExistPendingWorkflows => {
            let executor_id = string_field_optional(&payload, "executor_id");
            let application_version = string_field_optional(&payload, "application_version");
            let workflows = ctx
                .list_workflows(ListWorkflowsOptions {
                    status: vec![WorkflowStatusType::Pending],
                    application_version,
                    executor_ids: executor_id.into_iter().collect(),
                    limit: Some(1),
                    ..Default::default()
                })
                .await?;
            Ok(json!({ "exist": !workflows.is_empty() }))
        }
        ConductorMessageKind::CancelWorkflow => {
            let workflow_ids = workflow_ids_from_payload(&payload)?;
            ctx.cancel_workflows(&workflow_ids).await?;
            Ok(json!({ "workflow_ids": workflow_ids, "cancelled": true, "success": true }))
        }
        ConductorMessageKind::ResumeWorkflow => {
            let workflow_ids = workflow_ids_from_payload(&payload)?;
            let options =
                value_field(&payload, "options").map(from_value_or_default::<ResumeWorkflowOptions>).transpose()?.unwrap_or_default();
            let mut resumed = Vec::with_capacity(workflow_ids.len());
            for workflow_id in workflow_ids {
                let handle = ctx.resume_workflow::<Value>(&workflow_id, options.clone()).await?;
                resumed.push(handle.workflow_id().to_string());
            }
            Ok(json!({ "workflow_ids": resumed, "success": true }))
        }
        ConductorMessageKind::ListWorkflows => {
            let options = options_payload::<ListWorkflowsOptions>(&payload)?;
            let workflows = ctx.list_workflows(options).await?;
            Ok(json!({ "workflows": workflows }))
        }
        ConductorMessageKind::ListQueuedWorkflows => {
            let mut options = options_payload::<ListWorkflowsOptions>(&payload)?;
            options.status = vec![WorkflowStatusType::Enqueued];
            options.queues_only = true;
            if let Some(queue_name) = payload.get("queue_name").and_then(Value::as_str) {
                options.queue_name = Some(queue_name.to_string());
            }
            let workflows = ctx.list_workflows(options).await?;
            Ok(json!({ "workflows": workflows }))
        }
        ConductorMessageKind::ListSteps => {
            let workflow_id = string_field(&payload, "workflow_id")?;
            let steps = ctx.get_workflow_steps(&workflow_id).await?;
            Ok(json!({ "steps": steps }))
        }
        ConductorMessageKind::GetWorkflow => {
            let workflow_id = string_field(&payload, "workflow_id")?;
            let workflow = ctx.retrieve_workflow::<Value>(workflow_id).await.get_status().await?;
            Ok(json!({ "workflow": workflow }))
        }
        ConductorMessageKind::ForkWorkflow => {
            let input = from_value_or_default::<ForkWorkflowInput>(payload)?;
            let handle = ctx.fork_workflow::<Value>(input).await?;
            Ok(json!({ "workflow_id": handle.workflow_id() }))
        }
        ConductorMessageKind::Retention => Ok(json!({ "retention": "accepted" })),
        ConductorMessageKind::GetMetrics => {
            let workflow_id = payload.get("workflow_id").and_then(Value::as_str);
            let workflow_options = value_field(&payload, "workflow_options")
                .map(from_value_or_default::<ListWorkflowsOptions>)
                .transpose()?
                .unwrap_or_default();
            let workflow_aggregates = ctx.get_workflow_aggregates(workflow_options).await?;
            let step_aggregates = if let Some(workflow_id) = workflow_id {
                ctx.get_step_aggregates(workflow_id).await?
            } else {
                Vec::new()
            };
            Ok(json!({
                "workflow_aggregates": workflow_aggregates,
                "step_aggregates": step_aggregates,
            }))
        }
        ConductorMessageKind::ExportWorkflow => {
            let workflow_id = string_field(&payload, "workflow_id")?;
            let export = ctx.export_workflow_with_options(&workflow_id, options_payload::<ExportWorkflowOptions>(&payload)?).await?;
            Ok(serde_json::to_value(export)?)
        }
        ConductorMessageKind::ImportWorkflow => {
            let export = serde_json::from_value::<WorkflowExport>(payload).map_err(DbosError::from)?;
            let workflow_id = export.workflow.workflow_uuid.clone();
            ctx.import_workflow(export).await?;
            Ok(json!({ "workflow_id": workflow_id, "imported": true }))
        }
        ConductorMessageKind::DeleteWorkflow => {
            let workflow_ids = workflow_ids_from_payload(&payload)?;
            let force = payload.get("force").and_then(Value::as_bool).unwrap_or(false);
            ctx.delete_workflows(&workflow_ids, DeleteWorkflowOptions { force }).await?;
            Ok(json!({ "workflow_ids": workflow_ids, "deleted": true, "success": true }))
        }
        ConductorMessageKind::Alert => Ok(json!({ "alert": "accepted" })),
        ConductorMessageKind::ListSchedules => {
            let schedules = ctx.list_schedules(options_payload::<ListSchedulesOptions>(&payload)?).await?;
            Ok(json!({ "schedules": schedules }))
        }
        ConductorMessageKind::GetSchedule => {
            let schedule_name = string_field(&payload, "schedule_name")?;
            let schedule = ctx.get_schedule(&schedule_name).await?;
            Ok(json!({ "schedule": schedule }))
        }
        ConductorMessageKind::PauseSchedule => {
            let schedule_name = string_field(&payload, "schedule_name")?;
            ctx.pause_schedule(&schedule_name).await?;
            Ok(json!({ "schedule_name": schedule_name, "status": ScheduleStatus::Paused }))
        }
        ConductorMessageKind::ResumeSchedule => {
            let schedule_name = string_field(&payload, "schedule_name")?;
            ctx.resume_schedule(&schedule_name).await?;
            Ok(json!({ "schedule_name": schedule_name, "status": ScheduleStatus::Active }))
        }
        ConductorMessageKind::BackfillSchedule => {
            let schedule_name = string_field(&payload, "schedule_name")?;
            let start = chrono_field(&payload, "start")?;
            let end = chrono_field(&payload, "end")?;
            let workflow_ids = ctx.backfill_schedule(&schedule_name, start, end).await?;
            Ok(json!({ "workflow_ids": workflow_ids }))
        }
        ConductorMessageKind::TriggerSchedule => {
            let schedule_name = string_field(&payload, "schedule_name")?;
            let handle = ctx.trigger_schedule(&schedule_name).await?;
            Ok(json!({ "workflow_id": handle.workflow_id() }))
        }
        ConductorMessageKind::GetWorkflowEvents | ConductorMessageKind::GetWorkflowNotifications => {
            let workflow_id = string_field(&payload, "workflow_id")?;
            let key = string_field(&payload, "key")?;
            let value: Value = ctx.get_event(&workflow_id, &key, Duration::from_secs(0)).await?;
            Ok(json!({ "workflow_id": workflow_id, "key": key, "value": value }))
        }
        ConductorMessageKind::GetWorkflowStreams => {
            let workflow_id = string_field(&payload, "workflow_id")?;
            let key = string_field(&payload, "key")?;
            let (values, closed): (Vec<Value>, bool) = ctx.read_stream(&workflow_id, &key).await?;
            Ok(json!({ "workflow_id": workflow_id, "key": key, "values": values, "closed": closed }))
        }
        ConductorMessageKind::GetWorkflowAggregates => {
            let aggregates = ctx.get_workflow_aggregates_with_input(options_payload::<GetWorkflowAggregatesInput>(&payload)?).await?;
            Ok(json!({ "aggregates": aggregates }))
        }
        ConductorMessageKind::GetStepAggregates => {
            let mut input = options_payload::<GetStepAggregatesInput>(&payload)?;
            if input.workflow_ids.is_empty()
                && let Some(workflow_id) = string_field_optional(&payload, "workflow_id")
                    .or_else(|| payload.get("options").and_then(|options| string_field_optional(options, "workflow_id")))
            {
                input.workflow_ids.push(workflow_id);
            }
            let aggregates = ctx.get_step_aggregates_with_input(input).await?;
            Ok(json!({ "aggregates": aggregates }))
        }
        ConductorMessageKind::ListApplicationVersions => {
            let versions = ctx.list_application_versions().await?;
            let latest = ctx.get_latest_application_version().await?;
            Ok(json!({ "versions": versions, "latest": latest }))
        }
        ConductorMessageKind::SetLatestApplicationVersion => {
            let version = string_field(&payload, "version")?;
            ctx.set_latest_application_version(&version).await?;
            Ok(json!({ "version": version }))
        }
        ConductorMessageKind::ListQueues => {
            let queues = ctx.list_queues().await?;
            Ok(json!({ "queues": queues }))
        }
        ConductorMessageKind::GetQueue => {
            let queue_name = string_field(&payload, "queue_name")?;
            let queue = ctx.retrieve_queue(&queue_name).await?;
            Ok(json!({ "queue": queue }))
        }
    }
}

pub fn encode_conductor_response(response: &ConductorResponse) -> Result<String> {
    serde_json::to_string(response).map_err(DbosError::from)
}

pub fn decode_conductor_request(input: &str) -> Result<ConductorRequest> {
    serde_json::from_str(input).map_err(DbosError::from)
}

pub fn decode_conductor_wire_request(input: &str) -> Result<ConductorRequest> {
    let raw: Value = serde_json::from_str(input).map_err(DbosError::from)?;
    if raw.get("payload").is_some() || raw.get("kind").is_some() {
        return serde_json::from_value(raw).map_err(DbosError::from);
    }
    let kind = raw.get("type").cloned().ok_or_else(|| DbosError::invalid_argument("missing conductor message type"))?;
    let kind = serde_json::from_value::<ConductorMessageKind>(kind).map_err(DbosError::from)?;
    let request_id = raw.get("request_id").and_then(Value::as_str).map(ToString::to_string);
    let payload = normalize_wire_payload(kind, raw);
    Ok(ConductorRequest { request_id, kind, payload })
}

pub fn encode_conductor_wire_response(response: &ConductorResponse) -> Result<String> {
    let mut object = serde_json::Map::new();
    object.insert("type".to_string(), Value::String(response.kind.wire_name().to_string()));
    if let Some(request_id) = &response.request_id {
        object.insert("request_id".to_string(), Value::String(request_id.clone()));
    }
    if let Some(error) = &response.error {
        object.insert("error_message".to_string(), Value::String(error.message.clone()));
    }
    if response.ok {
        match &response.payload {
            Value::Object(payload) => {
                for (key, value) in payload {
                    object.insert(conductor_wire_payload_key(response.kind, key).to_string(), value.clone());
                }
            }
            Value::Null => {}
            payload => {
                object.insert("output".to_string(), payload.clone());
            }
        }
    }
    serde_json::to_string(&Value::Object(object)).map_err(DbosError::from)
}

fn normalize_wire_payload(kind: ConductorMessageKind, raw: Value) -> Value {
    let Value::Object(mut object) = raw else {
        return raw;
    };
    if let Some(Value::Object(body)) = object.remove("body") {
        for (key, value) in body {
            object.insert(normalize_wire_request_key(kind, &key).to_string(), value);
        }
    }
    Value::Object(
        object
            .into_iter()
            .filter(|(key, _)| key != "type" && key != "request_id")
            .map(|(key, value)| (normalize_wire_request_key(kind, &key).to_string(), value))
            .collect(),
    )
}

fn normalize_wire_request_key(kind: ConductorMessageKind, key: &str) -> &str {
    match (kind, key) {
        (ConductorMessageKind::ForkWorkflow, "workflow_id") => "original_workflow_id",
        (ConductorMessageKind::ForkWorkflow, "new_workflow_id") => "forked_workflow_id",
        _ => key,
    }
}

fn conductor_wire_payload_key(kind: ConductorMessageKind, key: &str) -> &str {
    match (kind, key) {
        (ConductorMessageKind::ListWorkflows | ConductorMessageKind::ListQueuedWorkflows, "workflows") => "output",
        (ConductorMessageKind::ListSteps, "steps") => "output",
        (ConductorMessageKind::GetWorkflow, "workflow") => "output",
        (ConductorMessageKind::GetWorkflowAggregates | ConductorMessageKind::GetStepAggregates, "aggregates") => "output",
        _ => key,
    }
}

impl ConductorMessageKind {
    fn wire_name(self) -> &'static str {
        match self {
            Self::ExecutorInfo => "executor_info",
            Self::Recovery => "recovery",
            Self::ExistPendingWorkflows => "exist_pending_workflows",
            Self::CancelWorkflow => "cancel",
            Self::ResumeWorkflow => "resume",
            Self::ListWorkflows => "list_workflows",
            Self::ListQueuedWorkflows => "list_queued_workflows",
            Self::ListSteps => "list_steps",
            Self::GetWorkflow => "get_workflow",
            Self::ForkWorkflow => "fork_workflow",
            Self::Retention => "retention",
            Self::GetMetrics => "get_metrics",
            Self::ExportWorkflow => "export_workflow",
            Self::ImportWorkflow => "import_workflow",
            Self::DeleteWorkflow => "delete",
            Self::Alert => "alert",
            Self::ListSchedules => "list_schedules",
            Self::GetSchedule => "get_schedule",
            Self::PauseSchedule => "pause_schedule",
            Self::ResumeSchedule => "resume_schedule",
            Self::BackfillSchedule => "backfill_schedule",
            Self::TriggerSchedule => "trigger_schedule",
            Self::GetWorkflowEvents => "get_workflow_events",
            Self::GetWorkflowNotifications => "get_workflow_notifications",
            Self::GetWorkflowStreams => "get_workflow_streams",
            Self::GetWorkflowAggregates => "get_workflow_aggregates",
            Self::GetStepAggregates => "get_step_aggregates",
            Self::ListApplicationVersions => "list_application_versions",
            Self::SetLatestApplicationVersion => "set_latest_application_version",
            Self::ListQueues => "list_queues",
            Self::GetQueue => "get_queue",
        }
    }
}

fn options_payload<T>(payload: &Value) -> Result<T>
where
    T: DeserializeOwned + Default,
{
    if let Some(options) = value_field(payload, "options") {
        return from_value_or_default(options);
    }
    from_value_or_default(payload.clone())
}

fn from_value_or_default<T>(payload: Value) -> Result<T>
where
    T: DeserializeOwned + Default,
{
    if payload.is_null() {
        return Ok(T::default());
    }
    serde_json::from_value(payload).map_err(DbosError::from)
}

fn value_field(payload: &Value, field: &str) -> Option<Value> {
    payload.get(field).cloned()
}

fn string_field(payload: &Value, field: &str) -> Result<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| DbosError::invalid_argument(format!("missing required field {field}")))
}

fn string_field_optional(payload: &Value, field: &str) -> Option<String> {
    payload.get(field).and_then(Value::as_str).filter(|value| !value.is_empty()).map(ToString::to_string)
}

fn string_vec_field(payload: &Value, field: &str) -> Vec<String> {
    payload
        .get(field)
        .and_then(Value::as_array)
        .map(|values| values.iter().filter_map(Value::as_str).map(ToString::to_string).collect())
        .unwrap_or_default()
}

fn string_map_field(payload: &Value, field: &str) -> BTreeMap<String, String> {
    payload
        .get(field)
        .and_then(Value::as_object)
        .map(|values| values.iter().map(|(key, value)| (key.clone(), json_scalar_to_string(value))).collect())
        .unwrap_or_default()
}

fn json_scalar_to_string(value: &Value) -> String {
    value.as_str().map(ToString::to_string).unwrap_or_else(|| value.to_string())
}

fn workflow_ids_from_payload(payload: &Value) -> Result<Vec<String>> {
    let mut workflow_ids = string_vec_field(payload, "workflow_ids");
    if let Some(workflow_id) = string_field_optional(payload, "workflow_id") {
        workflow_ids.push(workflow_id);
    }
    workflow_ids.sort();
    workflow_ids.dedup();
    if workflow_ids.is_empty() {
        return Err(DbosError::invalid_argument("missing required field workflow_id or workflow_ids"));
    }
    Ok(workflow_ids)
}

fn chrono_field(payload: &Value, field: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    let raw = string_field(payload, field)?;
    chrono::DateTime::parse_from_rfc3339(&raw)
        .map(|value| value.with_timezone(&chrono::Utc))
        .map_err(|err| DbosError::invalid_argument(format!("invalid RFC3339 timestamp in {field}: {err}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use tokio::sync::watch;

    use super::*;
    use crate::{DbosConfig, SystemDatabaseHandle, WorkflowRegistrationOptions};

    #[test]
    fn decodes_dbos_wire_body_into_normalized_request() {
        let request = decode_conductor_wire_request(
            r#"{
                "type": "fork_workflow",
                "request_id": "req-1",
                "body": {
                    "workflow_id": "source-workflow",
                    "new_workflow_id": "forked-workflow",
                    "start_step": 2
                }
            }"#,
        )
        .expect("wire request should decode");
        assert_eq!(request.request_id.as_deref(), Some("req-1"));
        assert_eq!(request.kind, ConductorMessageKind::ForkWorkflow);
        assert_eq!(request.payload["original_workflow_id"], "source-workflow");
        assert_eq!(request.payload["forked_workflow_id"], "forked-workflow");
        assert_eq!(request.payload["start_step"], 2);
    }

    #[test]
    fn rejects_conductor_request_without_kind() {
        let error = decode_conductor_request(r#"{"request_id":"req-missing","payload":{}}"#).expect_err("kind is required");
        assert_eq!(error.code, DbosErrorCode::Serialization);
    }

    #[test]
    fn encodes_wire_response_with_dbos_message_type_and_error_message() {
        let response = ConductorResponse {
            request_id: Some("req-2".to_string()),
            kind: ConductorMessageKind::CancelWorkflow,
            ok: false,
            payload: Value::Null,
            error: Some(ConductorErrorPayload {
                code: DbosErrorCode::InvalidArgument,
                message: "missing workflow id".to_string(),
            }),
        };
        let encoded = encode_conductor_wire_response(&response).expect("wire response should encode");
        let value: Value = serde_json::from_str(&encoded).expect("wire response should be valid JSON");
        assert_eq!(value["type"], "cancel");
        assert_eq!(value["request_id"], "req-2");
        assert_eq!(value["error_message"], "missing workflow id");
    }

    #[cfg(feature = "conductor")]
    #[test]
    fn redacted_websocket_url_does_not_expose_api_key() {
        let config = ConductorConfig {
            url: "https://conductor.example".to_string(),
            api_key: "super-secret-key".to_string(),
            app_name: "app name".to_string(),
            executor_metadata: None,
            alert_handler: None,
            transport: None,
            reconnect_interval: Duration::from_secs(1),
        };
        let url = conductor_websocket_url_redacted(&config);
        assert!(url.contains("REDACTED"));
        assert!(!url.contains("super-secret-key"));
    }

    #[tokio::test]
    async fn cancel_handle_tracks_cause() {
        let ctx = DbosContext::new(DbosConfig::new("cancel-cause-test").with_system_database(SystemDatabaseHandle::memory()))
            .await
            .expect("context should initialize");
        let (ctx, handle) = ctx.with_cancel_cause();
        assert!(!ctx.is_cancelled());
        handle.cancel_with_cause("operator requested stop").await;
        assert!(ctx.is_cancelled());
        assert_eq!(ctx.cancel_cause().await.as_deref(), Some("operator requested stop"));
        assert_eq!(handle.cause().await.as_deref(), Some("operator requested stop"));
    }

    #[cfg(feature = "conductor")]
    #[derive(Debug)]
    struct RecordingTransport {
        started: Arc<AtomicBool>,
    }

    #[cfg(feature = "conductor")]
    #[async_trait]
    impl ConductorTransport for RecordingTransport {
        async fn run(&self, _ctx: DbosContext, _config: ConductorConfig, mut shutdown: watch::Receiver<bool>) -> Result<()> {
            self.started.store(true, Ordering::SeqCst);
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
            Ok(())
        }
    }

    #[cfg(feature = "conductor")]
    #[tokio::test]
    async fn launch_starts_configured_conductor_transport() {
        let started = Arc::new(AtomicBool::new(false));
        let transport = Arc::new(RecordingTransport { started: Arc::clone(&started) });
        let ctx = DbosContext::new(
            DbosConfig::new("conductor-launch-test")
                .with_system_database(SystemDatabaseHandle::memory())
                .with_conductor("https://conductor.example", "secret")
                .with_conductor_transport(transport),
        )
        .await
        .expect("context should initialize");
        ctx.launch().await.expect("launch should start conductor transport");
        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("transport should start");
        ctx.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn launch_config_error_does_not_mark_context_launched() {
        let mut config = DbosConfig::new("conductor-config-error-test").with_system_database(SystemDatabaseHandle::memory());
        config.conductor_url = Some("https://conductor.example".to_string());
        let ctx = DbosContext::new(config).await.expect("context should initialize");

        let error = ctx.launch().await.expect_err("launch should reject partial conductor config");
        assert_eq!(error.code, DbosErrorCode::InvalidArgument);
        ctx.register_workflow(
            "after.failed.launch",
            |_ctx, input: i32| async move { Ok(input) },
            WorkflowRegistrationOptions::default(),
        )
        .await
        .expect("context should not be marked launched after failed launch");
    }
}
