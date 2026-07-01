use std::time::Duration;

use crate::context::DbosContext;
use crate::error::Result;
#[cfg(feature = "admin")]
use crate::observability::{log_admin_warning, log_database_warning};

#[cfg(feature = "admin")]
use std::collections::BTreeMap;

#[cfg(feature = "admin")]
use serde::Serialize;
#[cfg(feature = "admin")]
use serde_json::{Value, json};
#[cfg(feature = "admin")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "admin")]
use tokio::net::{TcpListener, TcpStream};
#[cfg(feature = "admin")]
use tokio::sync::oneshot;
#[cfg(feature = "admin")]
use tokio::task::JoinHandle;

#[cfg(feature = "admin")]
use crate::error::{DbosError, DbosErrorCode};
#[cfg(feature = "admin")]
use crate::types::{
    DeleteWorkflowOptions, ListSchedulesOptions, ListWorkflowsOptions, ResumeWorkflowOptions, ScheduleStatus, WorkflowStatusType,
};

#[derive(Debug, Clone)]
pub struct AdminServerConfig {
    pub host: String,
    pub port: u16,
}

impl Default for AdminServerConfig {
    fn default() -> Self {
        Self { host: "127.0.0.1".to_string(), port: 3001 }
    }
}

pub struct AdminServerHandle {
    port: u16,
    #[cfg(feature = "admin")]
    shutdown: Option<oneshot::Sender<()>>,
    #[cfg(feature = "admin")]
    task: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for AdminServerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("AdminServerHandle").field("port", &self.port).finish()
    }
}

impl AdminServerHandle {
    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn shutdown(self, timeout: Duration) -> Result<()> {
        #[cfg(feature = "admin")]
        {
            let mut handle = self;
            if let Some(shutdown) = handle.shutdown.take() {
                let _ = shutdown.send(());
            }
            if let Some(task) = handle.task.take() {
                let _ = tokio::time::timeout(timeout, task).await;
            }
        }
        #[cfg(not(feature = "admin"))]
        {
            let _ = self;
            let _ = timeout;
        }
        Ok(())
    }
}

pub async fn start_admin_server(ctx: DbosContext, config: AdminServerConfig) -> Result<AdminServerHandle> {
    #[cfg(feature = "admin")]
    {
        let bind_addr = format!("{}:{}", config.host, config.port);
        let listener = TcpListener::bind(&bind_addr).await.map_err(|err| {
            DbosError::with_source(DbosErrorCode::Initialization, format!("failed to bind DBOS admin server at {bind_addr}"), err)
        })?;
        let port = listener
            .local_addr()
            .map_err(|err| DbosError::with_source(DbosErrorCode::Initialization, "failed to read DBOS admin server address", err))?
            .port();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((stream, _peer)) => {
                                let ctx = ctx.clone();
                                tokio::spawn(async move {
                                    if let Err(error) = handle_admin_connection(ctx, stream).await {
                                        log_admin_warning("DBOS admin request failed", &error);
                                    }
                                });
                            }
                            Err(error) => {
                                log_database_warning("DBOS admin accept failed", &error);
                                break;
                            }
                        }
                    }
                }
            }
        });
        Ok(AdminServerHandle { port, shutdown: Some(shutdown_tx), task: Some(task) })
    }
    #[cfg(not(feature = "admin"))]
    {
        let _ = ctx;
        let _ = config;
        Err(crate::error::DbosError::unsupported("admin server requires the admin feature"))
    }
}

#[cfg(feature = "admin")]
async fn handle_admin_connection(ctx: DbosContext, mut stream: TcpStream) -> Result<()> {
    let mut buffer = vec![0_u8; 16 * 1024];
    let bytes_read = stream
        .read(&mut buffer)
        .await
        .map_err(|err| DbosError::with_source(DbosErrorCode::Database, "failed to read admin request", err))?;
    if bytes_read == 0 {
        return Ok(());
    }
    let request = String::from_utf8_lossy(&buffer[..bytes_read]);
    let Some(request_line) = request.lines().next() else {
        return write_json_response(&mut stream, 400, &json!({"error": "empty request"})).await;
    };
    let mut parts = request_line.split_whitespace();
    let Some(method) = parts.next() else {
        return write_json_response(&mut stream, 400, &json!({"error": "missing method"})).await;
    };
    let Some(target) = parts.next() else {
        return write_json_response(&mut stream, 400, &json!({"error": "missing target"})).await;
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    match route_admin_request(ctx, method, path, query).await {
        Ok((status, body)) => write_json_response(&mut stream, status, &body).await,
        Err(error) => {
            let status = status_for_error(&error);
            write_json_response(
                &mut stream,
                status,
                &json!({
                    "error": {
                        "code": error.code,
                        "message": error.message,
                        "workflow_id": error.workflow_id,
                        "queue_name": error.queue_name,
                        "deduplication_id": error.deduplication_id,
                    }
                }),
            )
            .await
        }
    }
}

#[cfg(feature = "admin")]
async fn route_admin_request(ctx: DbosContext, method: &str, path: &str, query: &str) -> Result<(u16, Value)> {
    let segments = path.trim_start_matches('/').split('/').filter(|segment| !segment.is_empty()).collect::<Vec<_>>();
    let query = parse_query(query);
    match (method, segments.as_slice()) {
        ("GET", []) | ("GET", ["healthz"]) | ("GET", ["dbos-healthz"]) => Ok((200, json!({"status": "ok"}))),
        ("GET", ["workflows"]) => {
            let workflows = ctx.list_workflows(list_workflows_options(&query)).await?;
            Ok((200, json!({ "workflows": workflows })))
        }
        ("GET", ["workflows", workflow_id]) => {
            let status = ctx.retrieve_workflow::<Value>(*workflow_id).await.get_status().await?;
            Ok((200, json!({ "workflow": status })))
        }
        ("GET", ["workflows", workflow_id, "steps"]) => {
            let steps = ctx.get_workflow_steps(workflow_id).await?;
            Ok((200, json!({ "steps": steps })))
        }
        ("POST", ["workflows", workflow_id, "cancel"]) => {
            ctx.cancel_workflow(workflow_id).await?;
            Ok((202, json!({ "workflow_id": workflow_id, "status": "cancelled" })))
        }
        ("POST", ["workflows", workflow_id, "resume"]) => {
            let handle = ctx.resume_workflow::<Value>(workflow_id, ResumeWorkflowOptions::default()).await?;
            Ok((202, json!({ "workflow_id": handle.workflow_id() })))
        }
        ("DELETE", ["workflows", workflow_id]) => {
            ctx.delete_workflows(&[(*workflow_id).to_string()], DeleteWorkflowOptions { force: query_bool(&query, "force") })
                .await?;
            Ok((202, json!({ "workflow_id": workflow_id, "deleted": true })))
        }
        ("GET", ["queues"]) => {
            let queues = ctx.list_queues().await?;
            Ok((200, json!({ "queues": queues })))
        }
        ("GET", ["queues", queue_name]) => {
            let queue = ctx.retrieve_queue(queue_name).await?;
            Ok((200, json!({ "queue": queue })))
        }
        ("DELETE", ["queues", queue_name]) => {
            ctx.delete_queue(queue_name).await?;
            Ok((202, json!({ "queue": queue_name, "deleted": true })))
        }
        ("GET", ["schedules"]) => {
            let schedules = ctx.list_schedules(list_schedules_options(&query)).await?;
            Ok((200, json!({ "schedules": schedules })))
        }
        ("GET", ["schedules", schedule_name]) => {
            let schedule = ctx.get_schedule(schedule_name).await?;
            Ok((200, json!({ "schedule": schedule })))
        }
        ("DELETE", ["schedules", schedule_name]) => {
            ctx.delete_schedule(schedule_name).await?;
            Ok((202, json!({ "schedule": schedule_name, "deleted": true })))
        }
        ("POST", ["schedules", schedule_name, "pause"]) => {
            ctx.pause_schedule(schedule_name).await?;
            Ok((202, json!({ "schedule": schedule_name, "status": "paused" })))
        }
        ("POST", ["schedules", schedule_name, "resume"]) => {
            ctx.resume_schedule(schedule_name).await?;
            Ok((202, json!({ "schedule": schedule_name, "status": "active" })))
        }
        ("POST", ["schedules", schedule_name, "trigger"]) => {
            let handle = ctx.trigger_schedule(schedule_name).await?;
            Ok((202, json!({ "workflow_id": handle.workflow_id() })))
        }
        ("GET", ["app-versions"]) => {
            let versions = ctx.list_application_versions().await?;
            let latest = ctx.get_latest_application_version().await?;
            Ok((200, json!({ "versions": versions, "latest": latest })))
        }
        ("GET", ["metrics"]) => {
            let workflow_id = query.get("workflow_id").map(String::as_str);
            let workflow_aggregates = ctx.get_workflow_aggregates(list_workflows_options(&query)).await?;
            let step_aggregates = if let Some(workflow_id) = workflow_id {
                ctx.get_step_aggregates(workflow_id).await?
            } else {
                Vec::new()
            };
            let mut body = json!({
                "workflow_aggregates": workflow_aggregates,
                "step_aggregates": step_aggregates,
            });
            if ctx.observability().is_enabled() {
                body["telemetry"] = serde_json::to_value(ctx.observability().snapshot())?;
                body["prometheus"] = Value::String(ctx.observability().export_prometheus());
            }
            Ok((200, body))
        }
        _ => Err(DbosError::unsupported(format!("unsupported admin endpoint {method} {path}"))),
    }
}

#[cfg(feature = "admin")]
async fn write_json_response<T: Serialize>(stream: &mut TcpStream, status: u16, body: &T) -> Result<()> {
    let body = serde_json::to_vec(body)?;
    let status_text = match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        504 => "Gateway Timeout",
        _ => "OK",
    };
    let header = format!(
        "HTTP/1.1 {status} {status_text}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(header.as_bytes())
        .await
        .map_err(|err| DbosError::with_source(DbosErrorCode::Database, "failed to write admin response", err))?;
    stream
        .write_all(&body)
        .await
        .map_err(|err| DbosError::with_source(DbosErrorCode::Database, "failed to write admin response", err))?;
    Ok(())
}

#[cfg(feature = "admin")]
fn status_for_error(error: &DbosError) -> u16 {
    match error.code {
        DbosErrorCode::InvalidArgument | DbosErrorCode::ConflictingWorkflow => 400,
        DbosErrorCode::NonExistentWorkflow => 404,
        DbosErrorCode::Timeout => 504,
        DbosErrorCode::Unsupported => 501,
        _ => 500,
    }
}

#[cfg(feature = "admin")]
fn parse_query(query: &str) -> BTreeMap<String, String> {
    query
        .split('&')
        .filter(|part| !part.is_empty())
        .filter_map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            if key.is_empty() {
                None
            } else {
                Some((key.to_string(), value.to_string()))
            }
        })
        .collect()
}

#[cfg(feature = "admin")]
fn query_bool(query: &BTreeMap<String, String>, key: &str) -> bool {
    query.get(key).is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes"))
}

#[cfg(feature = "admin")]
fn list_workflows_options(query: &BTreeMap<String, String>) -> ListWorkflowsOptions {
    let mut options = ListWorkflowsOptions {
        workflow_name: query.get("workflow_name").cloned(),
        workflow_id_prefix: query.get("workflow_id_prefix").cloned(),
        queue_name: query.get("queue_name").cloned(),
        deduplication_id: query.get("deduplication_id").cloned(),
        authenticated_user: query.get("authenticated_user").cloned(),
        application_version: query.get("application_version").cloned(),
        queues_only: query_bool(query, "queues_only"),
        load_input: query_bool(query, "load_input"),
        load_output: query_bool(query, "load_output"),
        sort_desc: query_bool(query, "sort_desc"),
        limit: query.get("limit").and_then(|value| value.parse::<usize>().ok()),
        offset: query.get("offset").and_then(|value| value.parse::<usize>().ok()),
        ..Default::default()
    };
    if let Some(workflow_ids) = query.get("workflow_ids") {
        options.workflow_ids = workflow_ids.split(',').filter(|value| !value.is_empty()).map(ToString::to_string).collect();
    }
    if let Some(statuses) = query.get("status") {
        options.status = statuses.split(',').filter_map(parse_workflow_status).collect();
    }
    if let Some(names) = query.get("workflow_names") {
        options.workflow_names = split_csv(names);
    }
    if let Some(prefixes) = query.get("workflow_id_prefixes") {
        options.workflow_id_prefixes = split_csv(prefixes);
    }
    if let Some(queue_names) = query.get("queue_names") {
        options.queue_names = split_csv(queue_names);
    }
    if let Some(users) = query.get("authenticated_users") {
        options.authenticated_users = split_csv(users);
    }
    if let Some(versions) = query.get("application_versions") {
        options.application_versions = split_csv(versions);
    }
    if let Some(deduplication_ids) = query.get("deduplication_ids") {
        options.deduplication_ids = split_csv(deduplication_ids);
    }
    if let Some(executor_ids) = query.get("executor_ids") {
        options.executor_ids = split_csv(executor_ids);
    }
    if let Some(forked_from) = query.get("forked_from") {
        options.forked_from = split_csv(forked_from);
    }
    if let Some(parent_workflow_ids) = query.get("parent_workflow_ids") {
        options.parent_workflow_ids = split_csv(parent_workflow_ids);
    }
    options.completed_after = query_time(query, "completed_after");
    options.completed_before = query_time(query, "completed_before");
    options.dequeued_after = query_time(query, "dequeued_after");
    options.dequeued_before = query_time(query, "dequeued_before");
    options.was_forked_from = query.get("was_forked_from").map(|_| query_bool(query, "was_forked_from"));
    options.has_parent = query.get("has_parent").map(|_| query_bool(query, "has_parent"));
    options
}

#[cfg(feature = "admin")]
fn split_csv(value: &str) -> Vec<String> {
    value.split(',').filter(|value| !value.is_empty()).map(ToString::to_string).collect()
}

#[cfg(feature = "admin")]
fn query_time(query: &BTreeMap<String, String>, key: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    query
        .get(key)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&chrono::Utc))
}

#[cfg(feature = "admin")]
fn list_schedules_options(query: &BTreeMap<String, String>) -> ListSchedulesOptions {
    let mut options = ListSchedulesOptions::default();
    if let Some(statuses) = query.get("status") {
        options.statuses = statuses.split(',').filter_map(parse_schedule_status).collect();
    }
    if let Some(workflow_names) = query.get("workflow_names") {
        options.workflow_names = workflow_names.split(',').filter(|value| !value.is_empty()).map(ToString::to_string).collect();
    }
    if let Some(prefixes) = query.get("schedule_name_prefixes") {
        options.schedule_name_prefixes = prefixes.split(',').filter(|value| !value.is_empty()).map(ToString::to_string).collect();
    }
    options
}

#[cfg(feature = "admin")]
fn parse_workflow_status(raw: &str) -> Option<WorkflowStatusType> {
    match raw.to_ascii_uppercase().as_str() {
        "PENDING" => Some(WorkflowStatusType::Pending),
        "ENQUEUED" => Some(WorkflowStatusType::Enqueued),
        "DELAYED" => Some(WorkflowStatusType::Delayed),
        "SUCCESS" => Some(WorkflowStatusType::Success),
        "ERROR" => Some(WorkflowStatusType::Error),
        "CANCELLED" => Some(WorkflowStatusType::Cancelled),
        "MAX_RECOVERY_ATTEMPTS_EXCEEDED" => Some(WorkflowStatusType::MaxRecoveryAttemptsExceeded),
        _ => None,
    }
}

#[cfg(feature = "admin")]
fn parse_schedule_status(raw: &str) -> Option<ScheduleStatus> {
    match raw.to_ascii_uppercase().as_str() {
        "ACTIVE" => Some(ScheduleStatus::Active),
        "PAUSED" => Some(ScheduleStatus::Paused),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminEndpoint {
    Health,
    WorkflowRecovery,
    Deactivate,
    WorkflowQueuesMetadata,
    GarbageCollect,
    GlobalTimeout,
    QueuedWorkflows,
    Workflows,
    Workflow,
    WorkflowSteps,
    WorkflowCancel,
    WorkflowResume,
    WorkflowFork,
    Conductor,
}

impl AdminEndpoint {
    pub fn path(self) -> &'static str {
        match self {
            Self::Health => "/healthz",
            Self::WorkflowRecovery => "/workflows/recovery",
            Self::Deactivate => "/deactivate",
            Self::WorkflowQueuesMetadata => "/queues",
            Self::GarbageCollect => "/gc",
            Self::GlobalTimeout => "/timeout",
            Self::QueuedWorkflows => "/workflows?queues_only=true",
            Self::Workflows => "/workflows",
            Self::Workflow => "/workflows/{workflow_id}",
            Self::WorkflowSteps => "/workflows/{workflow_id}/steps",
            Self::WorkflowCancel => "/workflows/{workflow_id}/cancel",
            Self::WorkflowResume => "/workflows/{workflow_id}/resume",
            Self::WorkflowFork => "/workflows/{workflow_id}/fork",
            Self::Conductor => "/conductor",
        }
    }
}
