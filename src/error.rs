use thiserror::Error;

/// Structured DBOS error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(i32)]
pub enum DbosErrorCode {
    ConflictingId = 1,
    Initialization = 2,
    NonExistentWorkflow = 3,
    ConflictingWorkflow = 4,
    WorkflowCancelled = 5,
    UnexpectedStep = 6,
    AwaitedWorkflowCancelled = 7,
    ConflictingRegistration = 8,
    WorkflowUnexpectedType = 9,
    WorkflowExecution = 10,
    StepExecution = 11,
    DeadLetterQueue = 12,
    MaxStepRetriesExceeded = 13,
    QueueDeduplicated = 14,
    PatchingNotEnabled = 15,
    Timeout = 16,
    NoApplicationVersions = 17,
    Database = 18,
    Serialization = 19,
    InvalidArgument = 20,
    Unsupported = 21,
}

/// Unified error type for DBOS operations.
#[derive(Debug, Error)]
#[error("DBOS error {code:?}: {message}")]
pub struct DbosError {
    pub code: DbosErrorCode,
    pub message: String,
    pub workflow_id: Option<String>,
    pub destination_id: Option<String>,
    pub step_name: Option<String>,
    pub queue_name: Option<String>,
    pub deduplication_id: Option<String>,
    pub step_id: Option<i32>,
    pub expected_name: Option<String>,
    pub recorded_name: Option<String>,
    pub max_retries: Option<u32>,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl DbosError {
    pub fn new(code: DbosErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            workflow_id: None,
            destination_id: None,
            step_name: None,
            queue_name: None,
            deduplication_id: None,
            step_id: None,
            expected_name: None,
            recorded_name: None,
            max_retries: None,
            source: None,
        }
    }

    pub fn with_source(code: DbosErrorCode, message: impl Into<String>, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        let mut err = Self::new(code, message);
        err.source = Some(Box::new(source));
        err
    }

    pub fn initialization(message: impl Into<String>) -> Self {
        Self::new(DbosErrorCode::Initialization, message)
    }

    pub fn database(message: impl Into<String>) -> Self {
        Self::new(DbosErrorCode::Database, message)
    }

    pub fn serialization(message: impl Into<String>) -> Self {
        Self::new(DbosErrorCode::Serialization, message)
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(DbosErrorCode::InvalidArgument, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(DbosErrorCode::Unsupported, message)
    }

    pub fn workflow_execution(workflow_id: impl Into<String>, message: impl Into<String>) -> Self {
        let workflow_id = workflow_id.into();
        let mut err = Self::new(
            DbosErrorCode::WorkflowExecution,
            format!("workflow {workflow_id} execution error: {}", message.into()),
        );
        err.workflow_id = Some(workflow_id);
        err
    }

    pub fn step_execution(workflow_id: impl Into<String>, step_name: impl Into<String>, message: impl Into<String>) -> Self {
        let workflow_id = workflow_id.into();
        let step_name = step_name.into();
        let mut err = Self::new(
            DbosErrorCode::StepExecution,
            format!("step {step_name} in workflow {workflow_id} execution error: {}", message.into()),
        );
        err.workflow_id = Some(workflow_id);
        err.step_name = Some(step_name);
        err
    }

    pub fn unexpected_step(workflow_id: impl Into<String>, step_id: i32, expected: impl Into<String>, recorded: impl Into<String>) -> Self {
        let workflow_id = workflow_id.into();
        let expected = expected.into();
        let recorded = recorded.into();
        let mut err = Self::new(
            DbosErrorCode::UnexpectedStep,
            format!(
                "during execution of workflow {workflow_id} step {step_id}, function {recorded} was recorded when {expected} was expected; workflow code must be deterministic"
            ),
        );
        err.workflow_id = Some(workflow_id);
        err.step_id = Some(step_id);
        err.expected_name = Some(expected);
        err.recorded_name = Some(recorded);
        err
    }

    pub fn non_existent_workflow(workflow_id: impl Into<String>) -> Self {
        let workflow_id = workflow_id.into();
        let mut err = Self::new(DbosErrorCode::NonExistentWorkflow, format!("workflow {workflow_id} does not exist"));
        err.workflow_id = Some(workflow_id.clone());
        err.destination_id = Some(workflow_id);
        err
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(DbosErrorCode::Timeout, message)
    }
}

impl From<serde_json::Error> for DbosError {
    fn from(value: serde_json::Error) -> Self {
        Self::with_source(DbosErrorCode::Serialization, "serialization failed", value)
    }
}

#[cfg(feature = "postgres")]
impl From<tokio_postgres::Error> for DbosError {
    fn from(value: tokio_postgres::Error) -> Self {
        Self::with_source(DbosErrorCode::Database, "postgres operation failed", value)
    }
}

#[cfg(feature = "turso")]
impl From<turso::Error> for DbosError {
    fn from(value: turso::Error) -> Self {
        Self::with_source(DbosErrorCode::Database, "turso operation failed", value)
    }
}

pub type Result<T> = std::result::Result<T, DbosError>;
