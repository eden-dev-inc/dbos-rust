use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::client::DbosClient;
use crate::context::{DbosContext, WorkflowHandle};
use crate::error::Result;
use crate::types::{EnqueueOptions, WorkflowOptions};

#[derive(Debug, Clone)]
pub struct Debouncer {
    workflow_name: String,
    timeout: Option<Duration>,
    queue_name: String,
}

impl Debouncer {
    pub fn new(workflow_name: impl Into<String>) -> Self {
        Self {
            workflow_name: workflow_name.into(),
            timeout: None,
            queue_name: "_dbos_internal_queue".to_string(),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_queue(mut self, queue_name: impl Into<String>) -> Self {
        self.queue_name = queue_name.into();
        self
    }

    pub async fn debounce<I, O>(
        &self,
        ctx: &DbosContext,
        key: impl Into<String>,
        delay: Duration,
        input: I,
        mut options: WorkflowOptions,
    ) -> Result<WorkflowHandle<O>>
    where
        I: Serialize + Send,
        O: DeserializeOwned + Send + 'static,
    {
        let key = key.into();
        options.workflow_id.get_or_insert_with(|| Uuid::new_v4().to_string());
        options.queue_name = Some(self.queue_name.clone());
        options.deduplication_id = Some(key);
        options.delay = Some(delay);
        if options.timeout.is_none() {
            options.timeout = self.timeout;
        }
        ctx.run_workflow(self.workflow_name.clone(), input, options).await
    }
}

#[derive(Clone)]
pub struct DebouncerClient {
    workflow_name: String,
    client: DbosClient,
    timeout: Option<Duration>,
    queue_name: String,
}

impl DebouncerClient {
    pub fn new(workflow_name: impl Into<String>, client: DbosClient) -> Self {
        Self {
            workflow_name: workflow_name.into(),
            client,
            timeout: None,
            queue_name: "_dbos_internal_queue".to_string(),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_queue(mut self, queue_name: impl Into<String>) -> Self {
        self.queue_name = queue_name.into();
        self
    }

    pub async fn debounce<I, O>(
        &self,
        key: impl Into<String>,
        delay: Duration,
        input: I,
        mut options: EnqueueOptions,
    ) -> Result<WorkflowHandle<O>>
    where
        I: Serialize + Send,
        O: DeserializeOwned + Send + 'static,
    {
        options.workflow_id.get_or_insert_with(|| Uuid::new_v4().to_string());
        options.deduplication_id = Some(key.into());
        options.delay = Some(delay);
        if options.timeout.is_none() {
            options.timeout = self.timeout;
        }
        self.client.enqueue(self.queue_name.clone(), self.workflow_name.clone(), input, options).await
    }
}
