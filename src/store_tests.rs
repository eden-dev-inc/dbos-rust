use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::{Barrier, Notify};
use uuid::Uuid;

use super::*;
use crate::error::DbosErrorCode;
use crate::types::WorkflowStatusType;
use crate::{
    DbosConfig, DbosContext, ForkWorkflowInput, JsonSerializer, StepInfo, WorkflowExport, WorkflowOptions, WorkflowRegistrationOptions,
};

const CONCURRENT_CLIENTS: usize = 8;

fn workflow(workflow_id: &str, name: &str, input: Value) -> WorkflowStatus {
    let mut workflow = WorkflowStatus::new(workflow_id, name, "test-version", crate::serialization::DBOS_JSON);
    workflow.input = Some(input);
    workflow
}

fn test_error(message: impl Into<String>) -> DbosError {
    DbosError::database(message)
}

fn test_error_with_source(message: impl Into<String>, source: DbosError) -> DbosError {
    DbosError::with_source(DbosErrorCode::Database, message, source)
}

fn expect_conflict(result: Result<WorkflowInsertResult>, case: &str) -> Result<()> {
    match result {
        Err(error) if error.code == DbosErrorCode::ConflictingWorkflow => Ok(()),
        Err(error) => Err(test_error(format!("{case} returned {error} instead of ConflictingWorkflow"))),
        Ok(result) => Err(test_error(format!("{case} unexpectedly returned {result:?}"))),
    }
}

async fn load_workflow(store: &Arc<dyn SystemDatabase>, workflow_id: &str) -> Result<WorkflowStatus> {
    store.get_workflow(workflow_id).await?.ok_or_else(|| test_error(format!("workflow {workflow_id} was not persisted")))
}

async fn ensure_workflow_unchanged(store: &Arc<dyn SystemDatabase>, workflow_id: &str, expected: &Value) -> Result<()> {
    let actual = serde_json::to_value(load_workflow(store, workflow_id).await?)?;
    if &actual == expected {
        Ok(())
    } else {
        Err(test_error(format!("workflow {workflow_id} changed during an idempotent insert")))
    }
}

async fn exercise_exact_retry_and_mismatches(store: &Arc<dyn SystemDatabase>) -> Result<()> {
    let workflow_id = format!("exact-retry-{}", Uuid::new_v4());
    let input = json!({"request": "original"});
    let initial = workflow(&workflow_id, "example-workflow", input.clone());

    assert_eq!(store.insert_workflow_with_result(initial.clone()).await?, WorkflowInsertResult::Inserted);
    assert_eq!(
        store.insert_workflow_with_result(workflow(&workflow_id, "example-workflow", input.clone())).await?,
        WorkflowInsertResult::ExistingExact
    );

    let mut terminal = initial;
    let completed_at = Utc::now();
    terminal.status = WorkflowStatusType::Success;
    terminal.authenticated_user = Some("durable-user".to_string());
    terminal.authenticated_roles = vec!["durable-role".to_string()];
    terminal.output = Some(json!({"result": "preserved"}));
    terminal.executor_id = Some("original-executor".to_string());
    terminal.updated_at = completed_at;
    terminal.application_id = Some("original-application".to_string());
    terminal.attempts = 3;
    terminal.started_at = Some(completed_at);
    terminal.completed_at = Some(completed_at);
    terminal.priority = Some(42);
    store.save_workflow(terminal.clone()).await?;
    let expected_terminal = serde_json::to_value(&terminal)?;

    assert_eq!(
        store.insert_workflow_with_result(workflow(&workflow_id, "example-workflow", input.clone())).await?,
        WorkflowInsertResult::ExistingExact
    );
    ensure_workflow_unchanged(store, &workflow_id, &expected_terminal).await?;

    expect_conflict(
        store.insert_workflow_with_result(workflow(&workflow_id, "different-workflow", input.clone())).await,
        "workflow name mismatch",
    )?;
    ensure_workflow_unchanged(store, &workflow_id, &expected_terminal).await?;

    expect_conflict(
        store.insert_workflow_with_result(workflow(&workflow_id, "example-workflow", json!({"request": "different"}))).await,
        "workflow input mismatch",
    )?;
    ensure_workflow_unchanged(store, &workflow_id, &expected_terminal).await
}

async fn exercise_concurrent_exact_retry(stores: &[Arc<dyn SystemDatabase>]) -> Result<()> {
    let workflow_id = format!("concurrent-exact-{}", Uuid::new_v4());
    let workflow_name = "concurrent-exact-workflow";
    let input = json!({"request": "same"});
    let barrier = Arc::new(Barrier::new(stores.len() + 1));
    let mut tasks = Vec::with_capacity(stores.len());

    for store in stores {
        let store = Arc::clone(store);
        let barrier = Arc::clone(&barrier);
        let candidate = workflow(&workflow_id, workflow_name, input.clone());
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store.insert_workflow_with_result(candidate).await
        }));
    }
    barrier.wait().await;

    let mut inserted = 0;
    for task in tasks {
        match task.await.map_err(|error| test_error(format!("concurrent exact-retry task failed: {error}")))?? {
            WorkflowInsertResult::Inserted => inserted += 1,
            WorkflowInsertResult::ExistingExact => {}
        }
    }
    if inserted != 1 {
        return Err(test_error(format!("concurrent exact retries created {inserted} workflow rows instead of one")));
    }

    let Some(primary) = stores.first() else {
        return Err(test_error("concurrent exact-retry test requires at least one store"));
    };
    let persisted = load_workflow(primary, &workflow_id).await?;
    if persisted.name != workflow_name || persisted.input.as_ref() != Some(&input) {
        return Err(test_error("concurrent exact retries did not converge on the requested workflow identity"));
    }
    Ok(())
}

async fn exercise_concurrent_immutable_winner(stores: &[Arc<dyn SystemDatabase>]) -> Result<()> {
    let workflow_id = format!("concurrent-mismatch-{}", Uuid::new_v4());
    let workflow_name = "concurrent-mismatch-workflow";
    let barrier = Arc::new(Barrier::new(stores.len() + 1));
    let mut tasks = Vec::with_capacity(stores.len());

    for (index, store) in stores.iter().enumerate() {
        let store = Arc::clone(store);
        let barrier = Arc::clone(&barrier);
        let input = json!({"candidate": index});
        let candidate = workflow(&workflow_id, workflow_name, input.clone());
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            (input, store.insert_workflow_with_result(candidate).await)
        }));
    }
    barrier.wait().await;

    let mut winner_input = None;
    let mut loser_input = None;
    for task in tasks {
        let (input, result) = task.await.map_err(|error| test_error(format!("concurrent mismatch task failed: {error}")))?;
        match result {
            Ok(WorkflowInsertResult::Inserted) if winner_input.is_none() => winner_input = Some(input),
            Ok(WorkflowInsertResult::Inserted) => {
                return Err(test_error("more than one conflicting workflow identity won concurrent insertion"));
            }
            Ok(WorkflowInsertResult::ExistingExact) => {
                return Err(test_error("distinct workflow identities unexpectedly converged during concurrent insertion"));
            }
            Err(error) if error.code == DbosErrorCode::ConflictingWorkflow => {
                if loser_input.is_none() {
                    loser_input = Some(input);
                }
            }
            Err(error) => return Err(error),
        }
    }

    let winner_input = winner_input.ok_or_else(|| test_error("no workflow identity won concurrent insertion"))?;
    let loser_input = loser_input.ok_or_else(|| test_error("no conflicting workflow identity was rejected"))?;
    let Some(primary) = stores.first() else {
        return Err(test_error("concurrent mismatch test requires at least one store"));
    };
    let persisted = load_workflow(primary, &workflow_id).await?;
    if persisted.input.as_ref() != Some(&winner_input) {
        return Err(test_error("the persisted workflow does not match the concurrent insertion winner"));
    }
    let expected = serde_json::to_value(&persisted)?;

    expect_conflict(
        primary.insert_workflow_with_result(workflow(&workflow_id, workflow_name, loser_input)).await,
        "retry of the concurrent mismatch loser",
    )?;
    ensure_workflow_unchanged(primary, &workflow_id, &expected).await
}

async fn exercise_execution_claim_retry(store: &Arc<dyn SystemDatabase>) -> Result<()> {
    let workflow_id = format!("claim-retry-{}", Uuid::new_v4());
    store.insert_workflow(workflow(&workflow_id, "claim-retry-workflow", json!({"request": "same"}))).await?;
    if !store.claim_workflow_execution(&workflow_id, "executor-a", "claim-a").await? {
        return Err(test_error("initial execution claim was not granted"));
    }
    if load_workflow(store, &workflow_id).await?.status != WorkflowStatusType::Pending {
        return Err(test_error("execution claim did not atomically transition the workflow to pending"));
    }
    if !store.claim_workflow_execution(&workflow_id, "executor-a", "claim-a").await? {
        return Err(test_error("retry of the same execution claim was not recognized"));
    }
    if store.claim_workflow_execution(&workflow_id, "executor-a", "claim-b").await? {
        return Err(test_error("a distinct execution claim was granted after the durable winner"));
    }
    Ok(())
}

async fn exercise_store(stores: Vec<Arc<dyn SystemDatabase>>) -> Result<()> {
    let Some(primary) = stores.first() else {
        return Err(test_error("store test requires at least one store"));
    };
    exercise_exact_retry_and_mismatches(primary).await?;
    exercise_concurrent_exact_retry(&stores).await?;
    exercise_concurrent_immutable_winner(&stores).await?;
    exercise_execution_claim_retry(primary).await
}

async fn exercise_concurrent_workflow_execution(stores: Vec<Arc<dyn SystemDatabase>>) -> Result<()> {
    let workflow_id = format!("concurrent-execution-{}", Uuid::new_v4());
    let workflow_name = "concurrent-execution-workflow";
    let execution_count = Arc::new(AtomicUsize::new(0));
    let release_execution = Arc::new(Notify::new());
    let mut contexts = Vec::with_capacity(stores.len());

    for (index, store) in stores.into_iter().enumerate() {
        let ctx = DbosContext::new(
            DbosConfig::new(format!("concurrent-execution-{index}")).with_system_database(SystemDatabaseHandle::from_arc(store)),
        )
        .await?;
        let execution_count = Arc::clone(&execution_count);
        let release_execution = Arc::clone(&release_execution);
        ctx.register_workflow(
            workflow_name,
            move |_ctx, input: i32| {
                let execution_count = Arc::clone(&execution_count);
                let release_execution = Arc::clone(&release_execution);
                async move {
                    execution_count.fetch_add(1, Ordering::SeqCst);
                    release_execution.notified().await;
                    Ok(input + 1)
                }
            },
            WorkflowRegistrationOptions::default(),
        )
        .await?;
        contexts.push(ctx);
    }

    let barrier = Arc::new(Barrier::new(contexts.len() + 1));
    let mut submissions = Vec::with_capacity(contexts.len());
    for ctx in &contexts {
        let ctx = ctx.clone();
        let barrier = Arc::clone(&barrier);
        let workflow_id = workflow_id.clone();
        submissions.push(tokio::spawn(async move {
            barrier.wait().await;
            ctx.run_workflow::<_, i32>(workflow_name, 41, WorkflowOptions { workflow_id: Some(workflow_id), ..Default::default() })
                .await
                .map_err(|error| test_error_with_source("concurrent workflow submission returned an error", error))
        }));
    }
    barrier.wait().await;

    let mut handles = Vec::with_capacity(submissions.len());
    for submission in submissions {
        handles.push(submission.await.map_err(|error| test_error(format!("concurrent workflow submission failed: {error}")))??);
    }

    tokio::time::timeout(Duration::from_secs(2), async {
        while execution_count.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| test_error("the inserted workflow did not begin execution"))?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let observed_executions = execution_count.load(Ordering::SeqCst);
    if observed_executions != 1 {
        release_execution.notify_waiters();
        for ctx in &contexts {
            ctx.shutdown(Duration::from_secs(1)).await;
        }
        return Err(test_error(format!(
            "exact workflow retries started {observed_executions} executions instead of one"
        )));
    }

    release_execution.notify_waiters();
    for handle in handles {
        let result = handle
            .get_result(Some(Duration::from_secs(2)))
            .await
            .map_err(|error| test_error_with_source("concurrent workflow result lookup returned an error", error))?;
        if result != 42 {
            return Err(test_error("exact workflow retry did not receive the stored result"));
        }
    }
    for ctx in &contexts {
        ctx.shutdown(Duration::from_secs(1)).await;
    }
    Ok(())
}

fn stored_workflow_input(input: i32) -> Result<Option<Value>> {
    Ok(JsonSerializer::encode(&input)?.data.map(Value::String))
}

async fn exercise_exact_retry_after_unclaimed_insert(store: Arc<dyn SystemDatabase>) -> Result<()> {
    let workflow_id = format!("unclaimed-exact-retry-{}", Uuid::new_v4());
    let workflow_name = "unclaimed-exact-retry-workflow";
    let executions = Arc::new(AtomicUsize::new(0));
    let ctx =
        DbosContext::new(DbosConfig::new("unclaimed-exact-retry").with_system_database(SystemDatabaseHandle::from_arc(Arc::clone(&store))))
            .await?;
    let handler_executions = Arc::clone(&executions);
    ctx.register_workflow(
        workflow_name,
        move |_ctx, input: i32| {
            let executions = Arc::clone(&handler_executions);
            async move {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok(input + 1)
            }
        },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.launch().await?;

    let mut unclaimed = WorkflowStatus::new(&workflow_id, workflow_name, "test-version", crate::serialization::DBOS_JSON);
    unclaimed.input = stored_workflow_input(41)?;
    store.insert_workflow(unclaimed).await?;

    let handle = ctx
        .run_workflow::<_, i32>(workflow_name, 41, WorkflowOptions { workflow_id: Some(workflow_id), ..Default::default() })
        .await?;
    if handle.get_result(Some(Duration::from_secs(2))).await? != 42 || executions.load(Ordering::SeqCst) != 1 {
        ctx.shutdown(Duration::from_secs(1)).await;
        return Err(test_error("an exact retry did not claim and execute an unclaimed durable workflow exactly once"));
    }
    ctx.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

async fn exercise_recovery_of_unclaimed_workflow(store: Arc<dyn SystemDatabase>) -> Result<()> {
    let workflow_id = format!("unclaimed-recovery-{}", Uuid::new_v4());
    let workflow_name = "unclaimed-recovery-workflow";
    let mut unclaimed = WorkflowStatus::new(&workflow_id, workflow_name, "test-version", crate::serialization::DBOS_JSON);
    unclaimed.input = stored_workflow_input(41)?;
    store.insert_workflow(unclaimed).await?;

    let executions = Arc::new(AtomicUsize::new(0));
    let ctx = DbosContext::new(DbosConfig::new("unclaimed-recovery").with_system_database(SystemDatabaseHandle::from_arc(store))).await?;
    let handler_executions = Arc::clone(&executions);
    ctx.register_workflow(
        workflow_name,
        move |_ctx, input: i32| {
            let executions = Arc::clone(&handler_executions);
            async move {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok(input + 1)
            }
        },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.launch().await?;

    let handle = ctx.retrieve_workflow::<i32>(&workflow_id).await;
    if handle.get_result(Some(Duration::from_secs(2))).await? != 42 || executions.load(Ordering::SeqCst) != 1 {
        ctx.shutdown(Duration::from_secs(1)).await;
        return Err(test_error("launch did not recover an unclaimed durable workflow exactly once"));
    }
    ctx.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

async fn exercise_recovery_of_claimed_enqueued_workflow(store: Arc<dyn SystemDatabase>) -> Result<()> {
    let workflow_id = format!("claimed-enqueued-recovery-{}", Uuid::new_v4());
    let workflow_name = "claimed-enqueued-recovery-workflow";
    let mut workflow = WorkflowStatus::new(&workflow_id, workflow_name, "test-version", crate::serialization::DBOS_JSON);
    workflow.status = WorkflowStatusType::Enqueued;
    workflow.input = stored_workflow_input(41)?;
    store.insert_workflow(workflow).await?;
    if !store.claim_workflow_execution(&workflow_id, "local", "pre-crash-claim").await? {
        return Err(test_error("pre-crash enqueued workflow claim was not granted"));
    }

    let executions = Arc::new(AtomicUsize::new(0));
    let ctx =
        DbosContext::new(DbosConfig::new("claimed-enqueued-recovery").with_system_database(SystemDatabaseHandle::from_arc(store))).await?;
    let handler_executions = Arc::clone(&executions);
    ctx.register_workflow(
        workflow_name,
        move |_ctx, input: i32| {
            let executions = Arc::clone(&handler_executions);
            async move {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok(input + 1)
            }
        },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.launch().await?;

    let handle = ctx.retrieve_workflow::<i32>(&workflow_id).await;
    if handle.get_result(Some(Duration::from_secs(2))).await? != 42 || executions.load(Ordering::SeqCst) != 1 {
        ctx.shutdown(Duration::from_secs(1)).await;
        return Err(test_error("launch did not recover a claimed enqueued workflow"));
    }
    ctx.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

async fn exercise_exact_fork_retry(store: Arc<dyn SystemDatabase>) -> Result<()> {
    let original_workflow_id = format!("fork-source-{}", Uuid::new_v4());
    let forked_workflow_id = format!("forked-retry-{}", Uuid::new_v4());
    let workflow_name = "forked-exact-retry-workflow";
    let executions = Arc::new(AtomicUsize::new(0));
    let ctx = DbosContext::new(DbosConfig::new("forked-exact-retry").with_system_database(SystemDatabaseHandle::from_arc(store))).await?;
    let handler_executions = Arc::clone(&executions);
    ctx.register_workflow(
        workflow_name,
        move |_ctx, input: i32| {
            let executions = Arc::clone(&handler_executions);
            async move {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok(input + 1)
            }
        },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.launch().await?;

    let original = ctx
        .run_workflow::<_, i32>(
            workflow_name,
            41,
            WorkflowOptions {
                workflow_id: Some(original_workflow_id.clone()),
                ..Default::default()
            },
        )
        .await?;
    if original.get_result(Some(Duration::from_secs(2))).await? != 42 {
        ctx.shutdown(Duration::from_secs(1)).await;
        return Err(test_error("fork source workflow did not complete"));
    }

    let fork_input = ForkWorkflowInput {
        original_workflow_id,
        start_step: None,
        forked_workflow_id: Some(forked_workflow_id),
        application_version: None,
    };
    let first = ctx.fork_workflow::<i32>(fork_input.clone()).await?;
    let retry = ctx.fork_workflow::<i32>(fork_input).await?;
    if first.get_result(Some(Duration::from_secs(2))).await? != 42
        || retry.get_result(Some(Duration::from_secs(2))).await? != 42
        || executions.load(Ordering::SeqCst) != 2
    {
        ctx.shutdown(Duration::from_secs(1)).await;
        return Err(test_error("an exact fork retry did not execute the fork exactly once"));
    }
    ctx.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

async fn exercise_import_rejects_existing_workflow(store: Arc<dyn SystemDatabase>) -> Result<()> {
    let workflow_id = format!("import-existing-{}", Uuid::new_v4());
    let ctx = DbosContext::new(DbosConfig::new("import-existing").with_system_database(SystemDatabaseHandle::from_arc(store))).await?;
    let export = WorkflowExport {
        workflow: workflow(&workflow_id, "import-existing-workflow", json!({"request": "original"})),
        steps: vec![StepInfo {
            workflow_uuid: workflow_id.clone(),
            step_id: 0,
            step_name: "original-step".to_string(),
            output: Some(json!({"result": "original"})),
            error: None,
            child_workflow_id: None,
            serialization: crate::serialization::DBOS_JSON.to_string(),
            started_at: Utc::now(),
            completed_at: Utc::now(),
        }],
        events: Vec::new(),
        messages: Vec::new(),
        streams: Vec::new(),
        children: Vec::new(),
    };
    ctx.import_workflow(export.clone()).await?;
    let mut conflicting_export = export;
    conflicting_export.steps[0].output = Some(json!({"result": "changed"}));
    match ctx.import_workflow(conflicting_export).await {
        Err(error) if error.code == DbosErrorCode::ConflictingWorkflow => {}
        Err(error) => return Err(test_error(format!("import collision returned {error} instead of ConflictingWorkflow"))),
        Ok(()) => return Err(test_error("import collision unexpectedly overwrote existing workflow history")),
    }
    let steps = ctx.get_workflow_steps(&workflow_id).await?;
    if steps.len() != 1 || steps[0].output != Some(json!({"result": "original"})) {
        return Err(test_error("import collision changed existing workflow step history"));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_workflow_insert_is_idempotent() -> Result<()> {
    let store = MemoryStore::shared();
    store.migrate().await?;
    exercise_store(vec![Arc::clone(&store); CONCURRENT_CLIENTS]).await?;
    exercise_concurrent_workflow_execution(vec![Arc::clone(&store); CONCURRENT_CLIENTS]).await?;
    exercise_exact_retry_after_unclaimed_insert(Arc::clone(&store)).await?;
    exercise_recovery_of_unclaimed_workflow(Arc::clone(&store)).await?;
    exercise_recovery_of_claimed_enqueued_workflow(Arc::clone(&store)).await?;
    exercise_exact_fork_retry(Arc::clone(&store)).await?;
    exercise_import_rejects_existing_workflow(store).await
}

#[cfg(feature = "turso")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turso_workflow_insert_is_idempotent() -> Result<()> {
    let test_directory = std::env::temp_dir().join(format!("dbos-rust-workflow-insert-{}", Uuid::new_v4()));
    std::fs::create_dir(&test_directory)
        .map_err(|error| test_error(format!("failed to create Turso test directory {}: {error}", test_directory.display())))?;
    let database_path = test_directory.join("state.sqlite");
    let database_path = database_path
        .to_str()
        .ok_or_else(|| test_error(format!("Turso test path is not valid UTF-8: {}", database_path.display())))?;

    let result = async {
        let database = turso::Builder::new_local(database_path).build().await.map_err(DbosError::from)?;
        let mut stores: Vec<Arc<dyn SystemDatabase>> = Vec::with_capacity(CONCURRENT_CLIENTS);
        for _ in 0..CONCURRENT_CLIENTS {
            let connection = database.connect().map_err(DbosError::from)?;
            stores.push(Arc::new(TursoStore { connection: tokio::sync::Mutex::new(connection) }));
        }
        let primary = stores.first().ok_or_else(|| test_error("Turso test requires at least one store"))?;
        primary.migrate().await?;
        exercise_store(stores.clone())
            .await
            .map_err(|error| test_error_with_source("Turso store insertion checks failed", error))?;
        exercise_concurrent_workflow_execution(stores.clone())
            .await
            .map_err(|error| test_error_with_source("Turso concurrent execution check failed", error))?;
        let primary = stores.first().ok_or_else(|| test_error("Turso test requires a primary store"))?;
        exercise_exact_retry_after_unclaimed_insert(Arc::clone(primary))
            .await
            .map_err(|error| test_error_with_source("Turso unclaimed exact-retry check failed", error))?;
        exercise_recovery_of_unclaimed_workflow(Arc::clone(primary))
            .await
            .map_err(|error| test_error_with_source("Turso unclaimed recovery check failed", error))?;
        exercise_recovery_of_claimed_enqueued_workflow(Arc::clone(primary))
            .await
            .map_err(|error| test_error_with_source("Turso claimed enqueued recovery check failed", error))?;
        exercise_exact_fork_retry(Arc::clone(primary))
            .await
            .map_err(|error| test_error_with_source("Turso exact fork retry check failed", error))?;
        exercise_import_rejects_existing_workflow(Arc::clone(primary))
            .await
            .map_err(|error| test_error_with_source("Turso import collision check failed", error))
    }
    .await;
    let cleanup = std::fs::remove_dir_all(&test_directory)
        .map_err(|error| test_error(format!("failed to remove Turso test directory {}: {error}", test_directory.display())));
    result?;
    cleanup
}

#[cfg(feature = "postgres")]
async fn drop_postgres_schema(database_url: &str, schema: &str) -> Result<()> {
    validate_schema_name(schema)?;
    let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls).await.map_err(DbosError::from)?;
    let connection_task = tokio::spawn(connection);
    let statement = format!("DROP SCHEMA IF EXISTS {schema} CASCADE");
    let drop_result = client.batch_execute(&statement).await.map_err(DbosError::from);
    drop(client);
    let connection_result = connection_task
        .await
        .map_err(|error| test_error(format!("postgres cleanup connection task failed: {error}")))?
        .map_err(DbosError::from);
    drop_result?;
    connection_result
}

#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires DBOS_TEST_POSTGRES_URL"]
async fn postgres_workflow_insert_is_idempotent() -> Result<()> {
    let database_url =
        std::env::var("DBOS_TEST_POSTGRES_URL").map_err(|error| test_error(format!("DBOS_TEST_POSTGRES_URL is required: {error}")))?;
    let schema = format!("dbos_workflow_insert_{}", Uuid::new_v4().simple());
    let result = async {
        let primary = PostgresStore::connect(&database_url, &schema).await?;
        primary.migrate().await?;
        let mut stores = vec![primary];
        for _ in 1..CONCURRENT_CLIENTS {
            stores.push(PostgresStore::connect(&database_url, &schema).await?);
        }
        exercise_store(stores.clone())
            .await
            .map_err(|error| test_error_with_source("Postgres store insertion checks failed", error))?;
        exercise_concurrent_workflow_execution(stores.clone())
            .await
            .map_err(|error| test_error_with_source("Postgres concurrent execution check failed", error))?;
        let primary = stores.first().ok_or_else(|| test_error("Postgres test requires a primary store"))?;
        exercise_exact_retry_after_unclaimed_insert(Arc::clone(primary))
            .await
            .map_err(|error| test_error_with_source("Postgres unclaimed exact-retry check failed", error))?;
        exercise_recovery_of_unclaimed_workflow(Arc::clone(primary))
            .await
            .map_err(|error| test_error_with_source("Postgres unclaimed recovery check failed", error))?;
        exercise_recovery_of_claimed_enqueued_workflow(Arc::clone(primary))
            .await
            .map_err(|error| test_error_with_source("Postgres claimed enqueued recovery check failed", error))?;
        exercise_exact_fork_retry(Arc::clone(primary))
            .await
            .map_err(|error| test_error_with_source("Postgres exact fork retry check failed", error))?;
        exercise_import_rejects_existing_workflow(Arc::clone(primary))
            .await
            .map_err(|error| test_error_with_source("Postgres import collision check failed", error))
    }
    .await;
    let cleanup = drop_postgres_schema(&database_url, &schema).await;
    result?;
    cleanup
}
