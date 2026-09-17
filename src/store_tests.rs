use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::{Barrier, Notify};
use uuid::Uuid;

use super::*;
use crate::error::DbosErrorCode;
use crate::types::WorkflowStatusType;
use crate::{DbosConfig, DbosContext, WorkflowOptions, WorkflowRegistrationOptions};

const CONCURRENT_CLIENTS: usize = 8;

fn workflow(workflow_id: &str, name: &str, input: Value) -> WorkflowStatus {
    let mut workflow = WorkflowStatus::new(workflow_id, name, "test-version", crate::serialization::DBOS_JSON);
    workflow.input = Some(input);
    workflow
}

fn test_error(message: impl Into<String>) -> DbosError {
    DbosError::database(message)
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

    assert_eq!(store.insert_workflow(initial.clone()).await?, WorkflowInsertResult::Inserted);
    assert_eq!(
        store.insert_workflow(workflow(&workflow_id, "example-workflow", input.clone())).await?,
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
        store.insert_workflow(workflow(&workflow_id, "example-workflow", input.clone())).await?,
        WorkflowInsertResult::ExistingExact
    );
    ensure_workflow_unchanged(store, &workflow_id, &expected_terminal).await?;

    expect_conflict(
        store.insert_workflow(workflow(&workflow_id, "different-workflow", input.clone())).await,
        "workflow name mismatch",
    )?;
    ensure_workflow_unchanged(store, &workflow_id, &expected_terminal).await?;

    expect_conflict(
        store.insert_workflow(workflow(&workflow_id, "example-workflow", json!({"request": "different"}))).await,
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
            store.insert_workflow(candidate).await
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
            (input, store.insert_workflow(candidate).await)
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
        primary.insert_workflow(workflow(&workflow_id, workflow_name, loser_input)).await,
        "retry of the concurrent mismatch loser",
    )?;
    ensure_workflow_unchanged(primary, &workflow_id, &expected).await
}

async fn exercise_store(stores: Vec<Arc<dyn SystemDatabase>>) -> Result<()> {
    let Some(primary) = stores.first() else {
        return Err(test_error("store test requires at least one store"));
    };
    exercise_exact_retry_and_mismatches(primary).await?;
    exercise_concurrent_exact_retry(&stores).await?;
    exercise_concurrent_immutable_winner(&stores).await
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
        ctx.launch().await?;
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
        if handle.get_result(Some(Duration::from_secs(2))).await? != 42 {
            return Err(test_error("exact workflow retry did not receive the stored result"));
        }
    }
    for ctx in &contexts {
        ctx.shutdown(Duration::from_secs(1)).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_workflow_insert_is_idempotent() -> Result<()> {
    let store = MemoryStore::shared();
    store.migrate().await?;
    exercise_store(vec![Arc::clone(&store); CONCURRENT_CLIENTS]).await?;
    exercise_concurrent_workflow_execution(vec![store; CONCURRENT_CLIENTS]).await
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
        exercise_store(stores.clone()).await?;
        exercise_concurrent_workflow_execution(stores).await
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
        exercise_store(stores.clone()).await?;
        exercise_concurrent_workflow_execution(stores).await
    }
    .await;
    let cleanup = drop_postgres_schema(&database_url, &schema).await;
    result?;
    cleanup
}
