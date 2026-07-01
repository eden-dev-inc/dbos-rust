#![allow(clippy::result_large_err)]

use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::Utc;
use dbos::{
    CreateScheduleRequest, DbosConfig, DbosContext, DbosError, ListWorkflowsOptions, ScheduledWorkflowInput, WorkflowOptions,
    WorkflowQueue, WorkflowRegistrationOptions, WorkflowStatusType,
};
use tokio::sync::Notify;
use uuid::Uuid;

const INTERNAL_QUEUE: &str = "_dbos_internal_queue";

#[derive(Clone)]
struct ChaosConfig {
    database_url: String,
    start_cmd: Option<String>,
    stop_cmd: Option<String>,
    no_restarts: bool,
    down_max: Duration,
    up_min: Duration,
    up_max: Duration,
}

impl ChaosConfig {
    fn from_env() -> dbos::Result<Self> {
        let database_url = std::env::var("DBOS_CHAOS_DATABASE_URL")
            .or_else(|_| std::env::var("DBOS_SYSTEM_DATABASE_URL"))
            .map_err(|_| DbosError::invalid_argument("set DBOS_CHAOS_DATABASE_URL to run dbos-rust chaos tests"))?;
        let start_cmd = std::env::var("DBOS_CHAOS_POSTGRES_START_CMD").ok();
        let stop_cmd = std::env::var("DBOS_CHAOS_POSTGRES_STOP_CMD").ok();
        let no_restarts = env_bool("DBOS_CHAOS_NO_RESTARTS");
        if !no_restarts && (start_cmd.is_none() || stop_cmd.is_none()) {
            return Err(DbosError::invalid_argument(
                "set DBOS_CHAOS_POSTGRES_START_CMD and DBOS_CHAOS_POSTGRES_STOP_CMD, or DBOS_CHAOS_NO_RESTARTS=1",
            ));
        }
        Ok(Self {
            database_url,
            start_cmd,
            stop_cmd,
            no_restarts,
            down_max: Duration::from_millis(env_u64("DBOS_CHAOS_DOWN_MAX_MS", 2_000)),
            up_min: Duration::from_millis(env_u64("DBOS_CHAOS_UP_MIN_MS", 5_000)),
            up_max: Duration::from_millis(env_u64("DBOS_CHAOS_UP_MAX_MS", 40_000)),
        })
    }

    async fn ensure_postgres_up(&self) -> dbos::Result<()> {
        if let Some(command) = &self.start_cmd {
            retry_shell(command, "start postgres", Duration::from_secs(60)).await?;
        }
        Ok(())
    }
}

struct PostgresChaos {
    stop: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<dbos::Result<()>>,
    config: ChaosConfig,
}

impl PostgresChaos {
    async fn start(config: ChaosConfig) -> dbos::Result<Self> {
        config.ensure_postgres_up().await?;
        let stop = Arc::new(AtomicBool::new(false));
        let task_stop = Arc::clone(&stop);
        let task_config = config.clone();
        let handle = tokio::spawn(async move {
            if task_config.no_restarts {
                while !task_stop.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                return Ok(());
            }
            loop {
                if task_stop.load(Ordering::SeqCst) {
                    task_config.ensure_postgres_up().await?;
                    return Ok(());
                }
                if let Some(command) = &task_config.stop_cmd {
                    retry_shell(command, "stop postgres", Duration::from_secs(60)).await?;
                }
                sleep_or_stop(random_duration(Duration::ZERO, task_config.down_max), &task_stop).await;
                task_config.ensure_postgres_up().await?;
                sleep_or_stop(random_duration(task_config.up_min, task_config.up_max), &task_stop).await;
            }
        });
        Ok(Self { stop, handle, config })
    }

    async fn shutdown(self) -> dbos::Result<()> {
        self.stop.store(true, Ordering::SeqCst);
        let join_result = self.handle.await.map_err(|err| DbosError::database(format!("postgres chaos task failed to join: {err}")))?;
        self.config.ensure_postgres_up().await?;
        join_result
    }
}

#[derive(Default)]
struct Event {
    is_set: AtomicBool,
    notify: Notify,
}

impl Event {
    async fn wait(&self) {
        loop {
            if self.is_set.load(Ordering::SeqCst) {
                return;
            }
            let notified = self.notify.notified();
            if self.is_set.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    fn set(&self) {
        self.is_set.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

async fn setup_dbos(test_name: &str, config: &ChaosConfig) -> dbos::Result<DbosContext> {
    let mut dbos_config = DbosConfig::new("chaos-test");
    dbos_config.database_url = Some(config.database_url.clone());
    dbos_config.database_schema = chaos_schema(test_name)?;
    dbos_config.scheduler_polling_interval = Duration::from_millis(200);
    DbosContext::new(dbos_config).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires DBOS_CHAOS_DATABASE_URL and DBOS_CHAOS_POSTGRES_*_CMD"]
async fn chaos_workflow_steps_and_schedules() -> dbos::Result<()> {
    let config = ChaosConfig::from_env()?;
    let chaos = PostgresChaos::start(config.clone()).await?;
    let ctx = setup_dbos("workflow", &config).await?;
    let mut internal_queue = WorkflowQueue::new(INTERNAL_QUEUE);
    internal_queue.polling_interval = Duration::from_millis(100);
    ctx.register_queue(internal_queue).await?;
    ctx.listen_queues(&[WorkflowQueue::new(INTERNAL_QUEUE)]).await;

    ctx.register_workflow(
        "scheduled-chaos-test",
        |_ctx, _input: ScheduledWorkflowInput| async move { Ok(()) },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.register_workflow(
        "chaos-step-workflow",
        |ctx, input: i32| async move {
            let x = ctx.run_as_step("step-one", move |_ctx| async move { Ok(input + 1) }).await?;
            ctx.run_as_step("step-two", move |_ctx| async move { Ok(x + 2) }).await
        },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.create_schedule(CreateScheduleRequest {
        schedule_name: "scheduled-chaos-test".to_string(),
        schedule: "* * * * * * *".to_string(),
        workflow_name: "scheduled-chaos-test".to_string(),
        context: None,
        automatic_backfill: false,
        cron_timezone: Some("UTC".to_string()),
        queue_name: Some(INTERNAL_QUEUE.to_string()),
        workflow_class_name: None,
    })
    .await?;
    ctx.launch().await?;

    let count = env_usize("DBOS_CHAOS_WORKFLOW_COUNT", 10_000);
    for i in 0..count {
        let handle = ctx
            .run_workflow::<_, i32>(
                "chaos-step-workflow",
                i as i32,
                WorkflowOptions {
                    workflow_id: Some(format!("chaos-workflow-{i}")),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(handle.get_result(Some(Duration::from_secs(30))).await?, i as i32 + 3);
    }

    let latest = wait_for_scheduled_success(&ctx).await?;
    let age = Utc::now().signed_duration_since(latest.created_at);
    assert!(age.num_seconds() < 10, "latest scheduled execution was {age:?} old");

    ctx.shutdown(Duration::from_secs(5)).await;
    chaos.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires DBOS_CHAOS_DATABASE_URL and DBOS_CHAOS_POSTGRES_*_CMD"]
async fn chaos_send_recv() -> dbos::Result<()> {
    let config = ChaosConfig::from_env()?;
    let chaos = PostgresChaos::start(config.clone()).await?;
    let ctx = setup_dbos("recv", &config).await?;
    let topic = "test_topic";
    let count = env_usize("DBOS_CHAOS_RECV_COUNT", 10_000);
    let signals = Arc::new((0..count).map(|_| Arc::new(Event::default())).collect::<Vec<_>>());
    let workflow_signals = Arc::clone(&signals);

    ctx.register_workflow(
        "chaos-recv-workflow",
        move |ctx, index: usize| {
            let workflow_signals = Arc::clone(&workflow_signals);
            async move {
                let Some(signal) = workflow_signals.get(index) else {
                    return Err(DbosError::invalid_argument(format!("missing signal {index}")));
                };
                signal.set();
                ctx.recv::<String>(topic, Duration::from_secs(600)).await
            }
        },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.launch().await?;

    for i in 0..count {
        let handle = ctx
            .run_workflow::<_, String>(
                "chaos-recv-workflow",
                i,
                WorkflowOptions {
                    workflow_id: Some(format!("chaos-recv-{i}")),
                    ..Default::default()
                },
            )
            .await?;
        signals[i].wait().await;
        let value = Uuid::new_v4().to_string();
        ctx.send(handle.workflow_id(), value.clone(), topic).await?;
        assert_eq!(handle.get_result(Some(Duration::from_secs(30))).await?, value);
    }

    ctx.shutdown(Duration::from_secs(5)).await;
    chaos.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires DBOS_CHAOS_DATABASE_URL and DBOS_CHAOS_POSTGRES_*_CMD"]
async fn chaos_events() -> dbos::Result<()> {
    let config = ChaosConfig::from_env()?;
    let chaos = PostgresChaos::start(config.clone()).await?;
    let ctx = setup_dbos("events", &config).await?;
    let key = "test_key";

    ctx.register_workflow(
        "chaos-event-workflow",
        move |ctx, _input: ()| async move {
            let value = Uuid::new_v4().to_string();
            ctx.set_event(key, value.clone()).await?;
            Ok(value)
        },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.launch().await?;

    let count = env_usize("DBOS_CHAOS_EVENT_COUNT", 5_000);
    for i in 0..count {
        let workflow_id = format!("chaos-event-{i}");
        let handle = ctx
            .run_workflow::<_, String>(
                "chaos-event-workflow",
                (),
                WorkflowOptions { workflow_id: Some(workflow_id.clone()), ..Default::default() },
            )
            .await?;
        let value = handle.get_result(Some(Duration::from_secs(30))).await?;
        let retrieved: String = ctx.get_event(&workflow_id, key, Duration::from_secs(600)).await?;
        assert_eq!(retrieved, value);
    }

    ctx.shutdown(Duration::from_secs(5)).await;
    chaos.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires DBOS_CHAOS_DATABASE_URL and DBOS_CHAOS_POSTGRES_*_CMD"]
async fn chaos_queues() -> dbos::Result<()> {
    let config = ChaosConfig::from_env()?;
    let chaos = PostgresChaos::start(config.clone()).await?;
    let ctx = setup_dbos("queues", &config).await?;
    let mut queue = WorkflowQueue::new("test_queue");
    queue.polling_interval = Duration::from_millis(100);
    ctx.register_queue(queue.clone()).await?;

    ctx.register_workflow(
        "chaos-step-one",
        |ctx, input: i32| async move { ctx.run_as_step("step-one", move |_ctx| async move { Ok(input + 1) }).await },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.register_workflow(
        "chaos-step-two",
        |ctx, input: i32| async move { ctx.run_as_step("step-two", move |_ctx| async move { Ok(input + 2) }).await },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    let queue_name = queue.name.clone();
    ctx.register_workflow(
        "chaos-queue-parent",
        move |ctx, input: i32| {
            let queue_name = queue_name.clone();
            async move {
                let first = ctx
                    .run_workflow::<_, i32>(
                        "chaos-step-one",
                        input,
                        WorkflowOptions { queue_name: Some(queue_name.clone()), ..Default::default() },
                    )
                    .await?;
                let x = first.get_result(Some(Duration::from_secs(30))).await?;
                let second = ctx
                    .run_workflow::<_, i32>("chaos-step-two", x, WorkflowOptions { queue_name: Some(queue_name), ..Default::default() })
                    .await?;
                second.get_result(Some(Duration::from_secs(30))).await
            }
        },
        WorkflowRegistrationOptions::default(),
    )
    .await?;
    ctx.launch().await?;

    let count = env_usize("DBOS_CHAOS_QUEUE_COUNT", 30);
    for i in 0..count {
        let handle = ctx
            .run_workflow::<_, i32>(
                "chaos-queue-parent",
                i as i32,
                WorkflowOptions {
                    workflow_id: Some(format!("chaos-queue-{i}")),
                    queue_name: Some(queue.name.clone()),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(handle.get_result(Some(Duration::from_secs(60))).await?, i as i32 + 3);
    }

    ctx.shutdown(Duration::from_secs(5)).await;
    chaos.shutdown().await
}

async fn wait_for_scheduled_success(ctx: &DbosContext) -> dbos::Result<dbos::WorkflowStatus> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let rows = ctx
            .list_workflows(ListWorkflowsOptions {
                workflow_name: Some("scheduled-chaos-test".to_string()),
                status: vec![WorkflowStatusType::Success],
                sort_desc: true,
                limit: Some(1),
                ..Default::default()
            })
            .await?;
        if let Some(row) = rows.into_iter().next() {
            return Ok(row);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(DbosError::timeout("timed out waiting for scheduled chaos workflow"));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn retry_shell(command: &str, label: &str, timeout: Duration) -> dbos::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match run_shell(command) {
            Ok(()) => return Ok(()),
            Err(error) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(DbosError::database(format!("{label} failed: {error}")));
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

fn run_shell(command: &str) -> dbos::Result<()> {
    let output = Command::new("sh")
        .arg("-c")
        .arg(command)
        .output()
        .map_err(|err| DbosError::database(format!("failed to run command {command:?}: {err}")))?;
    if output.status.success() {
        return Ok(());
    }
    Err(DbosError::database(format!(
        "command {command:?} failed with status {:?}: {}{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )))
}

async fn sleep_or_stop(duration: Duration, stop: &AtomicBool) {
    let deadline = tokio::time::Instant::now() + duration;
    while !stop.load(Ordering::SeqCst) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn random_duration(min: Duration, max: Duration) -> Duration {
    if max <= min {
        return min;
    }
    let spread_ms = max.saturating_sub(min).as_millis().min(u128::from(u64::MAX)) as u64;
    min + Duration::from_millis(rand::random::<u64>() % spread_ms.saturating_add(1))
}

fn chaos_schema(test_name: &str) -> dbos::Result<String> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| DbosError::invalid_argument(format!("system time before unix epoch: {err}")))?
        .as_millis();
    Ok(format!("dbos_chaos_{test_name}_{}_{}", std::process::id(), timestamp))
}

fn env_bool(name: &str) -> bool {
    std::env::var(name).ok().is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|raw| raw.parse::<usize>().ok()).unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|raw| raw.parse::<u64>().ok()).unwrap_or(default)
}
