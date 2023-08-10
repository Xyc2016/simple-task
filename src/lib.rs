pub mod utils;
use std::{collections::HashMap, sync::Arc};

use contants::task::TaskStatus;
use redis::{self};
use serde_json::Value;
pub mod contants;
use handler::{Handler, HandlerFn, HandlerMap};
pub mod handler;
use redis::AsyncCommands;
pub mod task;

#[derive(Debug)]
pub struct SimpleTaskApp {
    pub tasks: HandlerMap,
    redis_client: redis::Client,
}

const WAITING_TASK_ID_QUEUE: &str = "simple_task:waiting_task_id";
const PROCESSING_TASK_ID_QUEUE: &str = "simple_task:processing_task_id";
const TASK_TABLE: &str = "simple_task:task";

impl SimpleTaskApp {
    pub fn new(redis_client: redis::Client) -> Self {
        Self {
            tasks: HandlerMap::new(),
            redis_client,
        }
    }

    pub fn gen_task_id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    pub async fn send_task(&self, handler_name: &str, input: Value) -> anyhow::Result<()> {
        let mut conn = self.redis_client.get_async_connection().await?;

        let task_id = Self::gen_task_id();
        let task_id_key = join_key!(TASK_TABLE, &task_id);
        let task_id_key = task_id_key.as_str();

        conn.hset_multiple(
            task_id_key,
            &[
                ("handler_name", handler_name),
                ("input", &input.to_string()),
                ("status", TaskStatus::Waiting.as_str()),
            ],
        )
        .await?;
        conn.lpush(WAITING_TASK_ID_QUEUE, task_id).await?;
        Ok(())
    }

    pub async fn run_task(
        task_id: &str,
        handler: Arc<Handler>,
        input: Value,
        mut redis_conn: redis::aio::Connection,
    ) -> anyhow::Result<()> {
        let f = &handler.func;
        let task_id_key = join_key!(TASK_TABLE, task_id);
        match f(input).await {
            Ok(result) => {
                redis_conn
                    .hset_multiple(
                        task_id_key,
                        &[
                            ("status", TaskStatus::Done.as_str()),
                            ("result", &result.to_string()),
                        ],
                    )
                    .await?;
                redis_conn.lrem(PROCESSING_TASK_ID_QUEUE, 1, task_id).await?;
                redis_conn.expire(task_id_key, 3600).await?;
            }
            Err(e) => {
                redis_conn
                    .hset_multiple(
                        task_id_key,
                        &[
                            ("status", TaskStatus::Error.as_str()),
                            ("error", &e.to_string()),
                        ],
                    )
                    .await?;
            }
        };
        Ok(())
    }

    pub fn register_handler(&mut self, name: &str, func: HandlerFn) {
        let name = name.to_string();
        let task = handler::Handler {
            name: name.clone(),
            func,
        };
        self.tasks.insert(name, Arc::new(task));
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let mut conn = self.redis_client.get_async_connection().await?;

        loop {
            let task_id: Option<String> = conn
                .blmove(
                    WAITING_TASK_ID_QUEUE,
                    PROCESSING_TASK_ID_QUEUE,
                    redis::Direction::Right,
                    redis::Direction::Left,
                    1,
                )
                .await?;
            let Some(ref task_id) = task_id else {
                continue;
            };

            let task_id_key = join_key!(TASK_TABLE, task_id);

            conn.hset(task_id_key, "status", TaskStatus::Processing.as_str())
                .await?;

            let task: HashMap<String, String> = conn.hgetall(task_id_key).await?;

            let (input, handler_name) = match (task.get("input"), task.get("handler_name")) {
                (Some(input), Some(handler_name)) => (input.clone(), handler_name.clone()),
                _ => {
                    conn.hset_multiple(
                        task_id_key,
                        &[
                            ("status", TaskStatus::Error.as_str()),
                            ("error", "Missing field"),
                        ],
                    )
                    .await?;
                    continue;
                }
            };

            let input: Value = serde_json::from_str(&input)?;
            let handler = self
                .tasks
                .get(&handler_name)
                .ok_or(anyhow::anyhow!("Handler {} not found", handler_name))?
                .clone();
            let conn = self.redis_client.get_async_connection().await?;
            let task_id = task_id.clone();
            tokio::spawn(async move {
                Self::run_task(&task_id, handler, input, conn)
                    .await
                    .unwrap();
            });
        }
    }
}

#[macro_export]
macro_rules! register_handler {
    ($a:expr, $b:expr) => {
        $a.register_handler(
            $crate::utils::lang::type_name_of($b),
            std::sync::Arc::new(|input| Box::pin($b(input))),
        )
    };
}

#[macro_export]
macro_rules! send_task {
    ($app:expr, $f:expr, $value:expr) => {
        $app.send_task($crate::utils::lang::type_name_of($f), $value)
    };
}

#[macro_export]
macro_rules! join_key {
    ($a:expr) => {
        $a
    };
    ($a:expr, $($b:expr),+) => {
        &vec![$a, $($b),+].join(":")
    };
}
