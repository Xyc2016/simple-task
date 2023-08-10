use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

pub type TaskFunc = Box<dyn Fn(Value) -> Pin<Box<dyn Future<Output = anyhow::Result<Value>>>>>;

pub struct Task {
    pub name: String,
    pub func: TaskFunc,
}

impl Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("name", &self.name)
            .field("func", &"<...>")
            .finish()
    }
}

pub type TaskMap = HashMap<String, Task>;
