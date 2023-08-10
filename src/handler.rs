use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;


pub type HandlerFn = Arc<
    dyn Fn(Value) -> Pin<Box<dyn Future<Output = anyhow::Result<Value>> + Send + Sync>>
        + Send
        + Sync,
>;

pub struct Handler {
    pub name: String,
    pub func: HandlerFn,
}

impl Debug for Handler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handler")
            .field("name", &self.name)
            .field("func", &"<...>")
            .finish()
    }
}

pub type HandlerMap = HashMap<String, Arc<Handler>>;
