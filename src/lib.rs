pub mod types;
pub mod utils;
use redis;
use task::{TaskMap, TaskFunc};
pub mod task;


#[derive(Debug)]
pub struct SimpleTaskApp {
    pub tasks: TaskMap,
    redis_client: redis::Client,
}


impl SimpleTaskApp {
    pub fn new(redis_client: redis::Client) -> Self {
        Self {
            tasks: TaskMap::new(),
            redis_client,
        }
    }

    pub fn register_task(&mut self, name: &str, func: TaskFunc) {
        let name = name.to_string();
        let task = task::Task {
            name: name.clone(),
            func,
        };
        self.tasks.insert(name, task);
    }

}

#[macro_export]
macro_rules! register_task {
    ($a:expr, $b:expr) => {
        $a.register_task($crate::utils::lang::type_name_of($b), Box::new(|input| Box::pin($b(input))))
    };
}
