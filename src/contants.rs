pub mod task {
    #[derive(Debug)]
    pub enum TaskStatus {
        Waiting,
        Processing,
        Error,
        Done,
    }
}
