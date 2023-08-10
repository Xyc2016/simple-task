use crate::contants::task::TaskStatus;



impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Processing => "processing",
            Self::Error => "error",
            Self::Done => "done",
        }
    }
}
