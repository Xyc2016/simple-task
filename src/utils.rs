
pub mod lang {
    pub fn type_name_of<T>(_: T) -> &'static str {
        std::any::type_name::<T>()
    }
}
