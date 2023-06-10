use std::fmt::Debug;

pub(crate) trait Message: Clone + Send + Debug + 'static {}

impl<T: Clone + Send + Debug + 'static> Message for T {}
