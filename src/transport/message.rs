use std::fmt::Debug;

pub trait Message: Clone + Send + Debug + 'static {}

impl<T: Clone + Send + Debug + 'static> Message for T {}
