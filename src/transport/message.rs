pub(crate) trait Message: Clone + Send + 'static {}

impl<T: Clone + Send + 'static> Message for T {}
