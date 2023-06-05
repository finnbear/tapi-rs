pub(crate) trait Message: Clone {}

impl<T: Clone> Message for T {}
