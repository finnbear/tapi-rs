use std::fmt::Debug;

use serde::{de::DeserializeOwned, Serialize};

pub trait Message: Clone + Send + Debug + 'static {}

impl<T: Clone + Send + Debug + 'static> Message for T {}
