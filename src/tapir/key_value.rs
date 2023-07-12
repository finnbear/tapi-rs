use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, hash::Hash};

pub trait Key:
    Debug + Clone + Ord + Hash + Send + Sync + Serialize + DeserializeOwned + 'static
{
}

impl<T: Debug + Clone + Ord + Hash + Send + Sync + Serialize + DeserializeOwned + 'static> Key
    for T
{
}

pub trait Value:
    Debug + Clone + Eq + Hash + Send + Sync + Serialize + DeserializeOwned + 'static
{
}

impl<T: Debug + Clone + Eq + Hash + Send + Sync + Serialize + DeserializeOwned + 'static> Value
    for T
{
}
