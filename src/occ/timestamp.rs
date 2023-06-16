use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

pub trait Timestamp: Ord + Copy + Default + Debug + Serialize + DeserializeOwned {
    type Time: Ord + Copy + Serialize + DeserializeOwned;

    fn time(&self) -> Self::Time;
}
