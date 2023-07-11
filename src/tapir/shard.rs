use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Formatter};

/// Identifies a shard consisting of a group of replicas.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Number(pub u32);

impl Debug for Number {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "S({})", self.0)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Sharded<K> {
    pub shard: Number,
    pub key: K,
}

impl<K> From<K> for Sharded<K> {
    fn from(key: K) -> Self {
        Self {
            shard: Number(0),
            key,
        }
    }
}
