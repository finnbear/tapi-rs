use std::hash;

use serde::{Deserialize, Serialize};

use super::Timestamp;
use crate::OccPrepareResult;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Reply<V> {
    Get(Option<V>, Timestamp),
    Prepare(OccPrepareResult<Timestamp>),
}
