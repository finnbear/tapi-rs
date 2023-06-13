use std::hash;

use super::Timestamp;
use crate::OccPrepareResult;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) enum Reply<V> {
    Get(Option<V>, Timestamp),
    Prepare(OccPrepareResult<Timestamp>),
}
