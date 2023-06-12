use super::Timestamp;
use crate::OccPrepareResult;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Reply<V> {
    Get(Option<(V, Timestamp)>),
    Prepare(OccPrepareResult<Timestamp>),
}
