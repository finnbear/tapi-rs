use super::OpId;
use std::collections::HashMap;

pub(crate) enum State {
    Tentative,
    Finalized,
}

pub(crate) enum Consistency {
    Inconsistent,
    Consistent,
}

pub(crate) struct Entry<O, R> {
    operation: O,
    consistency: Consistency,
    result: Option<R>,
    state: State,
}

pub(crate) struct Record<O, R> {
    pub(crate) entries: HashMap<OpId, Entry<O, R>>,
}
