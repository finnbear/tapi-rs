use super::OpId;
use std::collections::HashMap;

#[derive(Copy, Clone, Debug)]
pub(crate) enum State {
    Tentative,
    Finalized,
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum Consistency {
    Inconsistent,
    Consistent,
}

pub(crate) struct Entry<O, R> {
    pub(crate) op: O,
    pub(crate) consistency: Consistency,
    pub(crate) result: Option<R>,
    pub(crate) state: State,
}

pub(crate) struct Record<O, R> {
    pub(crate) entries: HashMap<OpId, Entry<O, R>>,
}

impl<O, R> Default for Record<O, R> {
    fn default() -> Self {
        Self {
            entries: Default::default(),
        }
    }
}
