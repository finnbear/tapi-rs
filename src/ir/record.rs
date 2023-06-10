use super::OpId;
use std::collections::HashMap;

#[derive(Copy, Clone, Debug)]
pub(crate) enum State {
    Finalized,
    Tentative,
}

impl State {
    pub(crate) fn is_tentative(&self) -> bool {
        matches!(self, Self::Tentative)
    }

    pub(crate) fn is_finalized(&self) -> bool {
        matches!(self, Self::Finalized)
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum Consistency {
    Inconsistent,
    Consensus,
}

impl Consistency {
    pub(crate) fn is_inconsistent(&self) -> bool {
        matches!(self, Self::Inconsistent)
    }

    pub(crate) fn is_consensus(&self) -> bool {
        matches!(self, Self::Consensus)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Entry<O, R> {
    pub(crate) op: O,
    pub(crate) consistency: Consistency,
    pub(crate) result: Option<R>,
    pub(crate) state: State,
}

#[derive(Debug, Clone)]
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
