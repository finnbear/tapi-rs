use super::OpId;
use std::{collections::HashMap, fmt::Debug};

#[derive(Copy, Clone)]
pub(crate) enum State {
    Finalized,
    Tentative,
}

impl Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Finalized => "Fin",
            Self::Tentative => "Tnt",
        })
    }
}

impl State {
    pub(crate) fn is_tentative(&self) -> bool {
        matches!(self, Self::Tentative)
    }

    pub(crate) fn is_finalized(&self) -> bool {
        matches!(self, Self::Finalized)
    }
}

#[derive(Copy, Clone)]
pub(crate) enum Consistency {
    Inconsistent,
    Consensus,
}

impl Debug for Consistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Inconsistent => "Inc",
            Self::Consensus => "Con",
        })
    }
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
