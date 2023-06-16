use serde::{Deserialize, Serialize};

use super::OpId;
use std::{collections::HashMap, fmt::Debug};

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum State {
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
    pub fn is_tentative(&self) -> bool {
        matches!(self, Self::Tentative)
    }

    pub fn is_finalized(&self) -> bool {
        matches!(self, Self::Finalized)
    }
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum Consistency {
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
    pub fn is_inconsistent(&self) -> bool {
        matches!(self, Self::Inconsistent)
    }

    pub fn is_consensus(&self) -> bool {
        matches!(self, Self::Consensus)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry<O, R> {
    pub op: O,
    pub consistency: Consistency,
    pub result: Option<R>,
    pub state: State,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record<O, R> {
    pub entries: HashMap<OpId, Entry<O, R>>,
}

impl<O, R> Default for Record<O, R> {
    fn default() -> Self {
        Self {
            entries: Default::default(),
        }
    }
}
