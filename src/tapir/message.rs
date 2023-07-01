use super::{Key, Timestamp, Value};
use crate::{OccPrepareResult, OccTransaction, OccTransactionId};
use serde::{Deserialize, Serialize};
use std::hash::Hash;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UO<K> {
    Get {
        // TODO: Used by read-only extension:  transaction_id: OccTransactionId,
        /// Key to get the latest version of
        key: K,
        /// Get a different version instead (not part of normal TAPIR).
        timestamp: Option<Timestamp>,
    },
    /// For backup coordinators.
    CheckPrepare {
        /// Id of transaction to check the preparedness of.
        transaction_id: OccTransactionId,
        /// Same as (any) known prepared timestamp.
        commit: Timestamp,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UR<V> {
    /// To clients.
    Get(Option<V>, Timestamp),
    /// To backup coordinators.
    CheckPrepare(OccPrepareResult<Timestamp>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IO<K, V> {
    /// Commit a successfully prepared transaction.
    Commit {
        transaction_id: OccTransactionId,
        /// Same as successfully prepared transaction.
        transaction: OccTransaction<K, V, Timestamp>,
        /// Same as successfully prepared commit timestamp.
        commit: Timestamp,
    },
    /// Abort an unsuccessfully prepared transaction.
    ///
    /// Unlike TAPIR, tolerate `Abort` at any timestamp except
    /// that of a successful `Commit`.
    Abort {
        transaction_id: OccTransactionId,
        /// Same as unsuccessfully prepared transaction.
        #[serde(bound(deserialize = "K: Eq + Deserialize<'de> + Hash, V: Deserialize<'de>"))]
        transaction: OccTransaction<K, V, Timestamp>,
        /// Same as unsuccessfully prepared commit timestamp for backup coordinators or `None`
        /// used by clients to abort at every timestamp.
        commit: Option<Timestamp>,
    },
}

impl<K: Eq + Hash, V: PartialEq> PartialEq for IO<K, V> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Commit {
                    transaction_id,
                    transaction,
                    commit,
                },
                Self::Commit {
                    transaction_id: other_transaction_id,
                    transaction: other_transaction,
                    commit: other_commit,
                },
            ) => {
                transaction_id == other_transaction_id
                    && transaction == other_transaction
                    && commit == other_commit
            }
            (
                Self::Abort {
                    transaction_id,
                    transaction,
                    commit,
                },
                Self::Abort {
                    transaction_id: other_transaction_id,
                    transaction: other_transaction,
                    commit: other_commit,
                },
            ) => {
                transaction_id == other_transaction_id
                    && transaction == other_transaction
                    && commit == other_commit
            }
            _ => false,
        }
    }
}

impl<K: Eq + Hash, V: Eq> Eq for IO<K, V> {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CO<K, V> {
    Prepare {
        /// Id of transaction to prepare.
        transaction_id: OccTransactionId,
        /// Transaction to prepare.
        #[serde(bound(deserialize = "K: Eq + Deserialize<'de> + Hash, V: Deserialize<'de>"))]
        transaction: OccTransaction<K, V, Timestamp>,
        /// Proposed commit timestamp.
        commit: Timestamp,
    },
    RaiseMinPrepareTime {
        time: u64,
    },
}

impl<K: Eq + Hash, V: PartialEq> PartialEq for CO<K, V> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Prepare {
                    transaction_id,
                    transaction,
                    commit,
                },
                Self::Prepare {
                    transaction_id: other_transaction_id,
                    transaction: other_transaction,
                    commit: other_commit,
                },
            ) => {
                transaction_id == other_transaction_id
                    && transaction == other_transaction
                    && commit == other_commit
            }
            (
                Self::RaiseMinPrepareTime { time },
                Self::RaiseMinPrepareTime { time: other_time },
            ) => time == other_time,
            _ => false,
        }
    }
}

impl<K: Eq + Hash, V: Eq> Eq for CO<K, V> {}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum CR {
    Prepare(OccPrepareResult<Timestamp>),
    RaiseMinPrepareTime { time: u64 },
}
