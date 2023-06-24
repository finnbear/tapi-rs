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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UR<V> {
    Get(Option<V>, Timestamp),
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
    Abort {
        transaction_id: OccTransactionId,
        /// Same as unsuccessfully prepared transaction.
        #[serde(bound(deserialize = "K: Eq + Deserialize<'de> + Hash, V: Deserialize<'de>"))]
        transaction: OccTransaction<K, V, Timestamp>,
        /// Same as unsuccessfully prepared commit timestamp.
        commit: Timestamp,
    },
}

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

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum CR {
    Prepare(OccPrepareResult<Timestamp>),
    RaiseMinPrepareTime { time: u64 },
}
