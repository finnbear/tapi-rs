use std::hash::Hash;

use serde::{Deserialize, Serialize};

use super::Timestamp;
use crate::{OccTransaction, OccTransactionId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request<K: Hash + Eq, V> {
    Get {
        // TODO: Used by read-only extension:  transaction_id: OccTransactionId,
        /// Key to get the latest version of
        key: K,
        /// Get a different version instead (not part of normal TAPIR).
        timestamp: Option<Timestamp>,
    },
    Prepare {
        /// Id of transaction to prepare.
        transaction_id: OccTransactionId,
        /// Transaction to prepare.
        transaction: OccTransaction<K, V, Timestamp>,
        /// Proposed commit timestamp.
        commit: Timestamp,
    },
    Commit {
        transaction_id: OccTransactionId,
        /// Same as successfully prepared transaction.
        transaction: OccTransaction<K, V, Timestamp>,
        /// Same as successfully prepared commit timestamp.
        commit: Timestamp,
    },
    Abort {
        transaction_id: OccTransactionId,
        /// Same as unsuccessfully prepared transaction.
        transaction: OccTransaction<K, V, Timestamp>,
        /// Same as unsuccessfully prepared commit timestamp.
        commit: Timestamp,
    },
}
