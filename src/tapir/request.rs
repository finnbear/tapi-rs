use super::Timestamp;
use crate::{OccTransaction, OccTransactionId};

#[derive(Debug, Clone)]
pub(crate) enum Request<K, V> {
    Get {
        transaction_id: OccTransactionId,
        key: K,
        timestamp: Option<Timestamp>,
    },
    Prepare {
        transaction_id: OccTransactionId,
        transaction: OccTransaction<K, V, Timestamp>,
        commit: Timestamp,
    },
    Commit {
        transaction_id: OccTransactionId,
    },
    Abort {
        transaction_id: OccTransactionId,
    },
}
