use super::Timestamp;
use crate::OccTransaction;

#[derive(Debug, Clone)]
pub(crate) enum Request<K, V> {
    Get {
        transaction_id: u64,
        key: K,
        timestamp: Option<Timestamp>,
    },
    Prepare {
        transaction_id: u64,
        transaction: OccTransaction<K, V, Timestamp>,
        commit: Timestamp,
    },
    Commit {
        transaction_id: u64,
    },
    Abort {
        transaction_id: u64,
    },
}
