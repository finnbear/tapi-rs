use super::{Reply, Request, ShardClient, ShardTransaction, Timestamp};
use crate::{IrMembership, IrMessage, OccPrepareResult, OccTransactionId, Transport};
use rand::{thread_rng, Rng};
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    hash::Hash,
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

pub(crate) struct Client<K, V, T: Transport> {
    /// TODO: Add multiple shards.
    inner: ShardClient<K, V, T>,
    transport: T,
    next_transaction_number: AtomicU64,
}

pub(crate) struct Transaction<K, V, T: Transport> {
    id: OccTransactionId,
    // TODO: Multiple shards.
    inner: ShardTransaction<K, V, T>,
}

impl<
        K: Debug + Clone + Hash + Eq,
        V: Eq + Hash + Debug + Clone + Send,
        T: Transport<Message = IrMessage<Request<K, V>, Reply<V>>>,
    > Client<K, V, T>
{
    pub(crate) fn new(membership: IrMembership<T>, transport: T) -> Self {
        Self {
            inner: ShardClient::new(membership, transport.clone()),
            transport,
            next_transaction_number: AtomicU64::new(thread_rng().gen()),
        }
    }

    pub(crate) fn begin(&self) -> Transaction<K, V, T> {
        let transaction_id = OccTransactionId {
            client_id: self.inner.id(),
            number: self.next_transaction_number.fetch_add(1, Ordering::Relaxed),
        };
        Transaction {
            id: transaction_id,
            inner: self.inner.begin(transaction_id),
        }
    }
}

impl<
        K: Debug + Clone + Hash + Eq,
        V: Eq + Hash + Debug + Clone + Send,
        T: Transport<Message = IrMessage<Request<K, V>, Reply<V>>>,
    > Transaction<K, V, T>
{
    pub(crate) fn get(&self, key: K) -> impl Future<Output = Option<V>> {
        self.inner.get(key)
    }

    pub(crate) fn put(&self, key: K, value: Option<V>) {
        self.inner.put(key, value);
    }

    pub(crate) fn commit(&self) -> impl Future<Output = Option<Timestamp>> {
        fn get_time() -> u64 {
            use rand::Rng;
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
                + rand::thread_rng().gen_range(0..100 * 1000 * 1000)
        }

        let inner = self.inner.clone();
        let mut timestamp = Timestamp {
            time: get_time().max(inner.max_read_timestamp().saturating_add(1)),
            client_id: self.inner.client.id(),
        };

        async move {
            let mut remaining_tries = 3u8;

            loop {
                let result = inner.prepare(timestamp).await;

                if let OccPrepareResult::Retry { proposed } = &result && let Some(new_remaining_tries) = remaining_tries.checked_sub(1) {
                    timestamp.time = get_time().max(proposed.saturating_add(1));
                    remaining_tries = new_remaining_tries;
                    panic!("Retry code path was executed");
                    continue;
                }
                let ok = matches!(result, OccPrepareResult::Ok);
                inner.end(timestamp, ok).await;
                return Some(timestamp).filter(|_| ok);
            }
        }
    }
}
