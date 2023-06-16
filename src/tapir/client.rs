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

pub struct Client<K: Hash + Eq, V, T: Transport> {
    /// TODO: Add multiple shards.
    inner: ShardClient<K, V, T>,
    transport: T,
    next_transaction_number: AtomicU64,
}

pub struct Transaction<K: Hash + Eq, V, T: Transport> {
    id: OccTransactionId,
    // TODO: Multiple shards.
    inner: ShardTransaction<K, V, T>,
}

impl<
        K: Debug + Clone + Hash + Eq + Send,
        V: Eq + Hash + Debug + Clone + Send,
        T: Transport<Message = IrMessage<Request<K, V>, Reply<V>>>,
    > Client<K, V, T>
{
    pub fn new(membership: IrMembership<T>, transport: T) -> Self {
        Self {
            inner: ShardClient::new(membership, transport.clone()),
            transport,
            next_transaction_number: AtomicU64::new(thread_rng().gen()),
        }
    }

    pub fn begin(&self) -> Transaction<K, V, T> {
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
        K: Debug + Clone + Hash + Eq + Send,
        V: Eq + Hash + Debug + Clone + Send,
        T: Transport<Message = IrMessage<Request<K, V>, Reply<V>>>,
    > Transaction<K, V, T>
{
    pub fn get(&self, key: K) -> impl Future<Output = Option<V>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: K, value: Option<V>) {
        self.inner.put(key, value);
    }

    pub fn commit(&self) -> impl Future<Output = Option<Timestamp>> {
        let inner = self.inner.clone();
        let min_commit_timestamp = inner.max_read_timestamp().saturating_add(1);
        let mut timestamp = Timestamp {
            time: inner.client.transport().time().max(min_commit_timestamp),
            client_id: inner.client.id(),
        };

        async move {
            let mut remaining_tries = 3u8;

            loop {
                let result = inner.prepare(timestamp).await;

                if let OccPrepareResult::Retry { proposed } = &result && let Some(new_remaining_tries) = remaining_tries.checked_sub(1) {
                    let new_time =  inner.client.transport().time().max(proposed.saturating_add(1)).max(min_commit_timestamp);
                    if new_time != timestamp.time {
                        timestamp.time = new_time;
                        remaining_tries = new_remaining_tries;
                        continue;
                    }
                }
                let ok = matches!(result, OccPrepareResult::Ok);
                inner.end(timestamp, ok).await;

                if ok && remaining_tries != 3 {
                    eprintln!("Retry actually worked!");
                }

                return Some(timestamp).filter(|_| ok);
            }
        }
    }
}
