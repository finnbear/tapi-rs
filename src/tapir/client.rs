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
        V: Eq + Hash + Debug + Clone,
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
        V: Eq + Hash + Debug + Clone,
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
        let inner = self.inner.clone();
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let timestamp = Timestamp {
            time,
            client_id: self.inner.client.id(),
        };
        let future = inner.prepare(timestamp);

        async move {
            let result = future.await;
            println!("COMMITTING {:?}", result);
            // TODO: retries.
            let ok = matches!(result, OccPrepareResult::Ok);
            inner.end(timestamp, ok).await;
            Some(timestamp).filter(|_| ok)
        }
    }
}
