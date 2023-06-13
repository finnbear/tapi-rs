use super::{Reply, Request, Timestamp};
use crate::{
    transport::Transport, IrClient, IrMembership, IrMessage, IrReplicaIndex, OccTransaction,
    OccTransactionId,
};
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    hash::Hash,
    process::Output,
    sync::{Arc, Mutex},
};

pub(crate) struct ShardClient<K, V, T: Transport<Message = IrMessage<Request<K, V>, Reply<V>>>> {
    inner: IrClient<T, Request<K, V>, Reply<V>>,
    transaction: Option<Arc<Mutex<Transaction<K, V>>>>,
}

struct Transaction<K, V> {
    id: OccTransactionId,
    inner: OccTransaction<K, V, Timestamp>,
    read_cache: HashMap<K, Option<V>>,
}

impl<K, V> Transaction<K, V> {
    fn new(id: OccTransactionId) -> Self {
        Self {
            id,
            inner: Default::default(),
            read_cache: Default::default(),
        }
    }
}

impl<
        K: Debug + Clone + Hash + Eq,
        V: Eq + Hash + Debug + Clone,
        T: Transport<Message = IrMessage<Request<K, V>, Reply<V>>>,
    > ShardClient<K, V, T>
{
    pub(crate) fn new(membership: IrMembership<T>, transport: T) -> Self {
        Self {
            inner: IrClient::new(membership, transport),
            transaction: None,
        }
    }

    pub(crate) fn begin(&mut self, transaction_id: OccTransactionId) {
        if self.transaction.is_some() {
            panic!();
        }

        self.transaction = Some(Arc::new(Mutex::new(Transaction::new(transaction_id))));
    }

    pub(crate) fn get(&mut self, key: K) -> impl Future<Output = Option<V>> {
        let inner = self.inner.clone();
        let transaction = Arc::clone(self.transaction.as_ref().unwrap());

        async move {
            let lock = transaction.lock().unwrap();

            // Read own writes.
            if let Some(write) = lock.inner.write_set.get(&key) {
                return write.as_ref().cloned();
            }

            // Consistent reads.
            if let Some(read) = lock.read_cache.get(&key) {
                return read.as_ref().cloned();
            }

            let future = inner.invoke_unlogged(
                IrReplicaIndex(0),
                Request::Get {
                    transaction_id: lock.id,
                    key: key.clone(),
                    timestamp: None,
                },
            );

            drop(lock);

            let reply = future.await;

            let Reply::Get(value, timestamp) = reply else {
                panic!();
            };

            let mut lock = transaction.lock().unwrap();

            // Read own writes.
            if let Some(write) = lock.inner.write_set.get(&key) {
                return write.as_ref().cloned();
            }

            // Consistent reads.
            if let Some(read) = lock.read_cache.get(&key) {
                return read.as_ref().cloned();
            }

            lock.read_cache.insert(key.clone(), value.clone());
            lock.inner.add_read(key, timestamp);
            value
        }
    }

    pub(crate) fn put(&self, transaction_id: OccTransactionId, key: K, value: Option<V>) {
        let transaction = self.transaction.as_ref().unwrap();
        let mut lock = transaction.lock().unwrap();
        lock.inner.add_write(key, value);
    }

    pub(crate) fn prepare(
        &self,
        transaction_id: OccTransactionId,
        timestamp: Timestamp,
    ) -> impl Future<Output = bool> {
        let transaction = self.transaction.as_ref().unwrap();
        let mut lock = transaction.lock().unwrap();
        self.inner.invoke_consensus(
            Request::Prepare {
                transaction_id: lock.id,
                transaction: lock.inner.clone(),
                commit: timestamp,
            },
            |results, membership_size| {
                let mut ok_count = 0;
                let mut timestamp = 0u64;

                todo!();
            },
        );
        std::future::ready(todo!())
    }

    pub(crate) fn commit(&self, transaction_id: OccTransactionId) -> impl Future<Output = ()> {
        std::future::ready(todo!())
    }

    pub(crate) fn abort(&self, transaction_id: OccTransactionId) -> impl Future<Output = ()> {
        std::future::ready(todo!())
    }
}
