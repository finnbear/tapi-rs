use rand::{thread_rng, Rng};

use super::{Reply, Request, Timestamp};
use crate::{
    transport::Transport, IrClient, IrClientId, IrMembership, IrMessage, IrReplicaIndex,
    OccPrepareResult, OccTransaction, OccTransactionId,
};
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    hash::Hash,
    process::Output,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

pub(crate) struct ShardClient<K, V, T: Transport> {
    inner: IrClient<T, Request<K, V>, Reply<V>>,
}

impl<
        K: Debug + Clone + Hash + Eq,
        V: Eq + Hash + Debug + Clone + Send,
        T: Transport<Message = IrMessage<Request<K, V>, Reply<V>>>,
    > ShardClient<K, V, T>
{
    pub(crate) fn new(membership: IrMembership<T>, transport: T) -> Self {
        Self {
            inner: IrClient::new(membership, transport),
        }
    }

    // TODO: Use same id for all shards?
    pub(crate) fn id(&self) -> IrClientId {
        self.inner.id()
    }

    pub(crate) fn begin(&self, transaction_id: OccTransactionId) -> ShardTransaction<K, V, T> {
        ShardTransaction::new(self.inner.clone(), transaction_id)
    }
}

pub(crate) struct ShardTransaction<K, V, T: Transport> {
    pub(crate) client: IrClient<T, Request<K, V>, Reply<V>>,
    inner: Arc<Mutex<Inner<K, V>>>,
}

impl<K, V, T: Transport> Clone for ShardTransaction<K, V, T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            inner: Arc::clone(&self.inner),
        }
    }
}

struct Inner<K, V> {
    id: OccTransactionId,
    inner: OccTransaction<K, V, Timestamp>,
    read_cache: HashMap<K, Option<V>>,
}

impl<
        K: Eq + Hash + Clone + Debug,
        V: Clone + Debug + Hash + Eq + Send,
        T: Transport<Message = IrMessage<Request<K, V>, Reply<V>>>,
    > ShardTransaction<K, V, T>
{
    fn new(client: IrClient<T, Request<K, V>, Reply<V>>, id: OccTransactionId) -> Self {
        Self {
            client,
            inner: Arc::new(Mutex::new(Inner {
                id,
                inner: Default::default(),
                read_cache: Default::default(),
            })),
        }
    }

    pub(crate) fn get(&self, key: K) -> impl Future<Output = Option<V>> {
        let client = self.client.clone();
        let inner = Arc::clone(&self.inner);

        async move {
            let lock = inner.lock().unwrap();

            // Read own writes.
            if let Some(write) = lock.inner.write_set.get(&key) {
                return write.as_ref().cloned();
            }

            // Consistent reads.
            if let Some(read) = lock.read_cache.get(&key) {
                return read.as_ref().cloned();
            }
            drop(lock);

            let future = client.invoke_unlogged(
                IrReplicaIndex(0),
                Request::Get {
                    key: key.clone(),
                    timestamp: None,
                },
            );

            let reply = future.await;

            let Reply::Get(value, timestamp) = reply else {
                panic!();
            };

            let mut lock = inner.lock().unwrap();

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

    pub(crate) fn put(&self, key: K, value: Option<V>) {
        let mut lock = self.inner.lock().unwrap();
        lock.inner.add_write(key, value);
    }

    pub(crate) fn prepare(
        &self,
        timestamp: Timestamp,
    ) -> impl Future<Output = OccPrepareResult<Timestamp>> {
        let mut lock = self.inner.lock().unwrap();
        let future = self.client.invoke_consensus(
            Request::Prepare {
                transaction_id: lock.id,
                transaction: lock.inner.clone(),
                commit: timestamp,
            },
            |results, membership_size| {
                let mut ok_count = 0;
                let mut timestamp = 0u64;

                for (reply, count) in results {
                    let Reply::Prepare(reply) = reply else {
                        panic!();
                    };

                    match reply {
                        OccPrepareResult::Ok => {
                            ok_count += count;
                        }
                        OccPrepareResult::Retry { proposed } => {
                            timestamp = timestamp.max(proposed);
                        }
                        OccPrepareResult::Abstain => {}
                        OccPrepareResult::Fail => {
                            return Reply::Prepare(OccPrepareResult::Fail);
                        }
                        OccPrepareResult::NoVote => unimplemented!(),
                    }
                }

                Reply::Prepare(if ok_count >= membership_size.f_plus_one() {
                    OccPrepareResult::Ok
                } else {
                    OccPrepareResult::Retry {
                        proposed: timestamp,
                    }
                })
            },
        );
        drop(lock);

        async move {
            let reply = future.await;
            let Reply::Prepare(result) = reply else {
                unreachable!();
            };
            result
        }
    }

    pub(crate) fn end(
        &self,
        prepared_timestamp: Timestamp,
        commit: bool,
    ) -> impl Future<Output = ()> {
        let mut lock = self.inner.lock().unwrap();
        let future = self.client.invoke_inconsistent(if commit {
            Request::Commit {
                transaction_id: lock.id,
                transaction: lock.inner.clone(),
                commit: prepared_timestamp,
            }
        } else {
            Request::Abort {
                transaction_id: lock.id,
                transaction: lock.inner.clone(),
                commit: prepared_timestamp,
            }
        });
        drop(lock);
        future
    }
}
