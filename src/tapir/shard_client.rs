use rand::{thread_rng, Rng};

use super::{Key, Replica, Timestamp, Value, CO, CR, IO, UO, UR};
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

pub struct ShardClient<K: Key, V: Value, T: Transport> {
    inner: IrClient<Replica<K, V>, T>,
}

impl<K: Key, V: Value, T: Transport<Message = IrMessage<Replica<K, V>>>> ShardClient<K, V, T> {
    pub fn new(membership: IrMembership<T>, transport: T) -> Self {
        Self {
            inner: IrClient::new(membership, transport),
        }
    }

    // TODO: Use same id for all shards?
    pub fn id(&self) -> IrClientId {
        self.inner.id()
    }

    pub fn begin(&self, transaction_id: OccTransactionId) -> ShardTransaction<K, V, T> {
        ShardTransaction::new(self.inner.clone(), transaction_id)
    }
}

pub struct ShardTransaction<K: Key, V: Value, T: Transport> {
    pub client: IrClient<Replica<K, V>, T>,
    inner: Arc<Mutex<Inner<K, V>>>,
}

impl<K: Key, V: Value, T: Transport> Clone for ShardTransaction<K, V, T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            inner: Arc::clone(&self.inner),
        }
    }
}

struct Inner<K: Key, V: Value> {
    id: OccTransactionId,
    inner: OccTransaction<K, V, Timestamp>,
    read_cache: HashMap<K, Option<V>>,
}

impl<K: Key, V: Value, T: Transport<Message = IrMessage<Replica<K, V>>>> ShardTransaction<K, V, T> {
    fn new(client: IrClient<Replica<K, V>, T>, id: OccTransactionId) -> Self {
        Self {
            client,
            inner: Arc::new(Mutex::new(Inner {
                id,
                inner: Default::default(),
                read_cache: Default::default(),
            })),
        }
    }

    pub fn max_read_timestamp(&self) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .inner
            .read_set
            .values()
            .map(|v| v.time)
            .max()
            .unwrap_or_default()
    }

    pub fn get(&self, key: K) -> impl Future<Output = Option<V>> + Send {
        let client = self.client.clone();
        let inner = Arc::clone(&self.inner);

        async move {
            {
                let lock = inner.lock().unwrap();

                // Read own writes.
                if let Some(write) = lock.inner.write_set.get(&key) {
                    return write.as_ref().cloned();
                }

                // Consistent reads.
                if let Some(read) = lock.read_cache.get(&key) {
                    return read.as_ref().cloned();
                }
            }

            use rand::Rng;
            let future = client.invoke_unlogged(
                IrReplicaIndex(rand::thread_rng().gen_range(0..3)),
                UO::Get {
                    key: key.clone(),
                    timestamp: None,
                },
            );

            let reply = future.await;

            let UR::Get(value, timestamp) = reply;

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

    pub fn put(&self, key: K, value: Option<V>) {
        let mut lock = self.inner.lock().unwrap();
        lock.inner.add_write(key, value);
    }

    pub fn prepare(
        &self,
        timestamp: Timestamp,
    ) -> impl Future<Output = OccPrepareResult<Timestamp>> + Send {
        let mut lock = self.inner.lock().unwrap();
        let future = self.client.invoke_consensus(
            CO::Prepare {
                transaction_id: lock.id,
                transaction: lock.inner.clone(),
                commit: timestamp,
                backup: false,
            },
            |results, membership_size| {
                let mut ok_count = 0;
                let mut abstain_count = 0;
                let mut timestamp = 0u64;

                for (reply, count) in results {
                    let CR::Prepare(reply) = reply else {
                        debug_assert!(false);
                        continue;
                    };

                    match reply {
                        OccPrepareResult::Ok => {
                            ok_count += count;
                        }
                        OccPrepareResult::Retry { proposed } => {
                            timestamp = timestamp.max(proposed);
                        }
                        OccPrepareResult::Abstain => {
                            abstain_count += count;
                        }
                        OccPrepareResult::Fail => {
                            return CR::Prepare(OccPrepareResult::Fail);
                        }
                        OccPrepareResult::TooLate => {
                            return CR::Prepare(OccPrepareResult::TooLate);
                        }
                        OccPrepareResult::TooOld => {
                            return CR::Prepare(OccPrepareResult::TooOld);
                        }
                    }
                }

                CR::Prepare(if ok_count >= membership_size.f_plus_one() {
                    OccPrepareResult::Ok
                } else if abstain_count >= membership_size.f_plus_one() {
                    OccPrepareResult::Fail
                } else if timestamp > 0 {
                    OccPrepareResult::Retry {
                        proposed: timestamp,
                    }
                } else {
                    OccPrepareResult::Fail
                })
            },
        );
        drop(lock);

        async move {
            let reply = future.await;
            if let CR::Prepare(result) = reply {
                result
            } else {
                debug_assert!(false);
                OccPrepareResult::Fail
            }
        }
    }

    pub fn end(
        &self,
        prepared_timestamp: Timestamp,
        commit: bool,
    ) -> impl Future<Output = ()> + Send {
        let mut lock = self.inner.lock().unwrap();
        let future = self.client.invoke_inconsistent(if commit {
            IO::Commit {
                transaction_id: lock.id,
                transaction: lock.inner.clone(),
                commit: prepared_timestamp,
            }
        } else {
            IO::Abort {
                transaction_id: lock.id,
                transaction: lock.inner.clone(),
                commit: prepared_timestamp,
            }
        });
        drop(lock);
        future
    }
}
