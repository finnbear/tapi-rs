use super::{Key, ShardClient, ShardNumber, Sharded, Timestamp, Value};
use crate::{
    util::join, IrClientId, OccPrepareResult, OccTransaction, OccTransactionId, TapirTransport,
};
use rand::{thread_rng, Rng};
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::Context,
    time::Duration,
};
use tokio::select;

pub struct Client<K: Key, V: Value, T: TapirTransport<K, V>> {
    inner: Arc<Mutex<Inner<K, V, T>>>,
    next_transaction_number: AtomicU64,
}

pub struct Inner<K: Key, V: Value, T: TapirTransport<K, V>> {
    id: IrClientId,
    clients: HashMap<ShardNumber, ShardClient<K, V, T>>,
    transport: T,
}

impl<K: Key, V: Value, T: TapirTransport<K, V>> Inner<K, V, T> {
    fn shard_client(
        this: &Arc<Mutex<Self>>,
        shard: ShardNumber,
    ) -> impl Future<Output = ShardClient<K, V, T>> + Send + 'static {
        let this = Arc::clone(this);

        async move {
            let future = {
                let lock = this.lock().unwrap();
                if let Some(client) = lock.clients.get(&shard) {
                    return client.clone();
                }
                lock.transport.shard_addresses(shard)
            };

            let membership = future.await;

            let mut lock = this.lock().unwrap();
            let lock = &mut *lock;
            lock.clients
                .entry(shard)
                .or_insert_with(|| {
                    ShardClient::new(lock.id, shard, membership, lock.transport.clone())
                })
                .clone()
        }
    }
}

pub struct Transaction<K: Key, V: Value, T: TapirTransport<K, V>> {
    id: OccTransactionId,
    client: Arc<Mutex<Inner<K, V, T>>>,
    inner: Arc<Mutex<TransactionInner<K, V>>>,
}

struct TransactionInner<K: Key, V: Value> {
    inner: OccTransaction<K, V, Timestamp>,
    read_cache: HashMap<Sharded<K>, Option<V>>,
}

impl<K: Key, V: Value, T: TapirTransport<K, V>> Client<K, V, T> {
    pub fn new(transport: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                id: IrClientId::new(),
                clients: Default::default(),
                transport,
            })),
            next_transaction_number: AtomicU64::new(thread_rng().gen()),
        }
    }

    pub fn begin(&self) -> Transaction<K, V, T> {
        let transaction_id = OccTransactionId {
            client_id: self.inner.lock().unwrap().id,
            number: self.next_transaction_number.fetch_add(1, Ordering::Relaxed),
        };
        Transaction {
            id: transaction_id,
            client: Arc::clone(&self.inner),
            inner: Arc::new(Mutex::new(TransactionInner {
                inner: Default::default(),
                read_cache: Default::default(),
            })),
        }
    }
}

impl<K: Key, V: Value, T: TapirTransport<K, V>> Transaction<K, V, T> {
    pub fn get(&self, key: impl Into<Sharded<K>>) -> impl Future<Output = Option<V>> {
        let key = key.into();
        let client = Arc::clone(&self.client);
        let inner = Arc::clone(&self.inner);

        async move {
            let client = Inner::shard_client(&client, key.shard).await;

            loop {
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

                let (value, timestamp) = client.get(key.key.clone(), None).await;

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
                return value;
            }
        }
    }

    pub fn put(&self, key: impl Into<Sharded<K>>, value: Option<V>) {
        let key = key.into();
        let mut lock = self.inner.lock().unwrap();
        lock.inner.add_write(key, value);
    }

    fn commit_inner(&self, only_prepare: bool) -> impl Future<Output = Option<Timestamp>> {
        let id = self.id;
        let client = self.client.clone();
        let inner = self.inner.clone();

        let transaction = {
            let lock = inner.lock().unwrap();
            lock.inner.clone()
        };

        let min_commit_timestamp = max_read_timestamp(&transaction).saturating_add(1);
        let mut timestamp = {
            let client = self.client.lock().unwrap();
            Timestamp {
                time: client.transport.time().max(min_commit_timestamp),
                client_id: client.id,
            }
        };
        let participants = transaction.participants();

        async move {
            // Writes are buffered; make sure the shard clients exist.
            for key in transaction.write_set.keys() {
                Inner::shard_client(&client, key.shard).await;
            }

            let mut remaining_tries = 3u8;

            loop {
                let future = {
                    let client = client.lock().unwrap();
                    join(participants.iter().map(|shard| {
                        let shard_client = client.clients.get(shard).unwrap();
                        let future = shard_client.prepare(id, &transaction, timestamp);
                        (*shard, future)
                    }))
                };

                let results = future
                    .until(
                        |results: &HashMap<ShardNumber, OccPrepareResult<Timestamp>>,
                         _cx: &mut Context<'_>| {
                            results.values().any(|v| v.is_fail() || v.is_abstain())
                        },
                    )
                    .await;

                let result = results.values().next().unwrap();

                if let OccPrepareResult::Retry { proposed } = &result && let Some(new_remaining_tries) = remaining_tries.checked_sub(1) {
                    remaining_tries = new_remaining_tries;

                    let new_time =  client.lock().unwrap().transport.time().max(proposed.saturating_add(1)).max(min_commit_timestamp);
                    if new_time != timestamp.time {
                        timestamp.time = new_time;
                        continue;
                    }
                }
                if matches!(result, OccPrepareResult::TooLate | OccPrepareResult::TooOld) {
                    continue;
                }
                let ok = matches!(result, OccPrepareResult::Ok);
                if !only_prepare {
                    let future = {
                        let client = client.lock().unwrap();
                        join(participants.iter().map(|shard| {
                            let shard_client = client.clients.get(shard).unwrap();
                            let future = shard_client.end(id, &transaction, timestamp, ok);
                            (*shard, future)
                        }))
                    };

                    future.until(1 /* TODO */).await;
                }

                if ok && remaining_tries != 3 {
                    eprintln!("Retry actually worked!");
                }

                return Some(timestamp).filter(|_| ok);
            }
        }
    }

    #[doc(hidden)]
    pub fn only_prepare(self) -> impl Future<Output = Option<Timestamp>> {
        self.commit_inner(true)
    }

    pub fn commit(self) -> impl Future<Output = Option<Timestamp>> {
        self.commit_inner(false)
    }

    #[doc(hidden)]
    pub fn commit2(
        self,
        inject_fault: Option<Duration>,
    ) -> impl Future<Output = Option<Timestamp>> {
        let inner = self.commit();

        async move {
            if let Some(duration) = inject_fault {
                let sleep = T::sleep(duration);
                select! {
                    _ = sleep => {
                        std::future::pending::<()>().await;
                        unreachable!();
                    }
                    result = inner => {
                        result
                    }
                }
            } else {
                inner.await
            }
        }
    }
}

pub fn max_read_timestamp<K, V>(transaction: &OccTransaction<K, V, Timestamp>) -> u64 {
    transaction
        .read_set
        .values()
        .map(|v| v.time)
        .max()
        .unwrap_or_default()
}
