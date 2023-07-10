use super::{Key, Replica, ShardClient, ShardTransaction, Timestamp, Value};
use crate::{IrMembership, OccPrepareResult, OccTransactionId, Transport};
use rand::{thread_rng, Rng};
use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::select;

pub struct Client<K: Key, V: Value, T: Transport<Replica<K, V>>> {
    /// TODO: Add multiple shards.
    inner: ShardClient<K, V, T>,
    #[allow(unused)]
    transport: T,
    next_transaction_number: AtomicU64,
}

pub struct Transaction<K: Key, V: Value, T: Transport<Replica<K, V>>> {
    #[allow(unused)]
    id: OccTransactionId,
    // TODO: Multiple shards.
    inner: ShardTransaction<K, V, T>,
}

impl<K: Key, V: Value, T: Transport<Replica<K, V>>> Client<K, V, T> {
    pub fn new(membership: IrMembership<T::Address>, transport: T) -> Self {
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

impl<K: Key, V: Value, T: Transport<Replica<K, V>>> Transaction<K, V, T> {
    pub fn get(&self, key: K) -> impl Future<Output = Option<V>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: K, value: Option<V>) {
        self.inner.put(key, value);
    }

    fn commit_inner(&self, only_prepare: bool) -> impl Future<Output = Option<Timestamp>> {
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
                    remaining_tries = new_remaining_tries;

                    let new_time =  inner.client.transport().time().max(proposed.saturating_add(1)).max(min_commit_timestamp);
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
                    inner.end(timestamp, ok).await;
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
