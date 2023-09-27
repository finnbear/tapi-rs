use super::{Key, Replica, ShardNumber, Timestamp, Value, CO, CR, IO, UO, UR};
use crate::{
    transport::Transport, IrClient, IrClientId, IrMembership, OccPrepareResult, OccTransaction,
    OccTransactionId,
};
use std::future::Future;

pub struct ShardClient<K: Key, V: Value, T: Transport<Replica<K, V>>> {
    shard: ShardNumber,
    pub(crate) inner: IrClient<Replica<K, V>, T>,
}

impl<K: Key, V: Value, T: Transport<Replica<K, V>>> Clone for ShardClient<K, V, T> {
    fn clone(&self) -> Self {
        Self {
            shard: self.shard,
            inner: self.inner.clone(),
        }
    }
}

impl<K: Key, V: Value, T: Transport<Replica<K, V>>> ShardClient<K, V, T> {
    pub fn new(
        id: IrClientId,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
        transport: T,
    ) -> Self {
        let mut inner = IrClient::new(membership, transport);

        // Id of all shard clients must match for the timestamps to match during recovery.
        inner.set_id(id);

        Self { shard, inner }
    }

    pub fn get(
        &self,
        key: K,
        timestamp: Option<Timestamp>,
    ) -> impl Future<Output = (Option<V>, Timestamp)> {
        let future = self.inner.invoke_unlogged(UO::Get { key, timestamp });

        async move {
            let reply = future.await;

            if let UR::Get(value, timestamp) = reply {
                (value, timestamp)
            } else {
                debug_assert!(false);

                // Was valid at the beginning of time (the transaction will
                // abort if that's too old).
                (None, Default::default())
            }
        }
    }

    pub fn prepare(
        &self,
        transaction_id: OccTransactionId,
        transaction: &OccTransaction<K, V, Timestamp>,
        timestamp: Timestamp,
    ) -> impl Future<Output = OccPrepareResult<Timestamp>> + Send {
        let future = self.inner.invoke_consensus(
            CO::Prepare {
                transaction_id,
                transaction: transaction.clone(),
                commit: timestamp,
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
        transaction_id: OccTransactionId,
        transaction: &OccTransaction<K, V, Timestamp>,
        prepared_timestamp: Timestamp,
        commit: bool,
    ) -> impl Future<Output = ()> + Send {
        self.inner.invoke_inconsistent(if commit {
            IO::Commit {
                transaction_id,
                transaction: transaction.clone(),
                commit: prepared_timestamp,
            }
        } else {
            IO::Abort {
                transaction_id,
                commit: None,
            }
        })
    }

    pub fn raise_min_prepare_time(&self, time: u64) -> impl Future<Output = u64> + Send {
        let future =
            self.inner
                .invoke_consensus(CO::RaiseMinPrepareTime { time }, |results, size| {
                    let times = results.iter().filter_map(|(r, c)| {
                        if let CR::RaiseMinPrepareTime { time } = r {
                            Some((*time, *c))
                        } else {
                            debug_assert!(false);
                            None
                        }
                    });

                    // Find a time that a quorum of replicas agree on.
                    CR::RaiseMinPrepareTime {
                        time: times
                            .clone()
                            .filter(|&(time, _)| {
                                times
                                    .clone()
                                    .filter(|&(t, _)| t >= time)
                                    .map(|(_, c)| c)
                                    .sum::<usize>()
                                    >= size.f_plus_one()
                            })
                            .map(|(t, _)| t)
                            .max()
                            .unwrap_or_else(|| {
                                debug_assert!(false);
                                0
                            }),
                    }
                });
        async move {
            match future.await {
                CR::RaiseMinPrepareTime { time } => time,
                _ => {
                    debug_assert!(false);
                    0
                }
            }
        }
    }
}
