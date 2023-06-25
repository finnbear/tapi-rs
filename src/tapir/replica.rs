use super::{Key, Timestamp, Value, CO, CR, IO, UO, UR};
use crate::util::vectorize;
use crate::{
    IrClient, IrMembership, IrMessage, IrOpId, IrRecord, IrReplicaUpcalls, OccPrepareResult,
    OccStore, OccTransaction, OccTransactionId, Transport,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, future::Future, hash::Hash};

/// Diverge from TAPIR and don't maintain a no-vote list. Instead, wait for a
/// view change to syncronize each participant shard's prepare result and then
/// let one or more of many possible backup coordinators take them at face-value.
#[derive(Serialize, Deserialize)]
pub struct Replica<K, V> {
    #[serde(bound(deserialize = "K: Deserialize<'de> + Hash + Eq, V: Deserialize<'de>"))]
    inner: OccStore<K, V, Timestamp>,
    /// Stores the commit timestamp, read/write sets, and commit status (true if committed) for
    /// all known committed and aborted transactions.
    #[serde(
        with = "vectorize",
        bound(
            serialize = "K: Serialize, V: Serialize",
            deserialize = "K: Deserialize<'de> + Hash + Eq, V: Deserialize<'de>"
        )
    )]
    transaction_log: HashMap<OccTransactionId, (Timestamp, OccTransaction<K, V, Timestamp>, bool)>,
    /// Extension to TAPIR: Garbage collection watermark time.
    /// - All transactions before this are committed/aborted.
    /// - Must not prepare transactions before this.
    /// - May (at any time) garbage collect MVCC versions
    ///   that are invalid at and after this.
    /// - May (at any time) garbage collect keys with
    ///   a tombstone valid at and after this.
    gc_watermark: u64,
    /// Minimum acceptable prepare time.
    min_prepare_time: u64,
}

impl<K: Key, V: Value> Replica<K, V> {
    pub fn new(linearizable: bool) -> Self {
        Self {
            inner: OccStore::new(linearizable),
            transaction_log: HashMap::new(),
            gc_watermark: 0,
            min_prepare_time: 0,
        }
    }

    fn recover_coordination<T: Transport<Message = IrMessage<Self>>>(
        transaction_id: OccTransactionId,
        transaction: OccTransaction<K, V, Timestamp>,
        commit: Timestamp,
        membership: IrMembership<T>,
        transport: T,
    ) -> impl Future<Output = ()> {
        println!("trying to recover {transaction_id:?}");
        let client = IrClient::<Self, T>::new(membership, transport);
        async move {
            client
                .invoke_consensus(
                    CO::RaiseMinPrepareTime {
                        time: commit.time + 1,
                    },
                    |results, size| CR::RaiseMinPrepareTime {
                        time: results
                            .keys()
                            .filter_map(|r| {
                                if let CR::RaiseMinPrepareTime { time } = r {
                                    Some(*time)
                                } else {
                                    debug_assert!(false);
                                    None
                                }
                            })
                            .max()
                            .unwrap_or_default(),
                    },
                )
                .await;

            let result = client
                .invoke_consensus(
                    CO::Prepare {
                        transaction_id,
                        transaction: transaction.clone(),
                        commit,
                        backup: true,
                    },
                    |results, membership| {
                        println!("backup coordinator deciding on {results:?}");
                        CR::Prepare(
                            if results.contains_key(&CR::Prepare(OccPrepareResult::Fail)) {
                                OccPrepareResult::Fail
                            } else if results
                                .get(&CR::Prepare(OccPrepareResult::TooLate))
                                .copied()
                                .unwrap_or_default()
                                >= membership.f_plus_one()
                            {
                                OccPrepareResult::Fail
                            } else if results
                                .get(&CR::Prepare(OccPrepareResult::Ok))
                                .copied()
                                .unwrap_or_default()
                                >= membership.f_plus_one()
                            {
                                OccPrepareResult::Ok
                            } else {
                                OccPrepareResult::Retry {
                                    proposed: commit.time,
                                }
                            },
                        )
                    },
                )
                .await;

            let CR::Prepare(result) = result else {
                debug_assert!(false);
                return;
            };

            match result {
                OccPrepareResult::Ok => {
                    client
                        .invoke_inconsistent(IO::Commit {
                            transaction_id,
                            transaction,
                            commit,
                        })
                        .await
                }
                OccPrepareResult::Fail => {
                    client
                        .invoke_inconsistent(IO::Abort {
                            transaction_id,
                            transaction,
                            commit,
                        })
                        .await
                }
                _ => {}
            }

            eprintln!("BACKUP COORD got {result:?} for {commit:?}");
        }
    }
}

impl<K: Key, V: Value> IrReplicaUpcalls for Replica<K, V> {
    type UO = UO<K>;
    type UR = UR<V>;
    type IO = IO<K, V>;
    type CO = CO<K, V>;
    type CR = CR;

    fn exec_unlogged(&mut self, op: Self::UO) -> Self::UR {
        match op {
            UO::Get { key, timestamp } => {
                let (v, ts) = if let Some(timestamp) = timestamp {
                    self.inner.get_at(&key, timestamp)
                } else {
                    self.inner.get(&key)
                };
                UR::Get(v.cloned(), ts)
            }
        }
    }

    fn exec_inconsistent(&mut self, op: &Self::IO) {
        match op {
            IO::Commit {
                transaction_id,
                transaction,
                commit,
            } => {
                let old = self
                    .transaction_log
                    .insert(*transaction_id, (*commit, transaction.clone(), true));
                if let Some((_, _, was_committed)) = old {
                    debug_assert!(was_committed);
                }
                self.inner
                    .commit(*transaction_id, transaction.clone(), *commit);
            }
            IO::Abort {
                transaction_id,
                transaction,
                commit,
            } => {
                let old = self
                    .transaction_log
                    .insert(*transaction_id, (*commit, transaction.clone(), false));
                if let Some((_, _, was_committed)) = old {
                    debug_assert!(!was_committed);
                }
                self.inner.abort(*transaction_id);
            }
        }
    }

    fn exec_consensus(&mut self, op: &Self::CO) -> Self::CR {
        match op {
            CO::Prepare {
                transaction_id,
                transaction,
                commit,
                backup,
            } => CR::Prepare(if commit.time < self.gc_watermark {
                // In theory, could check the other conditions first, but
                // that might hide bugs.
                OccPrepareResult::TooOld
            } else if let Some((ts, _, committed)) = self.transaction_log.get(transaction_id) {
                if *committed {
                    if commit == ts {
                        // Already committed at this timestamp.
                        OccPrepareResult::Ok
                    } else {
                        // Committed at a different timestamp.
                        OccPrepareResult::Retry { proposed: ts.time }
                    }
                } else {
                    // Already aborted or committed at a different timestamp.
                    OccPrepareResult::Fail
                }
            } else if self
                .inner
                .prepared
                .get(transaction_id)
                .map(|(c, _)| c == commit)
                .unwrap_or(false)
            {
                // Already prepared at this timestamp.
                OccPrepareResult::Ok
            } else if commit.time < self.min_prepare_time
                || self
                    .inner
                    .prepared
                    .get(transaction_id)
                    .map(|(c, _)| c.time < self.min_prepare_time)
                    .unwrap_or(false)
            {
                // Too late to prepare or reprepare.
                OccPrepareResult::TooLate
            } else {
                self.inner
                    .prepare(*transaction_id, transaction.clone(), *commit, *backup)
            }),
            CO::RaiseMinPrepareTime { time } => {
                self.min_prepare_time = self.min_prepare_time.max(*time);
                CR::RaiseMinPrepareTime {
                    time: self.min_prepare_time,
                }
            }
        }
    }

    fn sync(&mut self, local: &IrRecord<Self>, leader: &IrRecord<Self>) {
        for (op_id, entry) in &leader.consensus {
            if local
                .consensus
                .get(op_id)
                .map(|local| local.result == entry.result)
                .unwrap_or(false)
            {
                // Record already in local state.
                continue;
            }

            match &entry.op {
                CO::Prepare {
                    transaction_id,
                    transaction,
                    commit,
                    backup,
                } => {
                    if matches!(entry.result, CR::Prepare(OccPrepareResult::Ok)) {
                        if !self.inner.prepared.contains_key(transaction_id)
                            && !self.transaction_log.contains_key(transaction_id)
                            && !*backup
                        {
                            // Enough other replicas agreed to prepare
                            // the transaction so it must be okay.
                            self.inner
                                .add_prepared(*transaction_id, transaction.clone(), *commit);
                        }
                    } else {
                        // TODO: Is it safe for a backup coordinator to
                        // trigger this code path?
                        self.inner.remove_prepared(*transaction_id);
                    }
                }
                CO::RaiseMinPrepareTime { .. } => {
                    if let CR::RaiseMinPrepareTime { time } = &entry.result {
                        self.min_prepare_time = self.min_prepare_time.max(*time);
                    } else {
                        debug_assert!(false);
                    }
                }
            }
        }
        for (op_id, entry) in &leader.inconsistent {
            if local.inconsistent.contains_key(op_id) {
                // Record already in local state.
                continue;
            }

            match &entry.op {
                IO::Commit {
                    transaction_id,
                    transaction,
                    commit,
                } => {
                    self.inner
                        .commit(*transaction_id, transaction.clone(), *commit);
                    let old = self
                        .transaction_log
                        .insert(*transaction_id, (*commit, transaction.clone(), true));
                    if let Some((_, _, was_committed)) = old {
                        debug_assert!(was_committed);
                    }
                }
                IO::Abort {
                    transaction_id,
                    transaction,
                    commit,
                } => {
                    self.inner.abort(*transaction_id);
                    let old = self
                        .transaction_log
                        .insert(*transaction_id, (*commit, transaction.clone(), false));
                    if let Some((_, _, was_committed)) = old {
                        debug_assert!(!was_committed);
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    fn merge(
        &mut self,
        d: HashMap<IrOpId, (Self::CO, Self::CR)>,
        u: Vec<(IrOpId, Self::CO, Self::CR)>,
    ) -> HashMap<IrOpId, Self::CR> {
        let mut ret: HashMap<IrOpId, Self::CR> = HashMap::new();

        // Remove inconsistencies caused by out-of-order execution at the leader.
        for (op_id, request) in u
            .iter()
            .map(|(op_id, op, _)| (op_id, op))
            .chain(d.iter().map(|(op_id, (op, _))| (op_id, op)))
        {
            match request {
                CO::Prepare {
                    transaction_id,
                    transaction,
                    ..
                } => {
                    self.inner.remove_prepared(*transaction_id);
                }
                CO::RaiseMinPrepareTime { .. } => {}
            }
        }

        for (op_id, (request, reply)) in &d {
            match request {
                CO::Prepare {
                    transaction_id,
                    transaction,
                    commit,
                    backup,
                } => {
                    ret.insert(
                        *op_id,
                        CR::Prepare(
                            if commit.time < self.min_prepare_time
                                || self
                                    .inner
                                    .prepared
                                    .get(transaction_id)
                                    .map(|(c, _)| c.time < self.min_prepare_time)
                                    .unwrap_or(false)
                            {
                                OccPrepareResult::TooLate
                            } else if !self.transaction_log.contains_key(transaction_id)
                                && matches!(reply, CR::Prepare(OccPrepareResult::Ok))
                            {
                                // Ensure the successful prepare is possible and, if so, durable.
                                self.inner.prepare(
                                    *transaction_id,
                                    transaction.clone(),
                                    *commit,
                                    *backup,
                                )
                            } else if let CR::Prepare(result) = reply {
                                *result
                            } else {
                                debug_assert!(false);
                                OccPrepareResult::Abstain
                            },
                        ),
                    );
                }
                CO::RaiseMinPrepareTime { time } => {
                    ret.insert(
                        *op_id,
                        CR::RaiseMinPrepareTime {
                            time: if let CR::RaiseMinPrepareTime { time } = reply {
                                *time
                            } else {
                                debug_assert!(false);
                                *time
                            },
                        },
                    );
                }
            }
        }

        for (op_id, request, _) in &u {
            match request {
                CO::Prepare {
                    transaction_id,
                    transaction,
                    commit,
                    backup,
                } => {
                    ret.insert(
                        *op_id,
                        CR::Prepare(if commit.time < self.gc_watermark {
                            OccPrepareResult::TooOld
                        } else if commit.time < self.min_prepare_time
                            || self
                                .inner
                                .prepared
                                .get(transaction_id)
                                .map(|(c, _)| c.time < self.min_prepare_time)
                                .unwrap_or(false)
                        {
                            OccPrepareResult::TooLate
                        } else {
                            self.inner.prepare(
                                *transaction_id,
                                transaction.clone(),
                                *commit,
                                *backup,
                            )
                        }),
                    );
                }
                CO::RaiseMinPrepareTime { time } => {
                    self.min_prepare_time = self.min_prepare_time.max(*time);
                    ret.insert(
                        *op_id,
                        CR::RaiseMinPrepareTime {
                            time: self.min_prepare_time,
                        },
                    );
                }
            }
        }

        ret
    }

    fn tick<T: Transport<Message = IrMessage<Self>>>(
        &mut self,
        membership: &IrMembership<T>,
        transport: &T,
    ) {
        let threshold: u64 = transport.time_offset(-500);
        for (transaction_id, (commit, transaction)) in &self.inner.prepared {
            if commit.time > threshold {
                // Allow the client to finish on its own.
                break;
            }
            let future = Self::recover_coordination(
                *transaction_id,
                transaction.clone(),
                *commit,
                membership.clone(),
                transport.clone(),
            );
            tokio::spawn(future);
        }
    }
}
