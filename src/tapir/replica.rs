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
    transaction_log: HashMap<OccTransactionId, (Timestamp, bool)>,
    /// Extension to TAPIR: Garbage collection watermark time.
    /// - All transactions before this are committed/aborted.
    /// - Must not prepare transactions before this.
    /// - May (at any time) garbage collect MVCC versions
    ///   that are invalid at and after this.
    /// - May (at any time) garbage collect keys with
    ///   a tombstone valid at and after this.
    gc_watermark: u64,
    /// Minimum acceptable prepare time (tentative).
    min_prepare_time: u64,
    /// Minimum acceptable prepare time (finalized).
    finalized_min_prepare_time: u64,
}

impl<K: Key, V: Value> Replica<K, V> {
    pub fn new(linearizable: bool) -> Self {
        Self {
            inner: OccStore::new(linearizable),
            transaction_log: HashMap::new(),
            gc_watermark: 0,
            min_prepare_time: 0,
            finalized_min_prepare_time: 0,
        }
    }

    fn recover_coordination<T: Transport<Message = IrMessage<Self>>>(
        transaction_id: OccTransactionId,
        transaction: OccTransaction<K, V, Timestamp>,
        commit: Timestamp,
        membership: IrMembership<T>,
        transport: T,
    ) -> impl Future<Output = ()> {
        eprintln!("trying to recover {transaction_id:?}");
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
                        let decision = if results.contains_key(&CR::Prepare(OccPrepareResult::Fail)) {
                            OccPrepareResult::Fail
                        } else if results
                            .get(&CR::Prepare(OccPrepareResult::TooLate))
                            .copied()
                            .unwrap_or_default()
                            >= membership.f_plus_one()
                        {
                            OccPrepareResult::TooLate
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
                        };
                        eprintln!(
                            "backup coordinator deciding on {results:?} -> {decision:?} for {transaction_id:?}"
                        );
                        CR::Prepare(decision)
                    },
                )
                .await;

            let CR::Prepare(result) = result else {
                debug_assert!(false);
                return;
            };

            eprintln!("BACKUP COORD got {result:?} for  {transaction_id:?} @ {commit:?}");

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
                OccPrepareResult::Fail | OccPrepareResult::TooLate => {
                    client
                        .invoke_inconsistent(IO::Abort {
                            transaction_id,
                            transaction,
                            commit: Some(commit),
                        })
                        .await
                }
                _ => {}
            }
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
                    .insert(*transaction_id, (*commit, true));
                if let Some((ts, committed)) = old {
                    debug_assert!(committed);
                    debug_assert_eq!(ts, *commit);
                }
                self.inner
                    .commit(*transaction_id, transaction.clone(), *commit);
            }
            IO::Abort {
                transaction_id,
                transaction,
                commit,
            } => {
                if commit
                    .map(|commit| {
                        self.inner
                            .prepared
                            .get(transaction_id)
                            .map(|(ts, _, _)| *ts == commit)
                            .unwrap_or(false)
                    })
                    .unwrap_or(
                        self.inner
                            .prepared
                            .get(transaction_id)
                            .map(|(ts, _, _)| ts.time >= self.min_prepare_time)
                            .unwrap_or(false),
                    )
                {
                    self.inner.remove_prepared(*transaction_id);
                }
                if let Some(commit) = commit {
                    debug_assert!(
                        !self
                            .transaction_log
                            .get(transaction_id)
                            .map(|(ts, c)| *c && ts == commit)
                            .unwrap_or(false),
                        "{transaction_id:?} committed at {commit:?}"
                    );
                } else {
                    debug_assert!(
                        !self
                            .transaction_log
                            .get(transaction_id)
                            .map(|(_, c)| *c)
                            .unwrap_or(false),
                        "{transaction_id:?} committed"
                    );
                    self.transaction_log
                        .insert(*transaction_id, (Default::default(), false));
                }
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
            } else if self
                .transaction_log
                .get(transaction_id)
                .map(|(ts, c)| *c && ts == commit)
                .unwrap_or(false)
            {
                // Already committed at this timestamp.
                OccPrepareResult::Ok
            } else if self
                .transaction_log
                .get(transaction_id)
                .map(|(_, c)| !*c)
                .unwrap_or(false)
            {
                // Already aborted by client.
                OccPrepareResult::Fail
            } else if let Some(f) = self
                .inner
                .prepared
                .get(transaction_id)
                .filter(|(ts, _, _)| *ts == *commit)
                .map(|(_, _, f)| *f)
            {
                // Already prepared at this timestamp.
                if f || !*backup {
                    OccPrepareResult::Ok
                } else {
                    // Backup coordinator needs to wait to finalized results,
                    // which won't simply evaporate on a view change.
                    OccPrepareResult::Retry {
                        proposed: commit.time,
                    }
                }
            } else if commit.time < self.min_prepare_time
                || self
                    .inner
                    .prepared
                    .get(transaction_id)
                    .map(|(c, _, _)| c.time < self.min_prepare_time)
                    .unwrap_or(false)
                || self
                    .transaction_log
                    .get(transaction_id)
                    .map(|(ts, _)| ts.time < self.min_prepare_time)
                    .unwrap_or(false)
            {
                // Too late to prepare or reprepare.
                OccPrepareResult::TooLate
            } else if let Some((ts, c)) = self.transaction_log.get(transaction_id) {
                // Committed at a different timestamp.
                debug_assert_ne!(ts, commit);
                debug_assert!(*c);
                OccPrepareResult::Retry { proposed: ts.time }
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

    fn finalize_consensus(&mut self, op: &Self::CO, res: &Self::CR) {
        match op {
            CO::Prepare {
                transaction_id,
                transaction,
                commit,
                backup,
            } => {
                if let Some((ts, _, finalized)) = self.inner.prepared.get_mut(transaction_id) {
                    if *commit == *ts {
                        *finalized = true;
                    }
                }
            }
            CO::RaiseMinPrepareTime { time } => {
                self.finalized_min_prepare_time = self.finalized_min_prepare_time.max(*time);
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
                    if !*backup {
                        if matches!(entry.result, CR::Prepare(OccPrepareResult::Ok)) {
                            if !self.inner.prepared.contains_key(transaction_id)
                                && !self.transaction_log.contains_key(transaction_id)
                            {
                                // Enough other replicas agreed to prepare
                                // the transaction so it must be okay.
                                self.inner.add_prepared(
                                    *transaction_id,
                                    transaction.clone(),
                                    *commit,
                                    true,
                                );
                            }
                        } else if self
                            .inner
                            .prepared
                            .get(transaction_id)
                            .map(|(ts, _, _)| ts == commit)
                            .unwrap_or(false)
                        {
                            // TODO: Would it be safe for a backup coordinator to
                            // trigger this code path?
                            self.inner.remove_prepared(*transaction_id);
                        }
                    }
                }
                CO::RaiseMinPrepareTime { .. } => {
                    if let CR::RaiseMinPrepareTime { time } = &entry.result {
                        self.min_prepare_time = self.min_prepare_time.max(*time);
                        self.finalized_min_prepare_time =
                            self.finalized_min_prepare_time.max(*time);
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

            self.exec_inconsistent(&entry.op);
        }
    }

    fn merge(
        &mut self,
        d: HashMap<IrOpId, (Self::CO, Self::CR)>,
        u: Vec<(IrOpId, Self::CO, Self::CR)>,
    ) -> HashMap<IrOpId, Self::CR> {
        let mut ret: HashMap<IrOpId, Self::CR> = HashMap::new();

        // Remove inconsistencies caused by out-of-order execution at the leader.
        self.min_prepare_time = self.finalized_min_prepare_time;
        for transaction_id in self
            .inner
            .prepared
            .iter()
            .filter(|(_, (_, _, f))| !*f)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>()
        {
            self.inner.remove_prepared(transaction_id);
        }

        // Preserve any potentially valid fast-path consensus operations.
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
                        CR::Prepare({
                            let result = if let &CR::Prepare(result) = reply {
                                result
                            } else {
                                debug_assert!(false);
                                OccPrepareResult::Abstain
                            };

                            if self
                                .transaction_log
                                .get(transaction_id)
                                .map(|(ts, c)| !*c || (*c && ts == commit))
                                .unwrap_or(false)
                                || !result.is_ok()
                            {
                                result
                                /*
                                } else if commit.time < self.min_prepare_time
                                    || self
                                        .inner
                                        .prepared
                                        .get(transaction_id)
                                        .map(|(c, _)| c.time < self.min_prepare_time)
                                        .unwrap_or(false)
                                    || self
                                        .transaction_log
                                        .get(transaction_id)
                                        .map(|ts| ts.time < self.min_prepare_time)
                                        .unwrap_or(false)
                                {
                                    OccPrepareResult::TooLate
                                */
                            } else {
                                // Analogous to the IR slow path.
                                //
                                // Ensure the successful prepare is possible and, if so, durable.
                                self.inner.prepare(
                                    *transaction_id,
                                    transaction.clone(),
                                    *commit,
                                    *backup,
                                )
                            }
                        }),
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
                        } else if self
                            .inner
                            .prepared
                            .get(transaction_id)
                            .map(|(c, _, _)| c == commit)
                            .unwrap_or(false)
                        {
                            // Already prepared at this timestamp.
                            OccPrepareResult::Ok
                        } else if commit.time < self.min_prepare_time
                            || self
                                .inner
                                .prepared
                                .get(transaction_id)
                                .map(|(c, _, _)| c.time < self.min_prepare_time)
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
        eprintln!(
            "there are {} prepared transactions",
            self.inner.prepared.len()
        );
        let threshold: u64 = transport.time_offset(-500);
        for (transaction_id, (commit, transaction, _)) in &self.inner.prepared {
            if commit.time > threshold {
                // Allow the client to finish on its own.
                continue;
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
