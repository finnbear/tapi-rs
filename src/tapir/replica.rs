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
            let min_prepare = client
                .invoke_consensus(
                    CO::RaiseMinPrepareTime {
                        time: commit.time + 1,
                    },
                    |results, size| {
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
                    },
                )
                .await;

            let CR::RaiseMinPrepareTime { time: min_prepare_time } = min_prepare else {
                debug_assert!(false);
                return;
            };

            if commit.time >= min_prepare_time {
                // Not ready.
                return;
            }

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

            eprintln!("BACKUP COORD got {result:?} for {transaction_id:?} @ {commit:?}");

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
                    debug_assert!(committed, "{transaction_id:?} aborted");
                    debug_assert_eq!(
                        ts, *commit,
                        "{transaction_id:?} committed at (different) {ts:?}"
                    );
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
                        debug_assert!(
                            !self
                                .transaction_log
                                .get(transaction_id)
                                .map(|(ts, c)| *c && *ts == commit)
                                .unwrap_or(false),
                            "{transaction_id:?} committed at {commit:?}"
                        );
                        self.inner
                            .prepared
                            .get(transaction_id)
                            .map(|(ts, _, _)| *ts == commit)
                            .unwrap_or(true)
                    })
                    .unwrap_or_else(|| {
                        debug_assert!(
                            !self
                                .transaction_log
                                .get(transaction_id)
                                .map(|(_, c)| *c)
                                .unwrap_or(false),
                            "{transaction_id:?} committed"
                        );
                        // TODO: Timestamp.
                        self.transaction_log
                            .insert(*transaction_id, (Default::default(), false));
                        true
                    })
                {
                    self.inner.remove_prepared(*transaction_id);
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
            } else if let Some((ts, c)) = self.transaction_log.get(transaction_id) {
                if *c {
                    if ts == commit {
                        // Already committed at this timestamp.
                        OccPrepareResult::Ok
                    } else if *backup {
                        // Didn't (and will never) commit at this timestamp.
                        OccPrepareResult::Fail
                    } else {
                        // Committed at a different timestamp.
                        OccPrepareResult::Retry { proposed: ts.time }
                    }
                } else {
                    // Already aborted by client.
                    OccPrepareResult::Fail
                }
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
            {
                // Too late to prepare or reprepare.
                OccPrepareResult::TooLate
            } else {
                self.inner
                    .prepare(*transaction_id, transaction.clone(), *commit, *backup)
            }),
            CO::RaiseMinPrepareTime { time } => {
                // Want to avoid tentative prepare operations materializing later on...
                self.min_prepare_time = self.min_prepare_time.max(
                    (*time).min(
                        self.inner
                            .prepared
                            .values()
                            //.filter(|(_, _, f)| !*f)
                            .map(|(ts, _, _)| ts.time)
                            .min()
                            .unwrap_or(u64::MAX),
                    ),
                );
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
                if !*backup && matches!(res, CR::Prepare(OccPrepareResult::Ok)) && let Some((ts, _, finalized)) = self.inner.prepared.get_mut(transaction_id) {
                    if *commit == *ts {
                        *finalized = true;
                    }
                }
            }
            CO::RaiseMinPrepareTime { time } => {
                self.finalized_min_prepare_time = self.finalized_min_prepare_time.max(*time);
                self.min_prepare_time = self.min_prepare_time.max(self.finalized_min_prepare_time);
            }
        }
    }

    fn sync(&mut self, local: &IrRecord<Self>, leader: &IrRecord<Self>) {
        for (op_id, entry) in &leader.consensus {
            if local
                .consensus
                .get(op_id)
                .map(|local| local.state.is_finalized() && local.result == entry.result)
                .unwrap_or(false)
            {
                // Record already finalized in local state.
                continue;
            }

            match &entry.op {
                CO::Prepare {
                    transaction_id,
                    transaction,
                    commit,
                    backup,
                } => {
                    // Backup coordinator prepares don't change state.
                    if !*backup {
                        if matches!(entry.result, CR::Prepare(OccPrepareResult::Ok)) {
                            if self
                                .inner
                                .prepared
                                .get(transaction_id)
                                .map(|(ts, _, _)| ts == commit)
                                .unwrap_or(true)
                                && !self.transaction_log.contains_key(transaction_id)
                            {
                                // Enough other replicas agreed to prepare
                                // the transaction so it must be okay.
                                //
                                // Finalize it immediately since we are syncing
                                // from the leader's record.
                                eprintln!("syncing successful {op_id:?} prepare for {transaction_id:?} at {commit:?}");
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
                            eprintln!(
                                "syncing {:?} {op_id:?} prepare for {transaction_id:?} at {commit:?}",
                                entry.result
                            );
                            self.inner.remove_prepared(*transaction_id);
                        }
                    }
                }
                CO::RaiseMinPrepareTime { .. } => {
                    if let CR::RaiseMinPrepareTime { time } = &entry.result {
                        // Finalized min prepare time is monotonically non-decreasing.
                        self.finalized_min_prepare_time =
                            self.finalized_min_prepare_time.max(*time);
                        // Can rollback tentative prepared time.
                        self.min_prepare_time =
                            self.min_prepare_time.min(self.finalized_min_prepare_time);
                    } else {
                        debug_assert!(false);
                    }
                }
            }
        }
        for (op_id, entry) in &leader.inconsistent {
            if local
                .inconsistent
                .get(op_id)
                .map(|e| e.state.is_finalized())
                .unwrap_or(false)
            {
                // Record already finalized in local state.
                continue;
            }

            eprintln!("syncing inconsistent {op_id:?} {:?}", entry.op);

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
                    let result = CR::Prepare({
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
                            eprintln!("merge preserving {op_id:?} result {result:?} for {transaction_id:?} at {commit:?}");
                            result
                        } else if commit.time < self.min_prepare_time
                            || self
                                .inner
                                .prepared
                                .get(transaction_id)
                                .map(|(c, _, _)| c != commit && c.time < self.min_prepare_time)
                                .unwrap_or(false)
                        {
                            eprintln!("merge found {transaction_id:?} at {commit:?} was TooLate in {op_id:?}",);
                            OccPrepareResult::TooLate
                        } else {
                            // Analogous to the IR slow path.
                            //
                            // Ensure the successful prepare is possible and, if so, durable.

                            let ret = self.inner.prepare(
                                *transaction_id,
                                transaction.clone(),
                                *commit,
                                *backup,
                            );
                            eprintln!("merge found {transaction_id:?} at {commit:?} was {ret:?} in {op_id:?}");

                            ret
                        }
                    });

                    self.finalize_consensus(request, &result);
                    ret.insert(*op_id, result);
                }
                CO::RaiseMinPrepareTime { time } => {
                    ret.insert(*op_id, self.exec_consensus(request));
                    /*
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
                    */
                }
            }
        }

        // Leader is consistent with a quorum so can decide consensus
        // results.
        for (op_id, request, _) in &u {
            ret.insert(*op_id, self.exec_consensus(request));
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
        let mut governor = 2u8;
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

            if let Some(new_governor) = governor.checked_sub(1) {
                governor = new_governor;
            } else {
                break;
            }
        }
    }
}
