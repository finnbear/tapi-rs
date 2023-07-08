use super::{Key, Timestamp, Value, CO, CR, IO, UO, UR};
use crate::ir::ReplyUnlogged;
use crate::util::vectorize;
use crate::{
    IrClient, IrMembership, IrMembershipSize, IrOpId, IrRecord, IrReplicaIndex, IrReplicaUpcalls,
    OccPrepareResult, OccStore, OccTransaction, OccTransactionId, Transport,
};
use serde::{Deserialize, Serialize};
use std::task::Context;
use std::time::Duration;
use std::{collections::HashMap, future::Future, hash::Hash};

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

    fn recover_coordination<T: Transport<Self>>(
        transaction_id: OccTransactionId,
        transaction: OccTransaction<K, V, Timestamp>,
        commit: Timestamp,
        membership: IrMembership<T::Address>,
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

            fn decide<V>(
                results: &HashMap<IrReplicaIndex, ReplyUnlogged<UR<V>>>,
                membership: IrMembershipSize,
            ) -> Option<OccPrepareResult<Timestamp>> {
                let highest_view = results.values().map(|r| r.view_number).max()?;
                Some(
                    if results
                        .values()
                        .any(|r| matches!(r.result, UR::CheckPrepare(OccPrepareResult::Fail)))
                    {
                        OccPrepareResult::Fail
                    } else if results
                        .values()
                        .filter(|r| {
                            r.view_number == highest_view
                                && matches!(r.result, UR::CheckPrepare(OccPrepareResult::Ok))
                        })
                        .count()
                        >= membership.f_plus_one()
                    {
                        OccPrepareResult::Ok
                    } else if results
                        .values()
                        .filter(|r| {
                            r.view_number == highest_view
                                && matches!(r.result, UR::CheckPrepare(OccPrepareResult::TooLate))
                        })
                        .count()
                        >= membership.f_plus_one()
                    {
                        // TODO: Check views too.
                        OccPrepareResult::TooLate
                    } else {
                        return None;
                    },
                )
            }

            let (future, membership) = client.invoke_unlogged_joined(UO::CheckPrepare {
                transaction_id,
                commit,
            });

            let mut timeout = std::pin::pin!(T::sleep(Duration::from_millis(1000)));
            let results = future
                .until(
                    |results: &HashMap<IrReplicaIndex, ReplyUnlogged<UR<V>>>,
                     cx: &mut Context<'_>| {
                        decide(results, membership).is_some()
                            || timeout.as_mut().poll(cx).is_ready()
                    },
                )
                .await;

            let Some(result) = decide(&results, membership) else {
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
            UO::CheckPrepare {
                transaction_id,
                commit,
            } => {
                UR::CheckPrepare(if commit.time < self.gc_watermark {
                    // In theory, could check the other conditions first, but
                    // that might hide bugs.
                    OccPrepareResult::TooOld
                } else if let Some((ts, c)) = self.transaction_log.get(&transaction_id) {
                    if *c && *ts == commit {
                        // Already committed at this timestamp.
                        OccPrepareResult::Ok
                    } else {
                        // Didn't (and will never) commit at this timestamp.
                        OccPrepareResult::Fail
                    }
                } else if let Some(f) = self
                    .inner
                    .prepared
                    .get(&transaction_id)
                    .filter(|(ts, _, _)| *ts == commit)
                    .map(|(_, _, f)| *f)
                {
                    // Already prepared at this timestamp.
                    if f {
                        // Prepare was finalized.
                        OccPrepareResult::Ok
                    } else {
                        // Prepare wasn't finalized, can't be sure yet.
                        OccPrepareResult::Abstain
                    }
                } else if commit.time < self.min_prepare_time
                    || self
                        .inner
                        .prepared
                        .get(&transaction_id)
                        .map(|(c, _, _)| c.time < self.min_prepare_time)
                        .unwrap_or(false)
                {
                    // Too late for the client to prepare.
                    OccPrepareResult::TooLate
                } else {
                    // Not sure.
                    OccPrepareResult::Abstain
                })
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
                commit,
            } => {
                #[allow(clippy::blocks_in_if_conditions)]
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
            } => CR::Prepare(if commit.time < self.gc_watermark {
                // In theory, could check the other conditions first, but
                // that might hide bugs.
                OccPrepareResult::TooOld
            } else if let Some((ts, c)) = self.transaction_log.get(transaction_id) {
                if *c {
                    if ts == commit {
                        // Already committed at this timestamp.
                        OccPrepareResult::Ok
                    } else {
                        // Committed at a different timestamp.
                        OccPrepareResult::Retry { proposed: ts.time }
                    }
                } else {
                    // Already aborted by client.
                    OccPrepareResult::Fail
                }
            } else if self
                .inner
                .prepared
                .get(transaction_id)
                .map(|(ts, _, _)| *ts == *commit)
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
                // Too late to prepare or reprepare.
                OccPrepareResult::TooLate
            } else {
                self.inner
                    .prepare(*transaction_id, transaction.clone(), *commit, false)
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
                commit,
                ..
            } => {
                if matches!(res, CR::Prepare(OccPrepareResult::Ok)) && let Some((ts, _, finalized)) = self.inner.prepared.get_mut(transaction_id) && *commit == *ts {
                    eprintln!("confirming prepare {transaction_id:?} at {commit:?}");
                    *finalized = true;
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
                } => {
                    // Backup coordinator prepares don't change state.
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
                            eprintln!("syncing successful {op_id:?} prepare for {transaction_id:?} at {commit:?} (had {:?})", self.inner.prepared.get(transaction_id));
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
            let result = match request {
                CO::Prepare {
                    transaction_id,
                    commit,
                    ..
                } => {
                    let result = if matches!(reply, CR::Prepare(OccPrepareResult::Ok)) {
                        // Possibly successful fast quorum.
                        self.exec_consensus(request)
                    } else {
                        reply.clone()
                    };

                    if &result == reply {
                        eprintln!("merge preserving {op_id:?} {transaction_id:?} result {result:?} at {commit:?}");
                    } else {
                        eprintln!("merge changed {op_id:?} {transaction_id:?} at {commit:?} from {reply:?} to {result:?}");
                    }

                    self.finalize_consensus(request, &result);
                    result
                }
                CO::RaiseMinPrepareTime { time } => {
                    let received = if let CR::RaiseMinPrepareTime { time } = reply {
                        *time
                    } else {
                        debug_assert!(false);
                        0
                    };

                    if received >= *time {
                        // Possibly successful fast quorum.
                        let mut result = self.exec_consensus(request);
                        if let CR::RaiseMinPrepareTime { time: new_time } = &mut result {
                            // Don't grant time in excess of the requested time,
                            // which should preserve semantics better if reordered.
                            *new_time = (*new_time).min(*time);
                        }
                        result
                    } else {
                        // Preserve unsuccessful result.
                        reply.clone()
                    }
                }
            };
            ret.insert(*op_id, result);
        }

        // Leader is consistent with a quorum so can decide consensus
        // results.
        for (op_id, request, _) in &u {
            let result = self.exec_consensus(request);
            eprintln!("merge choosing {result:?} for {op_id:?}");
            ret.insert(*op_id, result);
        }

        ret
    }

    fn tick<T: Transport<Self>>(&mut self, membership: &IrMembership<T::Address>, transport: &T) {
        eprintln!(
            "there are {} prepared transactions",
            self.inner.prepared.len()
        );
        let threshold: u64 = transport.time_offset(-500);
        if let Some((transaction_id, (commit, transaction, _))) =
            self.inner.prepared.iter().min_by_key(|(_, (c, _, _))| *c)
        {
            if commit.time > threshold {
                // Allow the client to finish on its own.
                return;
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
