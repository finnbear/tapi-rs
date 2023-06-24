use super::{Key, Timestamp, Value, CO, CR, IO, UO, UR};
use crate::util::vectorize;
use crate::{
    IrOpId, IrRecord, IrReplicaUpcalls, OccPrepareResult, OccStore, OccTransaction,
    OccTransactionId,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, hash::Hash};

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
                self.transaction_log
                    .insert(*transaction_id, (*commit, transaction.clone(), true));
                self.inner
                    .commit(*transaction_id, transaction.clone(), *commit);
            }
            IO::Abort {
                transaction_id,
                transaction,
                commit,
            } => {
                self.transaction_log
                    .insert(*transaction_id, (*commit, transaction.clone(), false));
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
                // Too late to prepare or reprepare.
                OccPrepareResult::TooLate
            } else {
                self.inner
                    .prepare(*transaction_id, transaction.clone(), *commit)
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
                } => {
                    if matches!(entry.result, CR::Prepare(OccPrepareResult::Ok)) {
                        if !self.inner.prepared.contains_key(transaction_id)
                            && !self.transaction_log.contains_key(transaction_id)
                        {
                            // Enough other replicas agreed to prepare
                            // the transaction so it must be okay.
                            self.inner
                                .add_prepared(*transaction_id, transaction.clone(), *commit);
                        }
                    } else {
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
                    self.transaction_log
                        .insert(*transaction_id, (*commit, transaction.clone(), true));
                }
                IO::Abort {
                    transaction_id,
                    transaction,
                    commit,
                } => {
                    self.inner.abort(*transaction_id);
                    self.transaction_log
                        .insert(*transaction_id, (*commit, transaction.clone(), false));
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
                } => {
                    ret.insert(
                        *op_id,
                        CR::Prepare(
                            if commit.time < self.min_prepare_time
                                || self
                                    .inner
                                    .prepared
                                    .get(transaction_id)
                                    .map(|(c, _, _)| c.time < self.min_prepare_time)
                                    .unwrap_or(false)
                            {
                                OccPrepareResult::TooLate
                            } else if !self.transaction_log.contains_key(transaction_id)
                                && matches!(reply, CR::Prepare(OccPrepareResult::Ok))
                            {
                                self.inner
                                    .prepare(*transaction_id, transaction.clone(), *commit)
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
                                .map(|(c, _, _)| c.time < self.min_prepare_time)
                                .unwrap_or(false)
                        {
                            OccPrepareResult::TooLate
                        } else {
                            self.inner
                                .prepare(*transaction_id, transaction.clone(), *commit)
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
}
