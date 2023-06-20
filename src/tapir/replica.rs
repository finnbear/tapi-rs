use super::{Key, Timestamp, Value, CO, CR, IO, UO, UR};
use crate::{
    IrOpId, IrRecord, IrReplicaUpcalls, OccPrepareResult, OccStore, OccTransaction,
    OccTransactionId,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, hash::Hash};

#[derive(Serialize, Deserialize)]
pub struct Replica<K, V> {
    #[serde(bound(deserialize = "K: Deserialize<'de> + Hash + Eq, V: Deserialize<'de>"))]
    inner: OccStore<K, V, Timestamp>,
    /// Stores the commit timestamp, read/write sets, and commit status (true if committed) for
    /// all known committed and aborted transactions.
    #[serde(bound(deserialize = "K: Deserialize<'de> + Hash + Eq, V: Deserialize<'de>"))]
    transaction_log: HashMap<OccTransactionId, (Timestamp, OccTransaction<K, V, Timestamp>, bool)>,
    no_vote_list: HashMap<OccTransactionId, Timestamp>,
}

impl<K: Key, V: Value> Replica<K, V> {
    pub fn new(linearizable: bool) -> Self {
        Self {
            inner: OccStore::new(linearizable),
            transaction_log: HashMap::new(),
            no_vote_list: HashMap::new(),
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
            } => CR::Prepare(
                if let Some((_, _, commit)) = self.transaction_log.get(transaction_id) {
                    if *commit {
                        OccPrepareResult::Ok
                    } else {
                        OccPrepareResult::Fail
                    }
                } else {
                    self.inner
                        .prepare(*transaction_id, transaction.clone(), *commit)
                },
            ),
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
                            self.inner
                                .add_prepared(*transaction_id, transaction.clone(), *commit);
                        }
                    } else if self.inner.remove_prepared(*transaction_id)
                        && matches!(entry.result, CR::Prepare(OccPrepareResult::NoVote))
                        && !self.transaction_log.contains_key(&transaction_id)
                    {
                        self.no_vote_list.insert(*transaction_id, *commit);
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
                    self.no_vote_list.remove(transaction_id);
                }
            }
        }

        for (op_id, (request, reply)) in &d {
            match request {
                CO::Prepare {
                    transaction_id,
                    transaction,
                    commit,
                } => {
                    let reply = ret.insert(
                        *op_id,
                        CR::Prepare(if self.no_vote_list.contains_key(transaction_id) {
                            OccPrepareResult::NoVote
                        } else if !self.transaction_log.contains_key(transaction_id)
                            && matches!(reply, CR::Prepare(OccPrepareResult::Ok))
                        {
                            self.inner
                                .prepare(*transaction_id, transaction.clone(), *commit)
                        } else {
                            let CR::Prepare(result) = reply;
                            *result
                        }),
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
                        CR::Prepare(if self.no_vote_list.contains_key(transaction_id) {
                            OccPrepareResult::NoVote
                        } else {
                            self.inner
                                .prepare(*transaction_id, transaction.clone(), *commit)
                        }),
                    );
                }
            }
        }

        ret
    }
}
