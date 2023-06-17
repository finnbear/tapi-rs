use std::{collections::HashMap, fmt::Debug, hash::Hash};

use super::{Reply, Request, Timestamp};
use crate::{
    IrOpId, IrRecord, IrReplicaUpcalls, OccPrepareResult, OccStore, OccTransaction,
    OccTransactionId,
};

pub struct Replica<K: Hash + Eq, V> {
    inner: OccStore<K, V, Timestamp>,
    /// Stores the commit timestamp, read/write sets, and commit status (true if committed) for
    /// all known committed and aborted transactions.
    transaction_log: HashMap<OccTransactionId, (Timestamp, OccTransaction<K, V, Timestamp>, bool)>,
    no_vote_list: HashMap<OccTransactionId, Timestamp>,
}

impl<K: Hash + Eq, V> Replica<K, V> {
    pub fn new(linearizable: bool) -> Self {
        Self {
            inner: OccStore::new(linearizable),
            transaction_log: HashMap::new(),
            no_vote_list: HashMap::new(),
        }
    }
}

impl<
        K: Eq + Hash + Clone + Debug + Send + 'static,
        V: Send + Clone + PartialEq + Debug + 'static,
    > IrReplicaUpcalls for Replica<K, V>
{
    type Op = Request<K, V>;
    type Result = Reply<V>;

    fn exec_unlogged(&mut self, op: Self::Op) -> Self::Result {
        match op {
            Request::Get { key, timestamp } => {
                let (v, ts) = if let Some(timestamp) = timestamp {
                    self.inner.get_at(&key, timestamp)
                } else {
                    self.inner.get(&key)
                };
                Reply::Get(v.cloned(), ts)
            }
            _ => unreachable!(),
        }
    }

    fn exec_inconsistent(&mut self, op: &Self::Op) {
        match op {
            Request::Commit {
                transaction_id,
                transaction,
                commit,
            } => {
                self.transaction_log
                    .insert(*transaction_id, (*commit, transaction.clone(), true));
                self.inner
                    .commit(*transaction_id, transaction.clone(), *commit);
            }
            Request::Abort {
                transaction_id,
                transaction,
                commit,
            } => {
                self.transaction_log
                    .insert(*transaction_id, (*commit, transaction.clone(), false));
                self.inner.abort(*transaction_id);
            }
            _ => unreachable!("unexpected {op:?}"),
        }
    }

    fn exec_consensus(&mut self, op: &Self::Op) -> Self::Result {
        if let Request::Prepare {
            transaction_id,
            transaction,
            commit,
        } = op
        {
            Reply::Prepare(
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
            )
        } else {
            unreachable!();
        }
    }

    fn sync(
        &mut self,
        local: &IrRecord<Self::Op, Self::Result>,
        leader: &IrRecord<Self::Op, Self::Result>,
    ) {
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
                Request::Prepare {
                    transaction_id,
                    transaction,
                    commit,
                } => {
                    if matches!(entry.result, Reply::Prepare(OccPrepareResult::Ok)) {
                        if !self.inner.prepared.contains_key(transaction_id)
                            && !self.transaction_log.contains_key(transaction_id)
                        {
                            self.inner
                                .add_prepared(*transaction_id, transaction.clone(), *commit);
                        }
                    } else if self.inner.remove_prepared(*transaction_id)
                        && matches!(entry.result, Reply::Prepare(OccPrepareResult::NoVote))
                        && !self.transaction_log.contains_key(&transaction_id)
                    {
                        self.no_vote_list.insert(*transaction_id, *commit);
                    }
                }
                Request::Commit {
                    transaction_id,
                    transaction,
                    commit,
                } => {
                    self.inner
                        .commit(*transaction_id, transaction.clone(), *commit);
                    self.transaction_log
                        .insert(*transaction_id, (*commit, transaction.clone(), true));
                }
                Request::Abort {
                    transaction_id,
                    transaction,
                    commit,
                } => {
                    self.inner.abort(*transaction_id);
                    self.transaction_log
                        .insert(*transaction_id, (*commit, transaction.clone(), false));
                }
                Request::Get { .. } => {
                    debug_assert!(false, "these are not replicated");
                }
            }
        }
        for (op_id, entry) in &leader.inconsistent {
            if local.inconsistent.contains_key(op_id) {
                // Record already in local state.
                continue;
            }

            match &entry.op {
                Request::Commit {
                    transaction_id,
                    transaction,
                    commit,
                } => {
                    self.inner
                        .commit(*transaction_id, transaction.clone(), *commit);
                    self.transaction_log
                        .insert(*transaction_id, (*commit, transaction.clone(), true));
                }
                Request::Abort {
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
        d: HashMap<IrOpId, (Self::Op, Self::Result)>,
        u: Vec<(IrOpId, Self::Op, Self::Result)>,
    ) -> HashMap<IrOpId, Self::Result> {
        let mut ret: HashMap<IrOpId, Self::Result> = HashMap::new();
        for (op_id, request) in u
            .iter()
            .map(|(op_id, op, _)| (op_id, op))
            .chain(d.iter().map(|(op_id, (op, _))| (op_id, op)))
        {
            match request {
                Request::Prepare {
                    transaction_id,
                    transaction,
                    ..
                }
                | Request::Commit {
                    transaction_id,
                    transaction,
                    ..
                }
                | Request::Abort {
                    transaction_id,
                    transaction,
                    ..
                } => {
                    self.inner.remove_prepared(*transaction_id);
                    self.no_vote_list.remove(transaction_id);
                }
                Request::Get { .. } => {
                    debug_assert!(false);
                }
            }
        }

        for (op_id, (request, reply)) in &d {
            match request {
                Request::Prepare {
                    transaction_id,
                    transaction,
                    commit,
                }
                | Request::Commit {
                    transaction_id,
                    transaction,
                    commit,
                }
                | Request::Abort {
                    transaction_id,
                    transaction,
                    commit,
                } => {
                    let reply = ret.insert(
                        *op_id,
                        Reply::Prepare(if self.no_vote_list.contains_key(transaction_id) {
                            OccPrepareResult::NoVote
                        } else if !self.transaction_log.contains_key(transaction_id)
                            && matches!(reply, Reply::Prepare(OccPrepareResult::Ok))
                        {
                            self.inner
                                .prepare(*transaction_id, transaction.clone(), *commit)
                        } else if let Reply::Prepare(result) = reply {
                            *result
                        } else {
                            unreachable!();
                        }),
                    );
                }
                Request::Get { .. } => {
                    debug_assert!(false);
                }
            }
        }

        for (op_id, request, _) in &u {
            match request {
                Request::Prepare {
                    transaction_id,
                    transaction,
                    commit,
                } => {
                    ret.insert(
                        *op_id,
                        Reply::Prepare(if self.no_vote_list.contains_key(transaction_id) {
                            OccPrepareResult::NoVote
                        } else {
                            self.inner
                                .prepare(*transaction_id, transaction.clone(), *commit)
                        }),
                    );
                }
                _ => {
                    debug_assert!(false);
                }
            }
        }

        ret
    }
}
