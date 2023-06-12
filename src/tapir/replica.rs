use std::{fmt::Debug, hash::Hash};

use super::{Reply, Request, Timestamp};
use crate::{IrReplicaUpcalls, OccStore};

struct Replica<K, V> {
    inner: OccStore<K, V, Timestamp>,
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
            Request::Get {
                transaction_id,
                key,
                timestamp,
            } => Reply::Get(
                if let Some(timestamp) = timestamp {
                    self.inner.get_at(&key, timestamp)
                } else {
                    self.inner.get(&key)
                }
                .map(|(ts, v)| (v.clone(), ts)),
            ),
            _ => unreachable!(),
        }
    }

    fn exec_inconsistent(&mut self, op: &Self::Op) {
        match op {
            Request::Commit { transaction_id } => {
                self.inner.commit(*transaction_id);
            }
            Request::Abort { transaction_id } => {
                self.inner.abort(*transaction_id);
            }
            _ => unreachable!(),
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
                self.inner
                    .prepare(*transaction_id, transaction.clone(), *commit),
            )
        } else {
            unreachable!();
        }
    }

    fn sync(&mut self, record: &crate::ir::Record<Self::Op, Self::Result>) {}

    fn merge(
        &mut self,
        d: std::collections::HashMap<
            crate::ir::OpId,
            Vec<crate::ir::RecordEntry<Self::Op, Self::Result>>,
        >,
        u: std::collections::HashMap<
            crate::ir::OpId,
            Vec<crate::ir::RecordEntry<Self::Op, Self::Result>>,
        >,
        majority_results_in_d: std::collections::HashMap<crate::ir::OpId, Self::Result>,
    ) -> std::collections::HashMap<crate::ir::OpId, Self::Result> {
        Default::default()
    }
}
