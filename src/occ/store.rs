use super::Transaction;
use crate::MvccStore;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashSet};
use std::hash::Hash;
use std::{borrow::Borrow, collections::HashMap};

pub(crate) struct Store<K, V, TS> {
    linearizable: bool,
    inner: MvccStore<K, V, TS>,
    prepared: HashMap<u64, (TS, Transaction<K, V, TS>)>,
}

#[derive(Debug)]
pub(crate) enum PrepareResult<TS> {
    Ok,
    Retry { proposed: TS },
    Abstain,
    Fail,
}

impl<K: Eq + Hash, V, TS: Copy + Ord> Store<K, V, TS> {
    pub(crate) fn new(linearizable: bool) -> Self {
        Self {
            linearizable,
            inner: Default::default(),
            prepared: Default::default(),
        }
    }

    pub(crate) fn get<Q: ?Sized + Eq + Hash>(&self, key: &Q) -> Option<(TS, &V)>
    where
        K: Borrow<Q>,
    {
        self.inner.get(key)
    }

    pub(crate) fn get_at<Q: ?Sized + Eq + Hash>(&self, key: &Q, timestamp: TS) -> Option<(TS, &V)>
    where
        K: Borrow<Q>,
    {
        self.inner.get_at(key, timestamp)
    }

    pub(crate) fn prepare(
        &mut self,
        id: u64,
        transaction: Transaction<K, V, TS>,
        commit: TS,
    ) -> PrepareResult<TS> {
        if let Entry::Occupied(occupied) = self.prepared.entry(id) {
            if occupied.get().0 == commit {
                return PrepareResult::Ok;
            } else {
                // Run the checks again for a new timestamp.
                occupied.remove();
            }
        }

        let prepared_reads = self.prepared_reads();
        let prepared_writes = self.prepared_writes();

        // Check for conflicts with the read set.
        for (key, read) in &transaction.read_set {
            // If we don't have this key then no conflicts for read.
            let Some((beginning, end)) = self.inner.get_range(key, *read) else {
                continue;
            };
        }

        PrepareResult::Ok
    }

    pub(crate) fn commit(&mut self, id: u64) {
        let Some((commit, transaction)) = self.prepared.remove(&id) else {
            return;
        };

        for (key, read) in transaction.read_set {
            self.inner.commit_get(&key, read, commit);
        }

        for (key, value) in transaction.write_set {
            self.inner.put(key, value, commit);
        }
    }

    pub(crate) fn abort(&mut self, id: u64) {
        self.prepared.remove(&id);
    }

    pub(crate) fn put(&mut self, key: K, value: V, timestamp: TS) {
        self.inner.put(key, value, timestamp);
    }

    pub(crate) fn prepared_reads(&self) -> HashMap<&K, BTreeSet<TS>> {
        let mut ret: HashMap<&K, BTreeSet<TS>> = HashMap::default();
        for (_, (timestamp, transaction)) in &self.prepared {
            for key in transaction.read_set.keys() {
                ret.entry(key).or_default().insert(*timestamp);
            }
        }
        ret
    }

    pub(crate) fn prepared_writes(&self) -> HashMap<&K, BTreeSet<TS>> {
        let mut ret: HashMap<&K, BTreeSet<TS>> = HashMap::default();
        for (_, (timestamp, transaction)) in &self.prepared {
            for key in transaction.write_set.keys() {
                ret.entry(key).or_default().insert(*timestamp);
            }
        }
        ret
    }
}
