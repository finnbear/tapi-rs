use super::{Transaction, TransactionId};
use crate::MvccStore;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::hash::Hash;
use std::ops::Bound;
use std::{borrow::Borrow, collections::HashMap};

pub(crate) struct Store<K, V, TS> {
    linearizable: bool,
    inner: MvccStore<K, V, TS>,
    prepared: HashMap<TransactionId, (TS, Transaction<K, V, TS>)>,
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum PrepareResult<TS> {
    Ok,
    Retry { proposed: TS },
    Abstain,
    Fail,
}

impl<K: Eq + Hash + Clone, V, TS: Copy + Ord + Default> Store<K, V, TS> {
    pub(crate) fn new(linearizable: bool) -> Self {
        Self {
            linearizable,
            inner: Default::default(),
            prepared: Default::default(),
        }
    }

    pub(crate) fn get<Q: ?Sized + Eq + Hash>(&self, key: &Q) -> (Option<&V>, TS)
    where
        K: Borrow<Q>,
    {
        self.inner.get(key)
    }

    pub(crate) fn get_at<Q: ?Sized + Eq + Hash>(&self, key: &Q, timestamp: TS) -> (Option<&V>, TS)
    where
        K: Borrow<Q>,
    {
        self.inner.get_at(key, timestamp)
    }

    pub(crate) fn prepare(
        &mut self,
        id: TransactionId,
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
            let (beginning, end) = self.inner.get_range(key, *read);

            // If we don't have this version then no conflicts for read.
            if beginning != *read {
                continue;
            }

            if let Some(end) = end {
                // Value is now invalid (not the latest version).
                if self.linearizable || commit > end {
                    return PrepareResult::Fail;
                } else {
                    // There may be a pending write in the past.
                    if let Some(writes) = prepared_writes.get(key) {
                        if writes
                            .range((Bound::Excluded(*read), Bound::Excluded(commit)))
                            .next()
                            .is_some()
                        {
                            // Read conflicts with later prepared write.
                            return PrepareResult::Abstain;
                        }
                    }
                }
            } else {
                // The value is still valid (the latest version).
                if let Some(writes) = prepared_writes.get(key) && (self.linearizable || writes.lower_bound(Bound::Excluded(&commit)).key().is_some()) {
                    // Read conflicts with later prepared write.
                    return PrepareResult::Abstain;
                }
            }
        }

        // Check for conflicts with the write set.
        for (key, write) in &transaction.write_set {
            {
                let (_, timestamp) = self.inner.get(key);
                // If the last commited write is after the write...
                if self.linearizable && timestamp > commit {
                    // ...then the write isn't linearizable.
                    return PrepareResult::Retry {
                        proposed: timestamp,
                    };
                }

                // if last committed read is after the write...
                let last_read = if self.linearizable {
                    // Cannot write an old version.
                    self.inner.get_last_read(key)
                } else {
                    // Might be able to write an old version.
                    self.inner.get_last_read_at(key, commit)
                };

                if let Some(last_read) = last_read && last_read > commit {
                    // Write conflicts with a later committed read.
                    return PrepareResult::Retry{proposed: last_read};
                }
            }

            if self.linearizable && let Some(writes) = prepared_writes.get(key) {
                if let Some(write) = writes.lower_bound(Bound::Excluded(&commit)).key() {
                    // Write conflicts with later prepared write.
                    return PrepareResult::Retry { proposed: *write };
                }
            }

            if let Some(reads) = prepared_reads.get(key) {
                if reads.lower_bound(Bound::Excluded(&commit)).key().is_some() {
                    // Write conflicts with later prepared read.
                    return PrepareResult::Abstain;
                }
            }
        }

        self.prepared.insert(id, (commit, transaction));

        PrepareResult::Ok
    }

    pub(crate) fn commit(&mut self, id: TransactionId) {
        let Some((commit, transaction)) = self.prepared.remove(&id) else {
            return;
        };

        for (key, read) in transaction.read_set {
            self.inner.commit_get(key.clone(), read, commit);
        }

        for (key, value) in transaction.write_set {
            self.inner.put(key, value, commit);
        }
    }

    pub(crate) fn abort(&mut self, id: TransactionId) {
        self.prepared.remove(&id);
    }

    pub(crate) fn put(&mut self, key: K, value: Option<V>, timestamp: TS) {
        self.inner.put(key, value, timestamp);
    }

    pub(crate) fn prepared_reads(&self) -> HashMap<&K, BTreeMap<TS, ()>> {
        let mut ret: HashMap<&K, BTreeMap<TS, ()>> = HashMap::default();
        for (_, (timestamp, transaction)) in &self.prepared {
            for key in transaction.read_set.keys() {
                ret.entry(key).or_default().insert(*timestamp, ());
            }
        }
        ret
    }

    pub(crate) fn prepared_writes(&self) -> HashMap<&K, BTreeMap<TS, ()>> {
        let mut ret: HashMap<&K, BTreeMap<TS, ()>> = HashMap::default();
        for (_, (timestamp, transaction)) in &self.prepared {
            for key in transaction.write_set.keys() {
                ret.entry(key).or_default().insert(*timestamp, ());
            }
        }
        ret
    }
}
