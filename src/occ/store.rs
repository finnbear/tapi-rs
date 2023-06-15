use super::{CoordinatorViewNumber, Timestamp, Transaction, TransactionId};
use crate::MvccStore;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Bound;
use std::{borrow::Borrow, collections::HashMap};

pub(crate) struct Store<K, V, TS> {
    linearizable: bool,
    inner: MvccStore<K, V, TS>,
    pub(crate) prepared: HashMap<TransactionId, (TS, Transaction<K, V, TS>, CoordinatorViewNumber)>,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub(crate) enum PrepareResult<TS: Timestamp> {
    /// The transaction is possible.
    Ok,
    /// There was a conflict that might be resolved by retrying prepare at a different timestamp.
    Retry { proposed: TS::Time },
    /// There was a conflict with a prepared transaction (which may later abort).
    Abstain,
    /// There was a conflict with a committed transaction.
    Fail,
    /// Used for coordinator recovery purposes.
    NoVote,
}

impl<K, V, TS> Store<K, V, TS> {
    pub(crate) fn new(linearizable: bool) -> Self {
        Self {
            linearizable,
            inner: Default::default(),
            prepared: Default::default(),
        }
    }
}

impl<K: Eq + Hash + Clone + Debug, V: Debug, TS: Timestamp> Store<K, V, TS> {
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

        println!("pr = {prepared_reads:?}, pw = {prepared_writes:?}");

        // Check for conflicts with the read set.
        for (key, read) in &transaction.read_set {
            if *read > commit {
                debug_assert!(false, "client picked too low commit timestamp for read");
                return PrepareResult::Retry {
                    proposed: read.time(),
                };
            }

            // If we don't have this key then no conflicts for read.
            let (beginning, end) = self.inner.get_range(key, *read);

            if beginning == *read {
                if let Some(end) = end && (self.linearizable || commit > end) {
                    // Read value is now invalid (not the latest version), so
                    // the prepare isn't linearizable and may not be serializable.
                    //
                    // In other words, the read conflicts with a later committed write.
                    return PrepareResult::Fail;
                }
            } else {
                // If we don't have this version then no conflicts for read.
            }

            // There may be a pending write that would invalidate the read version.
            if let Some(writes) = prepared_writes.get(key) {
                if self.linearizable
                    || writes
                        .range((Bound::Excluded(*read), Bound::Excluded(commit)))
                        .next()
                        .is_some()
                {
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
                        proposed: timestamp.time(),
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
                    return PrepareResult::Retry{proposed: last_read.time()};
                }
            }

            if self.linearizable && let Some(writes) = prepared_writes.get(key) {
                if let Some(write) = writes.lower_bound(Bound::Excluded(&commit)).key() {
                    // Write conflicts with later prepared write.
                    return PrepareResult::Retry { proposed: write.time() };
                }
            }

            if let Some(reads) = prepared_reads.get(key) {
                if reads.lower_bound(Bound::Excluded(&commit)).key().is_some() {
                    // Write conflicts with later prepared read.
                    return PrepareResult::Abstain;
                }
            }
        }

        self.prepared
            .insert(id, (commit, transaction, Default::default()));

        PrepareResult::Ok
    }

    pub(crate) fn commit(
        &mut self,
        id: TransactionId,
        transaction: Transaction<K, V, TS>,
        commit: TS,
    ) {
        for (key, read) in transaction.read_set {
            self.inner.commit_get(key.clone(), read, commit);
        }

        for (key, value) in transaction.write_set {
            self.inner.put(key, value, commit);
        }

        // Note: Transaction may not be in the prepared list of this particular replica, and that's okay.
        self.prepared.remove(&id);
    }

    pub(crate) fn abort(&mut self, id: TransactionId) {
        // Note: Transaction may not be in the prepared list of this particular replica, and that's okay.
        self.prepared.remove(&id);
    }

    pub(crate) fn put(&mut self, key: K, value: Option<V>, timestamp: TS) {
        self.inner.put(key, value, timestamp);
    }

    pub(crate) fn prepared_reads(&self) -> HashMap<&K, BTreeMap<TS, ()>> {
        let mut ret: HashMap<&K, BTreeMap<TS, ()>> = HashMap::default();
        for (_, (timestamp, transaction, _)) in &self.prepared {
            for key in transaction.read_set.keys() {
                ret.entry(key).or_default().insert(*timestamp, ());
            }
        }
        ret
    }

    pub(crate) fn prepared_writes(&self) -> HashMap<&K, BTreeMap<TS, ()>> {
        let mut ret: HashMap<&K, BTreeMap<TS, ()>> = HashMap::default();
        for (_, (timestamp, transaction, _)) in &self.prepared {
            for key in transaction.write_set.keys() {
                ret.entry(key).or_default().insert(*timestamp, ());
            }
        }
        ret
    }
}
