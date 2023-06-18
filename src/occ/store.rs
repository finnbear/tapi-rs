use serde::{Deserialize, Serialize};

use super::{CoordinatorViewNumber, Timestamp, Transaction, TransactionId};
use crate::tapir::{Key, Value};
use crate::MvccStore;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Bound;
use std::{borrow::Borrow, collections::HashMap};

pub struct Store<K: Key, V: Value, TS> {
    linearizable: bool,
    inner: MvccStore<K, V, TS>,
    pub prepared: HashMap<TransactionId, (TS, Transaction<K, V, TS>, CoordinatorViewNumber)>,
    // Cache.
    prepared_reads: HashMap<K, BTreeMap<TS, ()>>,
    // Cache.
    prepared_writes: HashMap<K, BTreeMap<TS, ()>>,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum PrepareResult<TS: Timestamp> {
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

impl<K: Key, V: Value, TS> Store<K, V, TS> {
    pub fn new(linearizable: bool) -> Self {
        Self {
            linearizable,
            inner: Default::default(),
            prepared: Default::default(),
            prepared_reads: Default::default(),
            prepared_writes: Default::default(),
        }
    }
}

impl<K: Key, V: Value, TS: Timestamp> Store<K, V, TS> {
    pub fn get<Q: ?Sized + Eq + Hash>(&self, key: &Q) -> (Option<&V>, TS)
    where
        K: Borrow<Q>,
    {
        self.inner.get(key)
    }

    pub fn get_at<Q: ?Sized + Eq + Hash>(&self, key: &Q, timestamp: TS) -> (Option<&V>, TS)
    where
        K: Borrow<Q>,
    {
        self.inner.get_at(key, timestamp)
    }

    pub fn prepare(
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
                self.remove_prepared(id);
            }
        }

        // println!("pr = {prepared_reads:?}, pw = {prepared_writes:?}");

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
            if let Some(writes) = self.prepared_writes.get(key) {
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

            if self.linearizable && let Some(writes) = self.prepared_writes.get(key) {
                if let Some(write) = writes.lower_bound(Bound::Excluded(&commit)).key() {
                    // Write conflicts with later prepared write.
                    return PrepareResult::Retry { proposed: write.time() };
                }
            }

            if let Some(reads) = self.prepared_reads.get(key) {
                if reads.lower_bound(Bound::Excluded(&commit)).key().is_some() {
                    // Write conflicts with later prepared read.
                    return PrepareResult::Abstain;
                }
            }
        }

        self.add_prepared(id, transaction, commit);

        PrepareResult::Ok
    }

    pub fn commit(&mut self, id: TransactionId, transaction: Transaction<K, V, TS>, commit: TS) {
        for (key, read) in transaction.read_set {
            self.inner.commit_get(key.clone(), read, commit);
        }

        for (key, value) in transaction.write_set {
            self.inner.put(key, value, commit);
        }

        // Note: Transaction may not be in the prepared list of this particular replica, and that's okay.
        self.remove_prepared(id);
    }

    pub fn abort(&mut self, id: TransactionId) {
        // Note: Transaction may not be in the prepared list of this particular replica, and that's okay.
        self.remove_prepared(id);
    }

    pub fn put(&mut self, key: K, value: Option<V>, timestamp: TS) {
        self.inner.put(key, value, timestamp);
    }

    pub fn add_prepared(
        &mut self,
        id: TransactionId,
        transaction: Transaction<K, V, TS>,
        commit: TS,
    ) {
        for key in transaction.read_set.keys() {
            self.prepared_reads
                .entry(key.clone())
                .or_default()
                .insert(commit, ());
        }
        for key in transaction.write_set.keys() {
            self.prepared_writes
                .entry(key.clone())
                .or_default()
                .insert(commit, ());
        }

        if let Some((old_commit, transaction, _)) = self
            .prepared
            .insert(id, (commit, transaction, Default::default()))
        {
            if old_commit != commit {
                self.remove_prepared_inner(id, transaction, old_commit);
            }
        }
    }

    pub fn remove_prepared(&mut self, id: TransactionId) -> bool {
        if let Some((commit, transaction, _)) = self.prepared.remove(&id) {
            self.remove_prepared_inner(id, transaction, commit);
            true
        } else {
            false
        }
    }

    fn remove_prepared_inner(
        &mut self,
        id: TransactionId,
        transaction: Transaction<K, V, TS>,
        commit: TS,
    ) {
        for key in transaction.read_set.into_keys() {
            if let Entry::Occupied(mut occupied) = self.prepared_reads.entry(key) {
                occupied.get_mut().remove(&commit);
                if occupied.get().is_empty() {
                    occupied.remove();
                }
            }
        }
        for key in transaction.write_set.into_keys() {
            if let Entry::Occupied(mut occupied) = self.prepared_writes.entry(key) {
                occupied.get_mut().remove(&commit);
                if occupied.get().is_empty() {
                    occupied.remove();
                }
            }
        }
    }
}
