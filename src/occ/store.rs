use super::{Timestamp, Transaction, TransactionId};
use crate::{
    tapir::{Key, Value},
    util::{vectorize, vectorize_btree},
    MvccStore,
};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    ops::{Bound, Deref, DerefMut},
};

#[derive(Serialize, Deserialize)]
pub struct Store<K, V, TS> {
    linearizable: bool,
    #[serde(bound(
        serialize = "K: Serialize, V: Serialize, TS: Serialize",
        deserialize = "K: Deserialize<'de> + Hash + Eq, V: Deserialize<'de>, TS: Deserialize<'de> + Ord"
    ))]
    inner: MvccStore<K, V, TS>,
    /// Transactions which may commit in the future (and whether they are undergoing
    /// coordinator recovery).
    #[serde(with = "vectorize")]
    pub prepared: HashMap<TransactionId, (TS, Transaction<K, V, TS>)>,
    // Cache.
    #[serde(with = "vectorize", bound(deserialize = "TS: Deserialize<'de> + Ord"))]
    prepared_reads: HashMap<K, TimestampSet<TS>>,
    // Cache.
    #[serde(with = "vectorize")]
    prepared_writes: HashMap<K, TimestampSet<TS>>,
}

#[derive(Serialize, Deserialize)]
struct TimestampSet<TS> {
    /// Use a map in order to use APIs sets don't have.
    #[serde(
        with = "vectorize_btree",
        bound(
            serialize = "TS: Serialize",
            deserialize = "TS: Deserialize<'de> + Ord"
        )
    )]
    inner: BTreeMap<TS, ()>,
}

impl<TS> Default for TimestampSet<TS> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<TS> Deref for TimestampSet<TS> {
    type Target = BTreeMap<TS, ()>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<TS> DerefMut for TimestampSet<TS> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
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
    /// It is too late to prepare with this commit timestamp.
    TooLate,
    /// The commit time is too old (would be or was already garbage collected).
    ///
    /// It isn't known whether such transactions were prepared, committed, or aborted.
    /// - Clients can safely hang i.e. while polling more replicas, or return an
    ///   indeterminate result.
    /// - Backup coordinators can safely give up (transaction guaranteed to have
    ///   committed or aborted already).
    /// - Merging replicas can safely self-destruct (TODO: is there a better option?)
    TooOld,
}

impl<TS: Timestamp> PrepareResult<TS> {
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }

    pub fn is_fail(&self) -> bool {
        matches!(self, Self::Fail)
    }
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
        dry_run: bool,
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

        if dry_run {
            PrepareResult::Retry {
                proposed: commit.time(),
            }
        } else {
            self.add_prepared(id, transaction, commit);
            PrepareResult::Ok
        }
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

        if let Some((old_commit, transaction)) = self.prepared.insert(id, (commit, transaction)) {
            if old_commit != commit {
                self.remove_prepared_inner(id, transaction, old_commit);
            }
        }
    }

    pub fn remove_prepared(&mut self, id: TransactionId) -> bool {
        if let Some((commit, transaction)) = self.prepared.remove(&id) {
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
