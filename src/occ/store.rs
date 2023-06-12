use super::Transaction;
use crate::MvccStore;
use std::hash::Hash;
use std::{borrow::Borrow, collections::HashMap};

pub(crate) struct Store<K, V, TS> {
    linearizable: bool,
    inner: MvccStore<K, V, TS>,
    prepared: HashMap<u64, (TS, Transaction<K, V, TS>)>,
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
}
