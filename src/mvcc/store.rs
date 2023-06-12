use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
};

#[derive(Debug, Clone)]
pub(crate) struct Store<K: Hash + Eq, V, TS: Ord + Eq> {
    store: HashMap<K, BTreeMap<TS, V>>,
    last_reads: HashMap<K, BTreeMap<TS, TS>>,
}

impl<K, V, TS: Ord + Eq> Store<K, V, TS> {
    fn get(&self, key: &K) -> (TS, &V) {}
    fn get_at(&self, key: &K, timestamp: TS) -> (TS, &V) {}
}
