use crate::IrClientId;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Debug, Clone)]
pub(crate) struct Transaction<K, V, TS> {
    pub(crate) read_set: HashMap<K, TS>,
    pub(crate) write_set: HashMap<K, Option<V>>,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct Id {
    pub(crate) client_id: IrClientId,
    pub(crate) number: u64,
}

impl Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Txn({}, {:?})", self.client_id.0, self.number)
    }
}

impl<K, V, TS> Default for Transaction<K, V, TS> {
    fn default() -> Self {
        Self {
            read_set: Default::default(),
            write_set: Default::default(),
        }
    }
}

impl<K: Eq + Hash, V, TS> Transaction<K, V, TS> {
    pub(crate) fn add_read(&mut self, key: K, timestamp: TS) {
        match self.read_set.entry(key) {
            Entry::Vacant(vacant) => {
                vacant.insert(timestamp);
            }
            Entry::Occupied(occupied) => {
                panic!();
            }
        }
    }

    pub(crate) fn add_write(&mut self, key: K, value: Option<V>) {
        match self.write_set.entry(key) {
            Entry::Vacant(vacant) => {
                vacant.insert(value);
            }
            Entry::Occupied(occupied) => {
                panic!();
            }
        }
    }
}
