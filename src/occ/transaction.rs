use serde::{Deserialize, Serialize};

use crate::tapir::{Key, Value};
use crate::IrClientId;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction<K, V, TS> {
    pub read_set: HashMap<K, TS>,
    #[serde(bound(
        serialize = "K: Serialize, V: Serialize",
        deserialize = "K: Deserialize<'de> + Eq + Hash, V: Deserialize<'de>"
    ))]
    pub write_set: HashMap<K, Option<V>>,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Id {
    pub client_id: IrClientId,
    pub number: u64,
}

impl Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Txn({}, {:?})", self.client_id.0, self.number)
    }
}

impl<K: Key, V: Value, TS> Default for Transaction<K, V, TS> {
    fn default() -> Self {
        Self {
            read_set: Default::default(),
            write_set: Default::default(),
        }
    }
}

impl<K: Key, V: Value, TS> Transaction<K, V, TS> {
    pub fn add_read(&mut self, key: K, timestamp: TS) {
        match self.read_set.entry(key) {
            Entry::Vacant(vacant) => {
                vacant.insert(timestamp);
            }
            Entry::Occupied(occupied) => {
                panic!();
            }
        }
    }

    pub fn add_write(&mut self, key: K, value: Option<V>) {
        self.write_set.insert(key, value);
    }
}
