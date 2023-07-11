use crate::{
    tapir::{Key, ShardNumber, Sharded, Value},
    util::vectorize,
    IrClientId,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction<K, V, TS> {
    #[serde(
        with = "vectorize",
        bound(serialize = "TS: Serialize", deserialize = "TS: Deserialize<'de>")
    )]
    pub read_set: HashMap<Sharded<K>, TS>,
    #[serde(
        with = "vectorize",
        bound(
            serialize = "K: Serialize, V: Serialize",
            deserialize = "K: Deserialize<'de> + Eq + Hash, V: Deserialize<'de>"
        )
    )]
    pub write_set: HashMap<Sharded<K>, Option<V>>,
}

impl<K: Eq + Hash, V: PartialEq, TS: PartialEq> PartialEq for Transaction<K, V, TS> {
    fn eq(&self, other: &Self) -> bool {
        self.read_set == other.read_set && self.write_set == other.write_set
    }
}

impl<K: Eq + Hash, V: Eq, TS: Eq> Eq for Transaction<K, V, TS> {}

impl<K, V, TS: Copy> Transaction<K, V, TS> {
    pub fn participants(&self) -> HashSet<ShardNumber> {
        self.read_set
            .iter()
            .map(|(k, _)| k.shard)
            .chain(self.write_set.iter().map(|(k, _)| k.shard))
            .collect()
    }

    pub fn shard_read_set(&self, shard: ShardNumber) -> impl Iterator<Item = (&K, TS)> + '_ {
        self.read_set
            .iter()
            .filter(move |(k, _)| k.shard == shard)
            .map(|(k, ts)| (&k.key, *ts))
    }

    pub fn shard_write_set(
        &self,
        shard: ShardNumber,
    ) -> impl Iterator<Item = (&K, &Option<V>)> + '_ {
        self.write_set
            .iter()
            .filter(move |(k, _)| k.shard == shard)
            .map(|(k, v)| (&k.key, v))
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

impl<K: Key, V: Value, TS: Ord> Transaction<K, V, TS> {
    pub fn add_read(&mut self, key: Sharded<K>, timestamp: TS) {
        match self.read_set.entry(key) {
            Entry::Vacant(vacant) => {
                vacant.insert(timestamp);
            }
            Entry::Occupied(_) => {
                panic!();
            }
        }
    }

    pub fn add_write(&mut self, key: Sharded<K>, value: Option<V>) {
        self.write_set.insert(key, value);
    }
}
