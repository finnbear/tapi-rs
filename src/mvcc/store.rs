use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap},
    hash::Hash,
    ops::Bound,
    time,
};

#[derive(Debug, Default, Clone)]
pub(crate) struct Store<K: Hash + Eq, V, TS: Ord + Eq + Copy> {
    inner: HashMap<K, BTreeMap<TS, (V, Option<TS>)>>,
}

impl<K: Hash + Eq, V, TS: Ord + Eq + Copy> Store<K, V, TS> {
    /// Get the latest version.
    fn get<Q: ?Sized + Hash + Eq>(&self, key: &Q) -> Option<(TS, &V)>
    where
        K: Borrow<Q>,
    {
        self.inner
            .get(key)
            .and_then(|versions| versions.last_key_value())
            .map(|(k, (v, _))| (*k, v))
    }

    /// Get the version valid at the timestamp.
    fn get_at<Q: ?Sized + Hash + Eq>(&self, key: &Q, timestamp: TS) -> Option<(TS, &V)>
    where
        K: Borrow<Q>,
    {
        self.inner.get(key).and_then(|versions| {
            versions
                .upper_bound(Bound::Included(&timestamp))
                .key_value()
                .map(|(t, (v, _))| (*t, v))
        })
    }

    /// Get range from a timestamp to the next timestamp (if any).
    fn get_range<Q: ?Sized + Hash + Eq>(&self, key: &Q, timestamp: TS) -> Option<(TS, Option<TS>)>
    where
        K: Borrow<Q>,
    {
        self.inner.get(key).and_then(|versions| {
            let mut cursor = versions.upper_bound(Bound::Included(&timestamp));
            if let Some((fk, _)) = cursor.key_value() {
                if let Some((lk, _)) = cursor.peek_next() {
                    Some((*fk, Some(*lk)))
                } else {
                    Some((*fk, None))
                }
            } else {
                None
            }
        })
    }

    /// Install a timestamped version for a key.
    fn put(&mut self, key: K, value: V, timestamp: TS) {
        self.inner
            .entry(key)
            .or_default()
            .insert(timestamp, (value, None));
    }

    /// Update the timestamp of the latest read transaction for the
    /// version of the key that the transaction read.
    fn commit_get<Q: ?Sized + Hash + Eq>(&mut self, key: &Q, read: TS, commit: TS)
    where
        K: Borrow<Q>,
    {
        if let Some(versions) = self.inner.get_mut(key) {
            if let Some((_, version)) = versions.upper_bound_mut(Bound::Included(&read)).value_mut()
            {
                *version = Some(if let Some(version) = *version {
                    version.max(commit)
                } else {
                    commit
                });
            }
        }
    }

    /// Get the last read timestamp of the last version.
    fn get_last_read<Q: ?Sized + Eq + Hash>(&self, key: &Q) -> Option<TS>
    where
        K: Borrow<Q>,
    {
        self.inner
            .get(key)
            .and_then(|entries| entries.last_key_value())
            .and_then(|(_, (_, ts))| *ts)
    }

    /// Get the last read timestamp of a specific version.
    fn get_last_read_at<Q: ?Sized + Eq + Hash>(&self, key: &Q, timestamp: TS) -> Option<TS>
    where
        K: Borrow<Q>,
    {
        self.inner
            .get(key)
            .and_then(|entries| entries.upper_bound(Bound::Included(&timestamp)).value())
            .and_then(|(_, ts)| *ts)
    }
}

#[cfg(test)]
mod tests {
    use super::Store;

    #[test]
    fn simple() {
        let mut store = Store::default();

        store.put("test1".to_owned(), "abc".to_owned(), 10);
        assert_eq!(store.get("test1"), Some((10, &String::from("abc"))));

        store.put("test1".to_owned(), "xyz".to_owned(), 11);
        assert_eq!(store.get("test1"), Some((11, &String::from("xyz"))));

        assert_eq!(store.get_at("test1", 10), Some((10, &String::from("abc"))));
    }
}
