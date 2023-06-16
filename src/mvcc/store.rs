use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap},
    hash::Hash,
    ops::Bound,
    time,
};

#[derive(Debug, Clone)]
pub struct Store<K, V, TS> {
    /// For each timestamped version of a key, track the
    /// value (or tombstone) and the optional read timestamp.
    ///
    /// For all keys, there is an implicit version (TS::default() => (None, None)),
    /// in other words the key was nonexistent at the beginning of time.
    inner: HashMap<K, BTreeMap<TS, (Option<V>, Option<TS>)>>,
}

impl<K, V, TS> Default for Store<K, V, TS> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<K: Hash + Eq, V, TS: Ord + Eq + Copy + Default> Store<K, V, TS> {
    /// Get the latest version.
    pub fn get<Q: ?Sized + Hash + Eq>(&self, key: &Q) -> (Option<&V>, TS)
    where
        K: Borrow<Q>,
    {
        self.inner
            .get(key)
            .and_then(|versions| versions.last_key_value())
            .map(|(ts, (v, _))| (v.as_ref(), *ts))
            .unwrap_or_default()
    }

    /// Get the version valid at the timestamp.
    pub fn get_at<Q: ?Sized + Hash + Eq>(&self, key: &Q, timestamp: TS) -> (Option<&V>, TS)
    where
        K: Borrow<Q>,
    {
        self.inner
            .get(key)
            .and_then(|versions| {
                versions
                    .upper_bound(Bound::Included(&timestamp))
                    .key_value()
            })
            .map(|(ts, (v, _))| (v.as_ref(), *ts))
            .unwrap_or_default()
    }

    /// Get range from a timestamp to the next timestamp (if any).
    pub fn get_range<Q: ?Sized + Hash + Eq>(&self, key: &Q, timestamp: TS) -> (TS, Option<TS>)
    where
        K: Borrow<Q>,
    {
        self.inner
            .get(key)
            .map(|versions| {
                let mut cursor = versions.upper_bound(Bound::Included(&timestamp));
                if let Some((fk, _)) = cursor.key_value() {
                    if let Some((lk, _)) = cursor.peek_next() {
                        (*fk, Some(*lk))
                    } else {
                        (*fk, None)
                    }
                } else {
                    // Start at the implicit version and end at the first explict version, if any.
                    (TS::default(), versions.first_key_value().map(|(k, _)| *k))
                }
            })
            .unwrap_or_default()
    }

    /// Install a timestamped version for a key.
    pub fn put(&mut self, key: K, value: Option<V>, timestamp: TS) {
        debug_assert!(timestamp > TS::default());
        self.inner
            .entry(key)
            .or_default()
            .insert(timestamp, (value, None));
    }

    /// Update the timestamp of the latest read transaction for the
    /// version of the key that the transaction read.
    pub fn commit_get(&mut self, key: K, read: TS, commit: TS) {
        let versions = self.inner.entry(key).or_default();
        if let Some((_, version)) = versions.upper_bound_mut(Bound::Included(&read)).value_mut() {
            *version = Some(if let Some(version) = *version {
                version.max(commit)
            } else {
                commit
            });
        } else {
            // Make the implicit version explicit.
            versions.insert(TS::default(), (None, Some(commit)));
        }
    }

    /// Get the last read timestamp of the last version.
    pub fn get_last_read<Q: ?Sized + Eq + Hash>(&self, key: &Q) -> Option<TS>
    where
        K: Borrow<Q>,
    {
        self.inner
            .get(key)
            .and_then(|entries| entries.last_key_value())
            .and_then(|(_, (_, ts))| *ts)
    }

    /// Get the last read timestamp of a specific version.
    pub fn get_last_read_at<Q: ?Sized + Eq + Hash>(&self, key: &Q, timestamp: TS) -> Option<TS>
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
        assert_eq!(store.get("test1"), (None, 0));

        store.put("test1".to_owned(), Some("abc".to_owned()), 10);
        assert_eq!(store.get("test1"), (Some(&String::from("abc")), 10));

        store.put("test1".to_owned(), Some("xyz".to_owned()), 11);
        assert_eq!(store.get("test1"), (Some(&String::from("xyz")), 11));

        assert_eq!(store.get_at("test1", 10), (Some(&String::from("abc")), 10));
    }
}
