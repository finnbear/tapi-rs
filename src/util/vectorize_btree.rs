use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};
use std::{collections::BTreeMap, marker::PhantomData};

pub fn serialize<S: Serializer, K: Serialize, V: Serialize>(
    m: &BTreeMap<K, V>,
    ser: S,
) -> Result<S::Ok, S::Error> {
    ser.collect_seq(m.iter())
}

pub fn deserialize<'de, D: Deserializer<'de>, K: Ord + Deserialize<'de>, V: Deserialize<'de>>(
    de: D,
) -> Result<BTreeMap<K, V>, D::Error> {
    struct Unvectorize<K, V>(PhantomData<(K, V)>);

    impl<'de, K: Ord + Deserialize<'de>, V: Deserialize<'de>> Visitor<'de> for Unvectorize<K, V> {
        type Value = BTreeMap<K, V>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("expecting a sequence of keys and values")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut ret = BTreeMap::new();
            while let Some((k, v)) = seq.next_element::<(K, V)>()? {
                ret.insert(k, v);
            }
            Ok(ret)
        }
    }

    de.deserialize_seq(Unvectorize(PhantomData))
}
