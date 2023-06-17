use super::OpId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{collections::HashMap, fmt::Debug};

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum State {
    Finalized,
    Tentative,
}

impl Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Finalized => "Fin",
            Self::Tentative => "Tnt",
        })
    }
}

impl State {
    pub fn is_tentative(&self) -> bool {
        matches!(self, Self::Tentative)
    }

    pub fn is_finalized(&self) -> bool {
        matches!(self, Self::Finalized)
    }
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum Consistency {
    Inconsistent,
    Consensus,
}

impl Debug for Consistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Inconsistent => "Inc",
            Self::Consensus => "Con",
        })
    }
}

impl Consistency {
    pub fn is_inconsistent(&self) -> bool {
        matches!(self, Self::Inconsistent)
    }

    pub fn is_consensus(&self) -> bool {
        matches!(self, Self::Consensus)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InconsistentEntry<O> {
    pub op: O,
    pub state: State,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusEntry<O, R> {
    pub op: O,
    pub result: R,
    pub state: State,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record<O, R> {
    #[serde(
        with = "vectorize",
        bound(serialize = "O: Serialize", deserialize = "O: Deserialize<'de>")
    )]
    pub inconsistent: HashMap<OpId, InconsistentEntry<O>>,
    #[serde(
        with = "vectorize",
        bound(
            serialize = "O: Serialize, R: Serialize",
            deserialize = "O: Deserialize<'de>, R: Deserialize<'de>"
        )
    )]
    pub consensus: HashMap<OpId, ConsensusEntry<O, R>>,
}

impl<O, R> Default for Record<O, R> {
    fn default() -> Self {
        Self {
            inconsistent: Default::default(),
            consensus: Default::default(),
        }
    }
}

mod vectorize {
    use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};
    use std::{collections::HashMap, fmt::format, hash::Hash, marker::PhantomData};

    pub fn serialize<S: Serializer, K: Serialize, V: Serialize>(
        m: &HashMap<K, V>,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        ser.collect_seq(m.iter())
    }

    pub fn deserialize<
        'de,
        D: Deserializer<'de>,
        K: Eq + Hash + Deserialize<'de>,
        V: Deserialize<'de>,
    >(
        de: D,
    ) -> Result<HashMap<K, V>, D::Error> {
        struct Unvectorize<K, V>(PhantomData<(K, V)>);

        impl<'de, K: Hash + Eq + Deserialize<'de>, V: Deserialize<'de>> Visitor<'de> for Unvectorize<K, V> {
            type Value = HashMap<K, V>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("expecting a sequence of keys and values")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut ret = HashMap::with_capacity(seq.size_hint().unwrap_or_default().min(64));
                while let Some((k, v)) = seq.next_element::<(K, V)>()? {
                    ret.insert(k, v);
                }
                Ok(ret)
            }
        }

        de.deserialize_seq(Unvectorize(PhantomData))
    }
}
