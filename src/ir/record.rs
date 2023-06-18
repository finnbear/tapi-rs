use super::{OpId, ReplicaUpcalls};
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
pub struct InconsistentEntry<IO> {
    pub op: IO,
    pub state: State,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusEntry<CO, CR> {
    pub op: CO,
    pub result: CR,
    pub state: State,
}

pub type Record<U> =
    RecordImpl<<U as ReplicaUpcalls>::IO, <U as ReplicaUpcalls>::CO, <U as ReplicaUpcalls>::CR>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordImpl<IO, CO, CR> {
    pub inconsistent: HashMap<OpId, InconsistentEntry<IO>>,
    pub consensus: HashMap<OpId, ConsensusEntry<CO, CR>>,
}

impl<IO, CO, CR> Default for RecordImpl<IO, CO, CR> {
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
