use crate::{IrClientId, OccTimestamp};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Timestamp {
    pub time: u64,
    pub client_id: IrClientId,
}

impl Debug for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ts({}, {})", self.time, self.client_id.0)
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self {
            time: 0,
            client_id: IrClientId(0),
        }
    }
}

impl OccTimestamp for Timestamp {
    type Time = u64;

    fn time(&self) -> Self::Time {
        self.time
    }
}
