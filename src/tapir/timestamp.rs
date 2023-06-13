use crate::{IrClientId, OccTimestamp};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) struct Timestamp {
    pub(crate) time: u64,
    pub(crate) client_id: IrClientId,
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
