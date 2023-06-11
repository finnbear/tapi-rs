use super::ClientId;
use std::fmt::Debug;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct Id {
    pub(crate) client_id: ClientId,
    pub(crate) number: u64,
}

impl Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "O({}, {:?})", self.client_id.0, self.number)
    }
}
