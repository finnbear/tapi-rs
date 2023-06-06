use super::ClientId;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct Id {
    pub(crate) client_id: ClientId,
    pub(crate) number: u64,
}
