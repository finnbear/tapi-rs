use crate::IrClientId;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct Timestamp {
    pub(crate) client_id: IrClientId,
    pub(crate) time: u64,
}
