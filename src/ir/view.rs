use crate::transport::Transport;

use super::Membership;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) struct Number(pub(crate) u64);
pub(crate) struct View<T: Transport> {
    pub(crate) membership: Membership<T>,
    pub(crate) number: Number,
}
