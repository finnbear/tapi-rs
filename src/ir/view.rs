use super::{Membership, ReplicaIndex};
use crate::transport::Transport;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) struct Number(pub(crate) u64);
pub(crate) struct View<T: Transport> {
    pub(crate) membership: Membership<T>,
    pub(crate) number: Number,
}

impl<T: Transport> View<T> {
    pub(crate) fn leader_index(&self) -> ReplicaIndex {
        ReplicaIndex((self.number.0 % self.membership.len() as u64) as usize)
    }
}
