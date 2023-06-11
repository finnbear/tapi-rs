use super::{Membership, ReplicaIndex};
use crate::transport::Transport;
use std::fmt::Debug;

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) struct Number(pub(crate) u64);

impl Debug for Number {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "V({})", self.0)
    }
}
pub(crate) struct View<T: Transport> {
    pub(crate) membership: Membership<T>,
    pub(crate) number: Number,
}

impl<T: Transport> View<T> {
    pub(crate) fn leader_index(&self) -> ReplicaIndex {
        ReplicaIndex((self.number.0 % self.membership.len() as u64) as usize)
    }
}
