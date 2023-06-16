use serde::{Deserialize, Serialize};

use super::{Membership, ReplicaIndex};
use crate::transport::Transport;
use std::fmt::Debug;

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Number(pub u64);

impl Debug for Number {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "V({})", self.0)
    }
}
pub struct View<T: Transport> {
    pub membership: Membership<T>,
    pub number: Number,
}

impl<T: Transport> View<T> {
    pub fn leader_index(&self) -> ReplicaIndex {
        ReplicaIndex((self.number.0 % self.membership.len() as u64) as usize)
    }
}
