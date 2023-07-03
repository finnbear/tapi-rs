use super::{MembershipSize, ReplicaIndex};
use crate::transport::Transport;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Number(pub u64);

impl Number {
    pub fn is_recent_relative_to(self, other: Self) -> bool {
        other.0.saturating_sub(self.0) <= 3
    }
}

impl Debug for Number {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "V({})", self.0)
    }
}
pub struct View {
    pub membership: MembershipSize,
    pub number: Number,
}

impl View {
    pub fn leader_index(&self) -> ReplicaIndex {
        ReplicaIndex((self.number.0 % self.membership.count() as u64) as usize)
    }
}
