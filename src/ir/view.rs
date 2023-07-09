use super::{Membership};
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct View<A> {
    pub membership: Membership<A>,
    pub number: Number,
}

impl<A: Eq + Copy> View<A> {
    pub fn leader(&self) -> A {
        self.membership
            .get((self.number.0 % self.membership.len() as u64) as usize)
            .unwrap()
    }
}
