use super::ReplicaIndex;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Internally stores 'f'
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Size(usize);

/// Stores the address of replica group members.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Membership<A> {
    members: Vec<A>,
}

impl<A> Membership<A> {
    /// # Panics
    ///
    /// If `members` is empty.
    pub fn new(members: Vec<A>) -> Self {
        assert!(!members.is_empty());
        Self { members }
    }

    pub fn size(&self) -> Size {
        Size(self.members.len() / 2)
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.members.len()
    }
}

impl<A: Eq + Copy> Membership<A> {
    pub fn get(&self, index: ReplicaIndex) -> Option<A> {
        self.members.get(index.0).cloned()
    }

    pub fn get_index(&self, address: A) -> Option<ReplicaIndex> {
        self.members
            .iter()
            .position(|a| *a == address)
            .map(ReplicaIndex)
    }

    #[allow(clippy::type_complexity)]
    pub fn iter(
        &self,
    ) -> std::iter::Map<
        std::iter::Enumerate<std::slice::Iter<'_, A>>,
        for<'a> fn((usize, &'a A)) -> (ReplicaIndex, A),
    > {
        self.into_iter()
    }
}

impl<A> IntoIterator for Membership<A> {
    type Item = (ReplicaIndex, A);
    type IntoIter =
        std::iter::Map<std::iter::Enumerate<std::vec::IntoIter<A>>, fn((usize, A)) -> Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.members
            .into_iter()
            .enumerate()
            .map(|(i, a)| (ReplicaIndex(i), a))
    }
}

impl<'a, A: Copy> IntoIterator for &'a Membership<A> {
    type Item = (ReplicaIndex, A);
    type IntoIter = std::iter::Map<
        std::iter::Enumerate<std::slice::Iter<'a, A>>,
        for<'b> fn((usize, &'b A)) -> Self::Item,
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.members
            .iter()
            .enumerate()
            .map(|(i, a)| (ReplicaIndex(i), *a))
    }
}

impl Size {
    /// One node fewer than a majority.
    ///
    /// With an odd number of replicas, this is the maximum
    /// number of nodes that can fail while preserving liveness.
    ///
    /// In a replica group of size 3, this is 1.
    pub fn f(&self) -> usize {
        self.0
    }

    /// A majority of nodes.
    ///
    /// In a replica group of size 3, this is 2.
    pub fn f_plus_one(&self) -> usize {
        self.f() + 1
    }

    /// Minimum number of nodes that guarantees a majority of
    /// all possible majorities of nodes.
    ///
    /// In a replica group of size 3, this is 3.
    pub fn three_over_two_f_plus_one(&self) -> usize {
        (self.f() * 3).div_ceil(2) + 1
    }

    /// In a replica group of size 3, this is 2.
    pub fn f_over_two_plus_one(&self) -> usize {
        self.f().div_ceil(2) + 1
    }
}
