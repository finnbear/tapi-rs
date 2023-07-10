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
    pub fn size(&self) -> Size {
        Size(self.members.len() / 2)
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.members.len()
    }
}

impl<A: Eq + Copy> Membership<A> {
    /// # Panics
    ///
    /// If `members` is empty or contains duplicates.
    pub fn new(members: Vec<A>) -> Self {
        assert!(!members.is_empty());
        assert!(members
            .iter()
            .all(|a| members.iter().filter(|a2| a == *a2).count() == 1));
        Self { members }
    }

    pub fn get(&self, index: usize) -> Option<A> {
        self.members.get(index).cloned()
    }

    pub fn contains(&self, address: A) -> bool {
        self.members.contains(&address)
    }

    pub fn get_index(&self, address: A) -> Option<usize> {
        self.members.iter().position(|a| *a == address)
    }

    #[allow(clippy::type_complexity)]
    pub fn iter(&self) -> std::iter::Copied<std::slice::Iter<'_, A>> {
        self.into_iter()
    }
}

impl<A> IntoIterator for Membership<A> {
    type Item = A;
    type IntoIter = std::vec::IntoIter<A>;
    fn into_iter(self) -> Self::IntoIter {
        self.members.into_iter()
    }
}

impl<'a, A: Copy> IntoIterator for &'a Membership<A> {
    type Item = A;
    type IntoIter = std::iter::Copied<std::slice::Iter<'a, A>>;
    fn into_iter(self) -> Self::IntoIter {
        self.members.iter().copied()
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
