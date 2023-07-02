use std::fmt::Debug;

use crate::transport::Transport;

use super::ReplicaIndex;

/// Internally stores 'f'
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Size(usize);

/// Stores the address of replica group members.
#[derive(Clone)]
pub struct Membership<T: Transport> {
    members: Vec<T::Address>,
}

impl<T: Transport> Debug for Membership<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Membership")
            .field("members", &self.members)
            .finish()
    }
}

impl<T: Transport> Membership<T> {
    /// Must have an odd number of replicas.
    pub fn new(members: Vec<T::Address>) -> Self {
        assert_eq!(members.len() % 2, 1);
        Self { members }
    }

    pub fn get(&self, index: ReplicaIndex) -> Option<T::Address> {
        self.members.get(index.0).cloned()
    }

    pub fn size(&self) -> Size {
        Size((self.members.len() - 1) / 2)
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn get_index(&self, address: T::Address) -> Option<ReplicaIndex> {
        self.members
            .iter()
            .position(|a| *a == address)
            .map(ReplicaIndex)
    }

    #[allow(clippy::type_complexity)]
    pub fn iter(
        &self,
    ) -> std::iter::Map<
        std::iter::Enumerate<std::slice::Iter<'_, T::Address>>,
        for<'a> fn((usize, &'a T::Address)) -> (ReplicaIndex, T::Address),
    > {
        self.into_iter()
    }
}

impl<T: Transport> IntoIterator for Membership<T> {
    type Item = (ReplicaIndex, T::Address);
    type IntoIter = std::iter::Map<
        std::iter::Enumerate<std::vec::IntoIter<T::Address>>,
        fn((usize, T::Address)) -> Self::Item,
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.members
            .into_iter()
            .enumerate()
            .map(|(i, a)| (ReplicaIndex(i), a))
    }
}

impl<'a, T: Transport> IntoIterator for &'a Membership<T> {
    type Item = (ReplicaIndex, T::Address);
    type IntoIter = std::iter::Map<
        std::iter::Enumerate<std::slice::Iter<'a, T::Address>>,
        for<'b> fn((usize, &'b T::Address)) -> Self::Item,
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.members
            .iter()
            .enumerate()
            .map(|(i, a)| (ReplicaIndex(i), *a))
    }
}

impl Size {
    /// In a replica group of size 3, this is 1.
    pub fn f(&self) -> usize {
        self.0
    }

    /// In a replica group of size 3, this is 2.
    pub fn f_plus_one(&self) -> usize {
        self.f() + 1
    }

    /// In a replica group of size 3, this is 3.
    pub fn three_over_two_f_plus_one(&self) -> usize {
        (self.f() * 3).div_ceil(2) + 1
    }

    /// In a replica group of size 3, this is 2.
    pub fn f_over_two_plus_one(&self) -> usize {
        self.f().div_ceil(2) + 1
    }
}
