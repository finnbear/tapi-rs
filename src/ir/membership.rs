use crate::transport::Transport;

use super::ReplicaIndex;

/// Internally stores 'f'
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub(crate) struct Size(usize);

#[derive(Clone, Debug)]
pub(crate) struct Membership<T: Transport> {
    members: Vec<T::Address>,
}

impl<T: Transport> Membership<T> {
    pub(crate) fn new(members: Vec<T::Address>) -> Self {
        assert_ne!(members.len(), 0);
        assert_eq!(members.len() % 2, 1);
        Self { members }
    }

    pub(crate) fn get(&self, index: ReplicaIndex) -> Option<T::Address> {
        self.members.get(index.0).cloned()
    }

    pub(crate) fn size(&self) -> Size {
        Size((self.members.len() - 1) / 2)
    }

    pub(crate) fn len(&self) -> usize {
        self.members.len()
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn iter(
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
    pub(crate) fn f(&self) -> usize {
        self.0
    }

    pub(crate) fn f_plus_one(&self) -> usize {
        self.f() + 1
    }

    pub(crate) fn three_over_two_f_plus_one(&self) -> usize {
        (self.f() * 3).div_ceil(2) + 1
    }
}
