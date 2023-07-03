use super::ReplicaIndex;
use crate::transport::Transport;
use std::{fmt::Debug, iter::Map, ops::Range};

/// Internally stores 'f'
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Size(usize);

impl Size {
    pub fn new(n: usize) -> Self {
        assert_eq!(n % 2, 1, "replica group must have odd size");
        Self(n / 2)
    }

    /// Number of replicas.
    pub fn count(&self) -> usize {
        self.0 * 2 + 1
    }

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

impl IntoIterator for Size {
    type Item = ReplicaIndex;
    type IntoIter = Map<Range<usize>, fn(usize) -> ReplicaIndex>;

    fn into_iter(self) -> Self::IntoIter {
        (0..self.count()).map(ReplicaIndex)
    }
}

#[cfg(test)]
mod tests {
    use super::Size;

    #[test]
    fn size() {
        let one = Size::new(1);
        assert_eq!(one.count(), 1);
        assert_eq!(one.f(), 0);

        let three = Size::new(3);
        assert_eq!(three.count(), 3);
        assert_eq!(three.f(), 1);
        assert_eq!(three.f_plus_one(), 2);
        assert_eq!(three.three_over_two_f_plus_one(), 3);
        assert_eq!(three.f_over_two_plus_one(), 2);

        let five = Size::new(5);
        assert_eq!(five.count(), 5);
        assert_eq!(five.f(), 2);
    }
}
