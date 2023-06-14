use std::fmt::Debug;

pub(crate) trait Timestamp: Ord + Copy + Default + Debug {
    type Time: Ord + Copy;

    fn time(&self) -> Self::Time;
}
