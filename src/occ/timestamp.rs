pub(crate) trait Timestamp: Ord + Copy + Default {
    type Time: Ord + Copy;

    fn time(&self) -> Self::Time;
}
