use crate::transport::Transport;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Copy, Clone, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub(crate) struct ViewNumber(pub(crate) u64);

impl Debug for ViewNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CV({})", self.0)
    }
}
