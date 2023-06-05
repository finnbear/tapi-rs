use std::collections::{HashMap, HashSet};

use crate::transport::Transport;

use super::{OpId, Record};

pub(crate) enum State {
    Normal,
    ViewChanging,
}

pub(crate) struct Replica<T: Transport, O, R> {
    transport: T,
    state: State,
    record: HashMap<OpId, Record<O, R>>,
}
