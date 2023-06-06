use super::{Message, OpId, Record};
use crate::{Transport, TransportMessage};
use std::collections::{HashMap, HashSet};

pub(crate) enum State {
    Normal,
    ViewChanging,
}

pub(crate) trait Upcalls {
    type Op: TransportMessage;
    type Result: TransportMessage;

    fn exec_inconsistent(&mut self, op: Self::Op);
    fn exec_consensus(&mut self, op: Self::Op);
    fn sync(&mut self, record: Record<Self::Op, Self::Result>);
    fn merge(
        &mut self,
        d: HashMap<OpId, Self::Op>,
        u: HashMap<OpId, Self::Op>,
    ) -> Record<Self::Op, Self::Result>;
}

pub(crate) struct Replica<T: Transport<Message = Message<O, R>>, O, R> {
    transport: T,
    state: State,
    record: Record<O, R>,
}

impl<T: Transport<Message = Message<O, R>>, O, R> Replica<T, O, R> {
    pub(crate) fn receive(&self, address: T::Address, message: Message<O, R>) {}
}
