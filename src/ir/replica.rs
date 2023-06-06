use super::{
    record::Consistency, Message, OpId, ProposeInconsistent, Record, RecordEntry, RecordState,
    ReplyInconsistent,
};
use crate::{Transport, TransportMessage};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Mutex,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct Index(pub usize);

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
    index: Index,
    membership: HashMap<Index, T::Address>,
    transport: T,
    state: State,
    record: Mutex<Record<O, R>>,
}

impl<T: Transport<Message = Message<O, R>>, O, R> Replica<T, O, R> {
    pub(crate) fn new(index: Index, membership: HashMap<Index, T::Address>, transport: T) -> Self {
        Self {
            index,
            transport,
            membership,
            state: State::Normal,
            record: Default::default(),
        }
    }

    pub(crate) fn receive(
        &self,
        address: T::Address,
        message: Message<O, R>,
    ) -> Option<Message<O, R>> {
        match message {
            Message::ProposeInconsistent(ProposeInconsistent { op_id, op }) => {
                let mut record = self.record.lock().unwrap();

                record.entries.entry(op_id).or_insert(RecordEntry {
                    op,
                    consistency: Consistency::Inconsistent,
                    result: None,
                    state: RecordState::Tentative,
                });

                return Some(Message::ReplyInconsistent(ReplyInconsistent { op_id }));
            }
            Message::ProposeConsensus(_) => todo!(),
            Message::FinalizeInconsistent(_) => {
                // mark op as final.
            }
            Message::FinalizeConsensus(_) => todo!(),
            Message::ReplyInconsistent(_) | Message::ReplyConsensus(_) | Message::Confirm(_) => {
                println!("unexpected message")
            }
        }
        None
    }
}
