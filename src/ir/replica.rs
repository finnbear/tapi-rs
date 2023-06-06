use super::{
    record::Consistency, FinalizeInconsistent, Message, OpId, ProposeInconsistent, Record,
    RecordEntry, RecordState, ReplyInconsistent,
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

    fn exec_inconsistent(&mut self, op: &Self::Op);
    fn exec_consensus(&mut self, op: &Self::Op);
    fn sync(&mut self, record: Record<Self::Op, Self::Result>);
    fn merge(
        &mut self,
        d: HashMap<OpId, Self::Op>,
        u: HashMap<OpId, Self::Op>,
    ) -> Record<Self::Op, Self::Result>;
}

pub(crate) struct Replica<U: Upcalls, T: Transport<Message = Message<U::Op, U::Result>>> {
    index: Index,
    membership: HashMap<Index, T::Address>,
    upcalls: Mutex<U>,
    transport: T,
    state: State,
    record: Mutex<Record<U::Op, U::Result>>,
}

impl<U: Upcalls, T: Transport<Message = Message<U::Op, U::Result>>> Replica<U, T> {
    pub(crate) fn new(
        index: Index,
        membership: HashMap<Index, T::Address>,
        upcalls: U,
        transport: T,
    ) -> Self {
        Self {
            index,
            upcalls: Mutex::new(upcalls),
            transport,
            membership,
            state: State::Normal,
            record: Default::default(),
        }
    }

    pub(crate) fn receive(
        &self,
        address: T::Address,
        message: Message<U::Op, U::Result>,
    ) -> Option<Message<U::Op, U::Result>> {
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
            Message::FinalizeInconsistent(FinalizeInconsistent { op_id }) => {
                let mut record = self.record.lock().unwrap();

                if let Some(entry) = record.entries.get_mut(&op_id) {
                    entry.state = RecordState::Finalized;
                    let mut upcalls = self.upcalls.lock().unwrap();
                    upcalls.exec_inconsistent(&entry.op);
                    drop(record);
                }
            }
            Message::FinalizeConsensus(_) => todo!(),
            Message::ReplyInconsistent(_) | Message::ReplyConsensus(_) | Message::Confirm(_) => {
                println!("unexpected message")
            }
        }
        None
    }
}
