use super::{
    record::Consistency, Confirm, FinalizeConsensus, FinalizeInconsistent, Membership, Message,
    OpId, ProposeConsensus, ProposeInconsistent, Record, RecordEntry, RecordState, ReplyConsensus,
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

    fn exec_inconsistent(&mut self, op: &Self::Op);
    fn exec_consensus(&mut self, op: &Self::Op) -> Self::Result;
    fn sync(&mut self, record: Record<Self::Op, Self::Result>);
    fn merge(
        &mut self,
        d: HashMap<OpId, Self::Op>,
        u: HashMap<OpId, Self::Op>,
    ) -> Record<Self::Op, Self::Result>;
}

pub(crate) struct Replica<U: Upcalls, T: Transport<Message = Message<U::Op, U::Result>>> {
    index: Index,
    membership: Membership<T>,
    upcalls: Mutex<U>,
    transport: T,
    state: State,
    record: Mutex<Record<U::Op, U::Result>>,
}

impl<U: Upcalls, T: Transport<Message = Message<U::Op, U::Result>>> Replica<U, T> {
    pub(crate) fn new(index: Index, membership: Membership<T>, upcalls: U, transport: T) -> Self {
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
            Message::ProposeConsensus(ProposeConsensus { op_id, op }) => {
                let mut record = self.record.lock().unwrap();
                let mut upcalls = self.upcalls.lock().unwrap();
                let result = upcalls.exec_consensus(&op);

                record.entries.entry(op_id).or_insert(RecordEntry {
                    op,
                    consistency: Consistency::Consistent,
                    result: Some(result.clone()),
                    state: RecordState::Tentative,
                });

                return Some(Message::ReplyConsensus(ReplyConsensus { op_id, result }));
            }
            Message::FinalizeInconsistent(FinalizeInconsistent { op_id }) => {
                let mut record = self.record.lock().unwrap();

                if let Some(entry) = record.entries.get_mut(&op_id) {
                    entry.state = RecordState::Finalized;
                    let mut upcalls = self.upcalls.lock().unwrap();
                    upcalls.exec_inconsistent(&entry.op);
                    drop(record);
                }
            }
            Message::FinalizeConsensus(FinalizeConsensus { op_id, result }) => {
                let mut record = self.record.lock().unwrap();
                if let Some(entry) = record.entries.get_mut(&op_id) {
                    entry.state = RecordState::Finalized;
                    entry.result = Some(result);
                    drop(record);
                    return Some(Message::Confirm(Confirm { op_id }));
                }
            }
            Message::ReplyInconsistent(_) | Message::ReplyConsensus(_) | Message::Confirm(_) => {
                println!("unexpected message")
            }
        }
        None
    }
}
