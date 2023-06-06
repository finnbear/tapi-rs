use super::{
    record::Consistency, Confirm, FinalizeConsensus, FinalizeInconsistent, Membership, Message,
    OpId, ProposeConsensus, ProposeInconsistent, Record, RecordEntry, RecordEntryState,
    ReplyConsensus, ReplyInconsistent, View, ViewNumber,
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
    Recovering,
}

impl State {
    pub(crate) fn is_normal(&self) -> bool {
        matches!(self, Self::Normal)
    }
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
    view: View<T>,
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
            view: View {
                membership,
                number: ViewNumber(0),
            },
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
            Message::ProposeInconsistent(ProposeInconsistent { op_id, op })
                if self.state.is_normal() =>
            {
                let mut record = self.record.lock().unwrap();

                let entry = record.entries.entry(op_id).or_insert(RecordEntry {
                    op,
                    consistency: Consistency::Inconsistent,
                    result: None,
                    state: RecordEntryState::Tentative,
                });

                return Some(Message::ReplyInconsistent(ReplyInconsistent {
                    op_id,
                    view_number: self.view.number,
                    state: entry.state,
                }));
            }
            Message::ProposeConsensus(ProposeConsensus { op_id, op }) if self.state.is_normal() => {
                let mut record = self.record.lock().unwrap();

                let (result, state) = match record.entries.entry(op_id) {
                    Entry::Occupied(entry) => {
                        let entry = entry.get();
                        (entry.result.clone(), entry.state)
                    }
                    Entry::Vacant(vacant) => {
                        let mut upcalls = self.upcalls.lock().unwrap();
                        let entry = vacant.insert(RecordEntry {
                            result: Some(upcalls.exec_consensus(&op)),
                            op,
                            consistency: Consistency::Consistent,
                            state: RecordEntryState::Tentative,
                        });
                        (entry.result.clone(), entry.state)
                    }
                };

                if let Some(result) = result {
                    return Some(Message::ReplyConsensus(ReplyConsensus {
                        op_id,
                        view_number: self.view.number,
                        result,
                        state,
                    }));
                }
            }
            Message::FinalizeInconsistent(FinalizeInconsistent { op_id })
                if self.state.is_normal() =>
            {
                let mut record = self.record.lock().unwrap();

                if let Some(entry) = record.entries.get_mut(&op_id) {
                    entry.state = RecordEntryState::Finalized;
                    let mut upcalls = self.upcalls.lock().unwrap();
                    upcalls.exec_inconsistent(&entry.op);
                    drop(record);
                }
            }
            Message::FinalizeConsensus(FinalizeConsensus { op_id, result })
                if self.state.is_normal() =>
            {
                let mut record = self.record.lock().unwrap();
                if let Some(entry) = record.entries.get_mut(&op_id) {
                    entry.state = RecordEntryState::Finalized;
                    entry.result = Some(result);
                    drop(record);
                    return Some(Message::Confirm(Confirm {
                        op_id,
                        view_number: self.view.number,
                    }));
                }
            }
            Message::ReplyInconsistent(_) | Message::ReplyConsensus(_) | Message::Confirm(_) => {
                println!("unexpected message")
            }
            _ => {
                if self.state.is_normal() {
                    println!("unexpected message");
                }
            }
        }
        None
    }
}
