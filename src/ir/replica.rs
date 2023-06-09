use super::{
    record::Consistency, Confirm, DoViewChange, FinalizeConsensus, FinalizeInconsistent,
    Membership, Message, OpId, ProposeConsensus, ProposeInconsistent, Record, RecordEntry,
    RecordEntryState, ReplyConsensus, ReplyInconsistent, RequestUnlogged, StartView, View,
    ViewNumber,
};
use crate::{Transport, TransportMessage};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::{Arc, Mutex},
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

    fn exec_unlogged(&mut self, op: Self::Op) -> Self::Result;
    fn exec_inconsistent(&mut self, op: &Self::Op);
    fn exec_consensus(&mut self, op: &Self::Op) -> Self::Result;
    fn sync(&mut self, record: &Record<Self::Op, Self::Result>);
    fn merge(
        &mut self,
        d: HashMap<OpId, Self::Op>,
        u: HashMap<OpId, Self::Op>,
    ) -> Record<Self::Op, Self::Result>;
}

pub(crate) struct Replica<U: Upcalls, T: Transport<Message = Message<U::Op, U::Result>>> {
    index: Index,
    inner: Arc<Inner<U, T>>,
}

struct Inner<U: Upcalls, T: Transport<Message = Message<U::Op, U::Result>>> {
    transport: T,
    sync: Mutex<Sync<U, T>>,
}

struct Sync<U: Upcalls, T: Transport<Message = Message<U::Op, U::Result>>> {
    state: State,
    view: View<T>,
    lastest_normal_view: ViewNumber,
    upcalls: U,
    record: Record<U::Op, U::Result>,
}

impl<U: Upcalls, T: Transport<Message = Message<U::Op, U::Result>>> Replica<U, T> {
    pub(crate) fn new(index: Index, membership: Membership<T>, upcalls: U, transport: T) -> Self {
        Self {
            index,
            inner: Arc::new(Inner {
                transport,
                sync: Mutex::new(Sync {
                    state: State::Normal,
                    view: View {
                        membership,
                        number: ViewNumber(0),
                    },
                    lastest_normal_view: ViewNumber(0),
                    upcalls,
                    record: Record::default(),
                }),
            }),
        }
    }

    pub(crate) fn receive(
        &self,
        address: T::Address,
        message: Message<U::Op, U::Result>,
    ) -> Option<Message<U::Op, U::Result>> {
        match message {
            Message::RequestUnlogged(RequestUnlogged { op }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                if sync.state.is_normal() {
                    let result = sync.upcalls.exec_unlogged(op);
                    return Some(Message::ReplyUnlogged(super::ReplyUnlogged { result }));
                }
            }
            Message::ProposeInconsistent(ProposeInconsistent { op_id, op }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                let sync = &mut *sync;
                if sync.state.is_normal() {
                    let entry = sync.record.entries.entry(op_id).or_insert(RecordEntry {
                        op,
                        consistency: Consistency::Inconsistent,
                        result: None,
                        state: RecordEntryState::Tentative,
                    });

                    return Some(Message::ReplyInconsistent(ReplyInconsistent {
                        op_id,
                        view_number: sync.view.number,
                        state: entry.state,
                    }));
                }
            }
            Message::ProposeConsensus(ProposeConsensus { op_id, op }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                let sync = &mut *sync;
                if sync.state.is_normal() {
                    let (result, state) = match sync.record.entries.entry(op_id) {
                        Entry::Occupied(entry) => {
                            let entry = entry.get();
                            (entry.result.clone(), entry.state)
                        }
                        Entry::Vacant(vacant) => {
                            let entry = vacant.insert(RecordEntry {
                                result: Some(sync.upcalls.exec_consensus(&op)),
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
                            view_number: sync.view.number,
                            result,
                            state,
                        }));
                    }
                }
            }
            Message::FinalizeInconsistent(FinalizeInconsistent { op_id }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                let sync = &mut *sync;
                if sync.state.is_normal() {
                    if let Some(entry) = sync.record.entries.get_mut(&op_id) {
                        entry.state = RecordEntryState::Finalized;
                        sync.upcalls.exec_inconsistent(&entry.op);
                    }
                }
            }
            Message::FinalizeConsensus(FinalizeConsensus { op_id, result }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                if sync.state.is_normal() {
                    if let Some(entry) = sync.record.entries.get_mut(&op_id) {
                        entry.state = RecordEntryState::Finalized;
                        entry.result = Some(result);
                        return Some(Message::Confirm(Confirm {
                            op_id,
                            view_number: sync.view.number,
                        }));
                    }
                }
            }
            Message::DoViewChange(DoViewChange {
                record,
                view_number,
                latest_normal_view,
            }) => {}
            Message::StartView(StartView {
                record: new_record,
                view_number,
            }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                let sync = &mut *sync;
                if view_number > sync.view.number
                    || (view_number == sync.view.number || !sync.state.is_normal())
                {
                    sync.record = new_record;
                    sync.upcalls.sync(&sync.record);
                    sync.state = State::Normal;
                    sync.view.number = view_number;
                    sync.lastest_normal_view = view_number;
                    // TODO: Persist view info.
                }
            }
            _ => {
                println!("unexpected message");
            }
        }
        None
    }
}
