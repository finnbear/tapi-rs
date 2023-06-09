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
    time::{Duration, Instant},
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct Index(pub usize);

pub(crate) enum Status {
    Normal,
    ViewChanging,
    Recovering,
}

impl Status {
    pub(crate) fn is_normal(&self) -> bool {
        matches!(self, Self::Normal)
    }
}

pub(crate) trait Upcalls: Send + 'static {
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
    status: Status,
    view: View<T>,
    lastest_normal_view: ViewNumber,
    view_change_timeout: Instant,
    upcalls: U,
    record: Record<U::Op, U::Result>,
}

impl<U: Upcalls, T: Transport<Message = Message<U::Op, U::Result>>> Replica<U, T> {
    const VIEW_CHANGE_INTERVAL: Duration = Duration::from_millis(500);

    pub(crate) fn new(index: Index, membership: Membership<T>, upcalls: U, transport: T) -> Self {
        let ret = Self {
            index,
            inner: Arc::new(Inner {
                transport,
                sync: Mutex::new(Sync {
                    status: Status::Normal,
                    view: View {
                        membership,
                        number: ViewNumber(0),
                    },
                    lastest_normal_view: ViewNumber(0),
                    view_change_timeout: Instant::now() + Self::VIEW_CHANGE_INTERVAL,
                    upcalls,
                    record: Record::default(),
                }),
            }),
        };
        ret.tick();
        ret
    }
    fn tick(&self) {
        let my_index = self.index;
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;

                let now = Instant::now();
                let mut sync = inner.sync.lock().unwrap();
                if now >= sync.view_change_timeout {
                    if sync.status.is_normal() {
                        sync.status = Status::ViewChanging;
                    }
                    sync.view.number.0 += 1;
                    sync.view_change_timeout = now + Self::VIEW_CHANGE_INTERVAL;

                    for (index, address) in &sync.view.membership {
                        if index == my_index {
                            continue;
                        }
                        inner.transport.do_send(
                            address,
                            Message::DoViewChange(DoViewChange {
                                record: (index == sync.view.leader_index())
                                    .then(|| sync.record.clone()),
                                view_number: sync.view.number,
                                latest_normal_view: sync.lastest_normal_view,
                            }),
                        )
                    }
                }
            }
        });
    }

    pub(crate) fn receive(
        &self,
        address: T::Address,
        message: Message<U::Op, U::Result>,
    ) -> Option<Message<U::Op, U::Result>> {
        match message {
            Message::RequestUnlogged(RequestUnlogged { op }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                if sync.status.is_normal() {
                    let result = sync.upcalls.exec_unlogged(op);
                    return Some(Message::ReplyUnlogged(super::ReplyUnlogged { result }));
                }
            }
            Message::ProposeInconsistent(ProposeInconsistent { op_id, op }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                let sync = &mut *sync;
                if sync.status.is_normal() {
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
                if sync.status.is_normal() {
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
                if sync.status.is_normal() {
                    if let Some(entry) = sync.record.entries.get_mut(&op_id) {
                        entry.state = RecordEntryState::Finalized;
                        sync.upcalls.exec_inconsistent(&entry.op);
                    }
                }
            }
            Message::FinalizeConsensus(FinalizeConsensus { op_id, result }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                if sync.status.is_normal() {
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
                    || (view_number == sync.view.number || !sync.status.is_normal())
                {
                    sync.record = new_record;
                    sync.upcalls.sync(&sync.record);
                    sync.status = Status::Normal;
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
