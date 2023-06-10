use super::{
    record::Consistency, Confirm, DoViewChange, FinalizeConsensus, FinalizeInconsistent,
    Membership, Message, OpId, ProposeConsensus, ProposeInconsistent, Record, RecordEntry,
    RecordEntryState, ReplyConsensus, ReplyInconsistent, RequestUnlogged, StartView, View,
    ViewNumber,
};
use crate::{Transport, TransportMessage};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
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

    pub(crate) fn is_view_changing(&self) -> bool {
        matches!(self, Self::ViewChanging)
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
    outstanding_do_view_changes: HashMap<Index, DoViewChange<U::Op, U::Result>>,
}

impl<U: Upcalls, T: Transport<Message = Message<U::Op, U::Result>>> Replica<U, T> {
    const VIEW_CHANGE_INTERVAL: Duration = Duration::from_secs(2);

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
                    outstanding_do_view_changes: HashMap::new(),
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

                let mut sync = inner.sync.lock().unwrap();
                if Instant::now() >= sync.view_change_timeout {
                    if sync.status.is_normal() {
                        sync.status = Status::ViewChanging;
                    }
                    sync.view.number.0 += 1;

                    println!(
                        "{my_index:?} timeout sending do view change {}",
                        sync.view.number.0
                    );

                    Self::broadcast_do_view_change(my_index, &inner.transport, &mut *sync);
                }
            }
        });
    }

    fn broadcast_do_view_change(my_index: Index, transport: &T, sync: &mut Sync<U, T>) {
        sync.view_change_timeout = Instant::now() + Self::VIEW_CHANGE_INTERVAL;
        for (index, address) in &sync.view.membership {
            if index == my_index {
                continue;
            }
            transport.do_send(
                address,
                Message::DoViewChange(DoViewChange {
                    replica_index: my_index,
                    record: (index == sync.view.leader_index()).then(|| sync.record.clone()),
                    view_number: sync.view.number,
                    latest_normal_view: sync.lastest_normal_view,
                }),
            )
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
                                consistency: Consistency::Consensus,
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
            Message::DoViewChange(msg) => {
                let mut sync = self.inner.sync.lock().unwrap();
                if msg.view_number > sync.view.number
                    || (msg.view_number == sync.view.number && sync.status.is_view_changing())
                {
                    if msg.view_number > sync.view.number {
                        sync.view.number = msg.view_number;
                        if sync.status.is_normal() {
                            sync.status = Status::ViewChanging;
                        }
                        // TODO: Persist
                        Self::broadcast_do_view_change(
                            self.index,
                            &self.inner.transport,
                            &mut *sync,
                        );
                    }

                    if self.index == sync.view.leader_index() {
                        let msg_view_number = msg.view_number;
                        match sync.outstanding_do_view_changes.entry(msg.replica_index) {
                            Entry::Vacant(vacant) => {
                                vacant.insert(msg);
                            }
                            Entry::Occupied(mut occupied) => {
                                if msg.view_number < occupied.get().view_number {
                                    return None;
                                }
                                occupied.insert(msg);
                            }
                        }

                        let threshold = sync.view.membership.size().f();
                        for do_view_change in sync.outstanding_do_view_changes.values() {
                            let matching = sync
                                .outstanding_do_view_changes
                                .values()
                                .filter(|other| other.view_number == do_view_change.view_number);

                            if matching.clone().count() >= threshold {
                                println!("DOING VIEW CHANGE");
                                {
                                    let latest_normal_view = sync.lastest_normal_view.max(
                                        matching
                                            .clone()
                                            .map(|r| r.latest_normal_view)
                                            .max()
                                            .unwrap(),
                                    );
                                    let mut latest_records = matching
                                        .clone()
                                        .filter(|r| r.latest_normal_view == latest_normal_view)
                                        .map(|r| r.record.as_ref().unwrap().clone())
                                        .collect::<Vec<_>>();
                                    if sync.lastest_normal_view == latest_normal_view {
                                        latest_records.push(sync.record.clone());
                                    }
                                    println!("have {} latest", latest_records.len());

                                    #[allow(non_snake_case)]
                                    let mut R = Record::default();
                                    let mut entries_by_opid =
                                        HashMap::<OpId, Vec<RecordEntry<U::Op, U::Result>>>::new();
                                    for r in latest_records {
                                        for (op_id, entry) in r.entries.clone() {
                                            if entry.consistency.is_inconsistent() {
                                                R.entries.entry(op_id).or_insert(entry);
                                            } else if entry.state.is_finalized() {
                                                R.entries.entry(op_id).or_insert(entry);
                                                entries_by_opid.remove(&op_id);
                                            } else {
                                                assert!(entry.consistency.is_consensus());
                                                assert!(entry.state.is_tentative());

                                                if !R.entries.contains_key(&op_id) {
                                                    entries_by_opid
                                                        .entry(op_id)
                                                        .or_default()
                                                        .push(entry);
                                                }
                                            }
                                        }
                                    }
                                }
                                sync.view_change_timeout =
                                    Instant::now() + Self::VIEW_CHANGE_INTERVAL;
                                sync.status = Status::Normal;
                                sync.view.number = msg_view_number;
                                sync.lastest_normal_view = msg_view_number;
                                // TODO: Persist
                                for (_, address) in &sync.view.membership {
                                    self.inner.transport.do_send(
                                        address,
                                        Message::StartView(StartView {
                                            record: sync.record.clone(),
                                            view_number: sync.view.number,
                                        }),
                                    );
                                }
                                break;
                            }
                        }
                    }
                }
            }
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
