use serde::{Deserialize, Serialize};

use super::{
    message::ViewChangeAddendum, record::Consistency, Confirm, DoViewChange, FinalizeConsensus,
    FinalizeInconsistent, Membership, Message, OpId, ProposeConsensus, ProposeInconsistent, Record,
    RecordConsensusEntry, RecordEntryState, RecordInconsistentEntry, ReplyConsensus,
    ReplyInconsistent, ReplyUnlogged, RequestUnlogged, StartView, View, ViewNumber,
};
use crate::{Transport, TransportMessage};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    sync::{Arc, Mutex, MutexGuard, Weak},
    time::{Duration, Instant},
};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Index(pub usize);

impl Debug for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "R({})", self.0)
    }
}

#[derive(Debug)]
pub enum Status {
    Normal,
    ViewChanging,
    Recovering,
}

impl Status {
    pub fn is_normal(&self) -> bool {
        matches!(self, Self::Normal)
    }

    pub fn is_view_changing(&self) -> bool {
        matches!(self, Self::ViewChanging)
    }
}

pub trait Upcalls: Sized + Send + 'static {
    /// Unlogged operation.
    type UO: TransportMessage;
    /// Unlogged result.
    type UR: TransportMessage;
    /// Inconsistent operation.
    type IO: TransportMessage;
    /// Consensus operation.
    type CO: TransportMessage;
    /// Consensus result.
    type CR: TransportMessage + Eq + Hash;

    fn exec_unlogged(&mut self, op: Self::UO) -> Self::UR;
    fn exec_inconsistent(&mut self, op: &Self::IO);
    fn exec_consensus(&mut self, op: &Self::CO) -> Self::CR;
    fn sync(&mut self, local: &Record<Self>, leader: &Record<Self>);
    fn merge(
        &mut self,
        d: HashMap<OpId, (Self::CO, Self::CR)>,
        u: Vec<(OpId, Self::CO, Self::CR)>,
    ) -> HashMap<OpId, Self::CR>;
}

pub struct Replica<U: Upcalls, T: Transport<Message = Message<U>>> {
    index: Index,
    inner: Arc<Inner<U, T>>,
}

impl<U: Upcalls, T: Transport<Message = Message<U>>> Debug for Replica<U, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Replica");
        if let Ok(sync) = self.inner.sync.try_lock() {
            s.field("stat", &sync.status);
            s.field("view", &sync.view.number);
            s.field("last_norm_view", &sync.latest_normal_view);
        }
        s.finish_non_exhaustive()
    }
}

struct Inner<U: Upcalls, T: Transport<Message = Message<U>>> {
    transport: T,
    sync: Mutex<Sync<U, T>>,
}

struct Sync<U: Upcalls, T: Transport<Message = Message<U>>> {
    status: Status,
    view: View<T>,
    latest_normal_view: ViewNumber,
    changed_view_recently: bool,
    upcalls: U,
    record: Record<U>,
    outstanding_do_view_changes: HashMap<Index, DoViewChange<U::IO, U::CO, U::CR>>,
}

#[derive(Serialize, Deserialize)]
struct PersistentViewInfo {
    view: ViewNumber,
    latest_normal_view: ViewNumber,
}

impl<U: Upcalls, T: Transport<Message = Message<U>>> Replica<U, T> {
    const VIEW_CHANGE_INTERVAL: Duration = Duration::from_secs(4);

    pub fn new(index: Index, membership: Membership<T>, upcalls: U, transport: T) -> Self {
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
                    latest_normal_view: ViewNumber(0),
                    changed_view_recently: true,
                    upcalls,
                    record: Record::<U>::default(),
                    outstanding_do_view_changes: HashMap::new(),
                }),
            }),
        };
        let mut sync = ret.inner.sync.lock().unwrap();

        if let Some(persistent) = ret
            .inner
            .transport
            .persisted::<PersistentViewInfo>(&ret.view_info_key())
        {
            sync.status = Status::Recovering;
            sync.view.number = persistent.view;
            sync.latest_normal_view = persistent.latest_normal_view;
            sync.view.number.0 += 1;

            if sync.view.leader_index() == ret.index {
                sync.view.number.0 += 1;
            }

            ret.persist_view_info(&*sync);

            Self::broadcast_do_view_change(ret.index, &ret.inner.transport, &mut *sync);
        } else {
            ret.persist_view_info(&*sync);
        }
        drop(sync);
        ret.tick();
        ret
    }

    fn view_info_key(&self) -> String {
        format!("ir_replica_{}", self.index.0)
    }

    fn persist_view_info(&self, sync: &Sync<U, T>) {
        if sync.view.membership.len() == 1 {
            return;
        }
        self.inner.transport.persist(
            &self.view_info_key(),
            Some(&PersistentViewInfo {
                view: sync.view.number,
                latest_normal_view: sync.latest_normal_view,
            }),
        );
    }

    fn tick(&self) {
        let my_index = self.index;
        let inner = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            loop {
                T::sleep(Self::VIEW_CHANGE_INTERVAL).await;

                let Some(inner) = inner.upgrade() else {
                    break;
                };
                let mut sync = inner.sync.lock().unwrap();
                if sync.changed_view_recently {
                    sync.changed_view_recently = false;
                } else {
                    if sync.status.is_normal() {
                        sync.status = Status::ViewChanging;
                    }
                    sync.view.number.0 += 1;

                    eprintln!(
                        "{my_index:?} timeout sending do view change {}",
                        sync.view.number.0
                    );

                    Self::broadcast_do_view_change(my_index, &inner.transport, &mut *sync);
                }
            }
        });
    }

    fn broadcast_do_view_change(my_index: Index, transport: &T, sync: &mut Sync<U, T>) {
        sync.changed_view_recently = true;
        for (index, address) in &sync.view.membership {
            if index == my_index {
                continue;
            }
            transport.do_send(
                address,
                Message::<U>::DoViewChange(DoViewChange {
                    view_number: sync.view.number,
                    addendum: (index == sync.view.leader_index()).then(|| ViewChangeAddendum {
                        record: sync.record.clone(),
                        replica_index: my_index,
                        latest_normal_view: sync.latest_normal_view,
                    }),
                }),
            )
        }
    }

    pub fn receive(&self, address: T::Address, message: Message<U>) -> Option<Message<U>> {
        match message {
            Message::<U>::RequestUnlogged(RequestUnlogged { op }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                if sync.status.is_normal() {
                    let result = sync.upcalls.exec_unlogged(op);
                    return Some(Message::<U>::ReplyUnlogged(ReplyUnlogged {
                        result,
                        view_number: sync.view.number,
                    }));
                }
            }
            Message::<U>::ProposeInconsistent(ProposeInconsistent { op_id, op, recent }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                let sync = &mut *sync;
                if sync.status.is_normal() {
                    if !recent.is_recent_relative_to(sync.view.number) {
                        eprintln!("ancient relative to {:?}", sync.view.number);
                        return Some(Message::<U>::ReplyInconsistent(ReplyInconsistent {
                            op_id,
                            view_number: sync.view.number,
                            state: None,
                        }));
                    }
                    let entry =
                        sync.record
                            .inconsistent
                            .entry(op_id)
                            .or_insert(RecordInconsistentEntry {
                                op,
                                state: RecordEntryState::Tentative,
                            });

                    return Some(Message::<U>::ReplyInconsistent(ReplyInconsistent {
                        op_id,
                        view_number: sync.view.number,
                        state: Some(entry.state),
                    }));
                }
            }
            Message::<U>::ProposeConsensus(ProposeConsensus { op_id, op, recent }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                let sync = &mut *sync;
                if sync.status.is_normal() {
                    if !recent.is_recent_relative_to(sync.view.number) {
                        eprintln!("ancient relative to {:?}", sync.view.number);
                        return Some(Message::<U>::ReplyConsensus(ReplyConsensus {
                            op_id,
                            view_number: sync.view.number,
                            result_state: None,
                        }));
                    }
                    let (result, state) = match sync.record.consensus.entry(op_id) {
                        Entry::Occupied(entry) => {
                            let entry = entry.get();
                            (entry.result.clone(), entry.state)
                        }
                        Entry::Vacant(vacant) => {
                            let entry = vacant.insert(RecordConsensusEntry {
                                result: sync.upcalls.exec_consensus(&op),
                                op,
                                state: RecordEntryState::Tentative,
                            });
                            (entry.result.clone(), entry.state)
                        }
                    };

                    return Some(Message::<U>::ReplyConsensus(ReplyConsensus {
                        op_id,
                        view_number: sync.view.number,
                        result_state: Some((result, state)),
                    }));
                } else {
                    //println!("{:?} abnormal", self.index);
                }
            }
            Message::<U>::FinalizeInconsistent(FinalizeInconsistent { op_id }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                let sync = &mut *sync;
                if sync.status.is_normal() && let Some(entry) = sync.record.inconsistent.get_mut(&op_id) && entry.state.is_tentative() {
                    entry.state = RecordEntryState::Finalized;
                    sync.upcalls.exec_inconsistent(&entry.op);
                }
            }
            Message::<U>::FinalizeConsensus(FinalizeConsensus { op_id, result }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                if sync.status.is_normal() {
                    if let Some(entry) = sync.record.consensus.get_mut(&op_id) {
                        entry.state = RecordEntryState::Finalized;
                        entry.result = result;
                        return Some(Message::<U>::Confirm(Confirm {
                            op_id,
                            view_number: sync.view.number,
                        }));
                    }
                }
            }
            Message::<U>::DoViewChange(msg) => {
                let mut sync = self.inner.sync.lock().unwrap();
                if msg.view_number > sync.view.number
                    || (msg.view_number == sync.view.number && sync.status.is_view_changing())
                {
                    if msg.view_number > sync.view.number {
                        sync.view.number = msg.view_number;
                        if sync.status.is_normal() {
                            sync.status = Status::ViewChanging;
                        }
                        self.persist_view_info(&*sync);
                        Self::broadcast_do_view_change(
                            self.index,
                            &self.inner.transport,
                            &mut *sync,
                        );
                    }

                    eprintln!(
                        "index = {:?} , leader index = {:?}",
                        self.index,
                        sync.view.leader_index()
                    );

                    if self.index == sync.view.leader_index() && let Some(addendum) = msg.addendum.as_ref() {
                        let msg_view_number = msg.view_number;
                        match sync.outstanding_do_view_changes.entry(addendum.replica_index) {
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
                                eprintln!("DOING VIEW CHANGE");
                                {
                                    let latest_normal_view = sync.latest_normal_view.max(
                                        matching
                                            .clone()
                                            .map(|r| {
                                                r.addendum.as_ref().unwrap().latest_normal_view
                                            })
                                            .max()
                                            .unwrap(),
                                    );
                                    let mut latest_records = matching
                                        .clone()
                                        .filter(|r| {
                                            r.addendum.as_ref().unwrap().latest_normal_view
                                                == latest_normal_view
                                        })
                                        .map(|r| r.addendum.as_ref().unwrap().record.clone())
                                        .collect::<Vec<_>>();
                                    if sync.latest_normal_view == latest_normal_view {
                                        latest_records.push(sync.record.clone());
                                    }
                                    eprintln!("have {} latest", latest_records.len());

                                    #[allow(non_snake_case)]
                                    let mut R = Record::<U>::default();
                                    let mut entries_by_opid =
                                        HashMap::<OpId, Vec<RecordConsensusEntry<U::CO, U::CR>>>::new();
                                    let mut finalized = HashSet::new();
                                    for r in latest_records {
                                        for (op_id, entry) in r.inconsistent.clone() {
                                            match R.inconsistent.entry(op_id) {
                                                Entry::Vacant(vacant) => {
                                                    vacant.insert(entry);
                                                }
                                                Entry::Occupied(mut occupied) => {
                                                    if entry.state.is_finalized() {
                                                        occupied.get_mut().state = RecordEntryState::Finalized;
                                                    }
                                                }
                                            }
                                        }
                                        for (op_id, entry) in r.consensus.clone() {
                                            match entry.state {
                                                RecordEntryState::Finalized => {
                                                    R.consensus.insert(op_id, entry);
                                                    finalized.insert(op_id);
                                                    entries_by_opid.remove(&op_id);
                                                }
                                                RecordEntryState::Tentative => {
                                                    if !finalized.contains(&op_id) {
                                                        entries_by_opid
                                                            .entry(op_id)
                                                            .or_default()
                                                            .push(entry);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    // build d and u
                                    let mut d =
                                        HashMap::<OpId, (U::CO, U::CR)>::new();
                                    let mut u =
                                        Vec::<(OpId, U::CO, U::CR)>::new();

                                    for (op_id, entries) in entries_by_opid.clone() {
                                        let mut majority_result_in_d = None;

                                        for entry in &entries {
                                            let matches = entries
                                                .iter()
                                                .filter(|other| other.result == entry.result)
                                                .count();

                                            if matches
                                                >= sync.view.membership.size().f_over_two_plus_one()
                                            {
                                                majority_result_in_d = Some(entry.result.clone());
                                                break;
                                            }
                                        }

                                        if let Some(majority_result_in_d) = majority_result_in_d {
                                            d.insert(op_id, (entries[0].op.clone(), majority_result_in_d));
                                        } else {
                                            u.extend(entries.into_iter().map(|e| (op_id, e.op, e.result)));
                                        }
                                    }

                                    // println!("d = {d:?}");
                                    // println!("u = {u:?}");

                                    {
                                        let sync = &mut *sync;
                                        sync.upcalls.sync(&sync.record, &R);
                                    }

                                    let results_by_opid =
                                        sync.upcalls.merge(d, u);

                                    debug_assert_eq!(results_by_opid.len(), entries_by_opid.len());

                                    for (op_id, result) in results_by_opid {
                                        let mut entries = entries_by_opid.get(&op_id).unwrap();
                                        let entry = &entries[0];
                                        R.consensus.insert(
                                            op_id,
                                            RecordConsensusEntry {
                                                op: entry.op.clone(),
                                                result: result.clone(),
                                                state: RecordEntryState::Finalized,
                                            },
                                        );
                                    }

                                    sync.record = R;
                                }
                                sync.changed_view_recently = true;
                                sync.status = Status::Normal;
                                sync.view.number = msg_view_number;
                                sync.latest_normal_view = msg_view_number;
                                self.persist_view_info(&*sync);
                                for (index, address) in &sync.view.membership {
                                    if index == self.index {
                                        continue;
                                    }
                                    self.inner.transport.do_send(
                                        address,
                                        Message::<U>::StartView(StartView {
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
            Message::<U>::StartView(StartView {
                record: new_record,
                view_number,
            }) => {
                let mut sync = self.inner.sync.lock().unwrap();
                let sync = &mut *sync;
                if view_number > sync.view.number
                    || (view_number == sync.view.number || !sync.status.is_normal())
                {
                    eprintln!("{:?} starting view {view_number:?}", self.index);
                    sync.upcalls.sync(&sync.record, &new_record);
                    sync.record = new_record;
                    sync.status = Status::Normal;
                    sync.view.number = view_number;
                    sync.latest_normal_view = view_number;
                    self.persist_view_info(&*sync);
                }
            }
            _ => {
                eprintln!("unexpected message");
            }
        }
        None
    }
}
