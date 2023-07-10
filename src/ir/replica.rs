use super::{
    message::ViewChangeAddendum, AddMember, Confirm, DoViewChange, FinalizeConsensus,
    FinalizeInconsistent, Membership, Message, OpId, ProposeConsensus, ProposeInconsistent, Record,
    RecordConsensusEntry, RecordEntryState, RecordInconsistentEntry, ReplyConsensus,
    ReplyInconsistent, ReplyUnlogged, RequestUnlogged, StartView, View, ViewNumber,
};
use crate::{Transport, TransportMessage};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

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

pub trait Upcalls: Sized + Send + Serialize + DeserializeOwned + 'static {
    /// Unlogged operation.
    type UO: TransportMessage;
    /// Unlogged result.
    type UR: TransportMessage;
    /// Inconsistent operation.
    type IO: TransportMessage + Eq;
    /// Consensus operation.
    type CO: TransportMessage + Eq;
    /// Consensus result.
    type CR: TransportMessage + Eq + Hash;

    fn exec_unlogged(&mut self, op: Self::UO) -> Self::UR;
    fn exec_inconsistent(&mut self, op: &Self::IO);
    fn exec_consensus(&mut self, op: &Self::CO) -> Self::CR;
    /// Extension to TAPIR: Called when an entry becomes finalized. This
    /// addresses a potential issue with `merge` rolling back finalized
    /// operations. The application assumes responsibility for calling
    /// this during `sync` and, if necessary, `merge`.
    fn finalize_consensus(&mut self, op: &Self::CO, res: &Self::CR) {
        // No-op.
        let _ = (op, res);
    }
    /// In addition to the IR spec, this must not rely on the existence
    /// of any ancient records (from before the last view change) in the
    /// leader's record.
    fn sync(&mut self, local: &Record<Self>, leader: &Record<Self>);
    fn merge(
        &mut self,
        d: HashMap<OpId, (Self::CO, Self::CR)>,
        u: Vec<(OpId, Self::CO, Self::CR)>,
    ) -> HashMap<OpId, Self::CR>;
    fn tick<T: Transport<Self>>(&mut self, membership: &Membership<T::Address>, transport: &T) {
        let _ = (membership, transport);
        // No-op.
    }
}

pub struct Replica<U: Upcalls, T: Transport<U>> {
    inner: Arc<Inner<U, T>>,
}

impl<U: Upcalls, T: Transport<U>> Debug for Replica<U, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Replica");
        if let Ok(sync) = self.inner.sync.try_lock() {
            s.field("stat", &sync.status);
            s.field("view", &sync.view.number);
            s.field("norm", &sync.latest_normal_view);
            s.field("inc", &sync.record.inconsistent.len());
            s.field("con", &sync.record.consensus.len());
        }
        s.finish_non_exhaustive()
    }
}

struct Inner<U: Upcalls, T: Transport<U>> {
    transport: T,
    sync: Mutex<Sync<U, T>>,
}

struct Sync<U: Upcalls, T: Transport<U>> {
    status: Status,
    view: View<T::Address>,
    latest_normal_view: View<T::Address>,
    changed_view_recently: bool,
    upcalls: U,
    record: Record<U>,
    // TODO: GC old replicas.
    outstanding_do_view_changes: HashMap<T::Address, DoViewChange<U::IO, U::CO, U::CR, T::Address>>,
    /// Last time received message from each peer replica.
    // TODO: GC old replicas.
    peer_liveness: HashMap<T::Address, Instant>,
}

#[derive(Serialize, Deserialize)]
struct PersistentViewInfo<A> {
    view: View<A>,
    latest_normal_view: View<A>,
}

impl<U: Upcalls, T: Transport<U>> Replica<U, T> {
    const VIEW_CHANGE_INTERVAL: Duration = Duration::from_secs(4);

    pub fn new(membership: Membership<T::Address>, upcalls: U, transport: T) -> Self {
        let view = View {
            membership,
            number: ViewNumber(0),
        };
        let ret = Self {
            inner: Arc::new(Inner {
                transport,
                sync: Mutex::new(Sync {
                    status: Status::Normal,
                    latest_normal_view: view.clone(),
                    view,
                    changed_view_recently: true,
                    upcalls,
                    record: Record::<U>::default(),
                    outstanding_do_view_changes: HashMap::new(),
                    peer_liveness: HashMap::new(),
                }),
            }),
        };
        let mut sync = ret.inner.sync.lock().unwrap();

        if let Some(persistent) = ret
            .inner
            .transport
            .persisted::<PersistentViewInfo<T::Address>>(&ret.view_info_key())
        {
            sync.status = Status::Recovering;
            sync.view = persistent.view;
            sync.latest_normal_view = persistent.latest_normal_view;
            sync.view.number.0 += 1;

            if sync.view.leader() == ret.inner.transport.address() {
                sync.view.number.0 += 1;
            }

            ret.persist_view_info(&*sync);

            Self::broadcast_do_view_change(&ret.inner.transport, &mut *sync);
        } else {
            ret.persist_view_info(&*sync);
        }
        drop(sync);
        ret.tick();
        ret.tick_app();
        ret
    }

    pub fn transport(&self) -> &T {
        &self.inner.transport
    }

    pub fn address(&self) -> T::Address {
        self.inner.transport.address()
    }

    fn view_info_key(&self) -> String {
        format!("ir_replica_{}", self.inner.transport.address())
    }

    fn persist_view_info(&self, sync: &Sync<U, T>) {
        if sync.view.membership.len() == 1 {
            return;
        }
        self.inner.transport.persist(
            &self.view_info_key(),
            Some(&PersistentViewInfo {
                view: sync.view.clone(),
                latest_normal_view: sync.latest_normal_view.clone(),
            }),
        );
    }

    fn tick(&self) {
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
                }
                /* else if sync
                    .peer_liveness
                    .get(&Index(
                        ((sync.view.number.0 + 1) % sync.view.membership.len() as u64) as usize,
                    ))
                    .map(|t| t.elapsed() > Duration::from_secs(3))
                    .unwrap_or(false)
                {
                    // skip this view change.
                } */
                else {
                    if sync.status.is_normal() {
                        sync.status = Status::ViewChanging;
                    }
                    sync.view.number.0 += 1;

                    eprintln!(
                        "{:?} timeout sending do view change {}",
                        inner.transport.address(),
                        sync.view.number.0
                    );

                    Self::broadcast_do_view_change(&inner.transport, &mut *sync);
                }
            }
        });
    }

    fn tick_app(&self) {
        let inner = Arc::downgrade(&self.inner);
        let transport = self.inner.transport.clone();
        tokio::spawn(async move {
            loop {
                T::sleep(Duration::from_secs(1)).await;

                let Some(inner) = inner.upgrade() else {
                    break;
                };
                let mut sync = inner.sync.lock().unwrap();
                let sync = &mut *sync;
                sync.upcalls.tick(&sync.view.membership, &transport);
            }
        });
    }

    fn broadcast_do_view_change(transport: &T, sync: &mut Sync<U, T>) {
        sync.changed_view_recently = true;
        for address in &sync.view.membership {
            if address == transport.address() {
                continue;
            }
            transport.do_send(
                address,
                Message::<U, T>::DoViewChange(DoViewChange {
                    view: sync.view.clone(),
                    addendum: (address == sync.view.leader()).then(|| ViewChangeAddendum {
                        record: sync.record.clone(),
                        latest_normal_view: sync.latest_normal_view.clone(),
                    }),
                }),
            )
        }
    }

    pub fn receive(&self, address: T::Address, message: Message<U, T>) -> Option<Message<U, T>> {
        let mut sync = self.inner.sync.lock().unwrap();
        let sync = &mut *sync;

        if sync.view.membership.get_index(address).is_some() {
            sync.peer_liveness.insert(address, Instant::now());
        }

        match message {
            Message::<U, T>::RequestUnlogged(RequestUnlogged { op }) => {
                if sync.status.is_normal() {
                    let result = sync.upcalls.exec_unlogged(op);
                    return Some(Message::<U, T>::ReplyUnlogged(ReplyUnlogged {
                        result,
                        view: sync.view.clone(),
                    }));
                } else {
                    eprintln!("{:?} abnormal", self.inner.transport.address());
                }
            }
            Message::<U, T>::ProposeInconsistent(ProposeInconsistent { op_id, op, recent }) => {
                if sync.status.is_normal() {
                    if !recent.is_recent_relative_to(sync.view.number) {
                        eprintln!("ancient relative to {:?}", sync.view.number);
                        return Some(Message::<U, T>::ReplyInconsistent(ReplyInconsistent {
                            op_id,
                            view: sync.view.clone(),
                            state: None,
                        }));
                    }

                    let state = match sync.record.inconsistent.entry(op_id) {
                        Entry::Vacant(vacant) => {
                            vacant.insert(RecordInconsistentEntry {
                                op,
                                state: RecordEntryState::Tentative,
                            }).state
                        }
                        Entry::Occupied(occupied) => {
                            debug_assert_eq!(occupied.get().op, op);
                            occupied.get().state
                        }
                    };

                    return Some(Message::<U, T>::ReplyInconsistent(ReplyInconsistent {
                        op_id,
                        view: sync.view.clone(),
                        state: Some(state),
                    }));
                }
            }
            Message::<U, T>::ProposeConsensus(ProposeConsensus { op_id, op, recent }) => {
                if sync.status.is_normal() {
                    if !recent.is_recent_relative_to(sync.view.number) {
                        eprintln!("ancient relative to {:?}", sync.view.number);
                        return Some(Message::<U, T>::ReplyConsensus(ReplyConsensus {
                            op_id,
                            view: sync.view.clone(),
                            result_state: None,
                        }));
                    }
                    let (result, state) = match sync.record.consensus.entry(op_id) {
                        Entry::Occupied(entry) => {
                            let entry = entry.get();
                            debug_assert_eq!(entry.op, op);
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

                    return Some(Message::<U, T>::ReplyConsensus(ReplyConsensus {
                        op_id,
                        view: sync.view.clone(),
                        result_state: Some((result, state)),
                    }));
                } else {
                    eprintln!("{:?} abnormal", self.inner.transport.address());
                }
            }
            Message::<U, T>::FinalizeInconsistent(FinalizeInconsistent { op_id }) => {
                if sync.status.is_normal() && let Some(entry) = sync.record.inconsistent.get_mut(&op_id) && entry.state.is_tentative() {
                    entry.state = RecordEntryState::Finalized(sync.view.number);
                    sync.upcalls.exec_inconsistent(&entry.op);
                }
            }
            Message::<U, T>::FinalizeConsensus(FinalizeConsensus { op_id, result }) => {
                if sync.status.is_normal() {
                    if let Some(entry) = sync.record.consensus.get_mut(&op_id) {
                        // Don't allow a late `FinalizeConsensus` to overwrite
                        // a view change decision.
                        if entry.state.is_tentative() {
                            entry.state = RecordEntryState::Finalized(sync.view.number);
                            entry.result = result;
                            sync.upcalls.finalize_consensus(&entry.op, &entry.result);
                        } else if cfg!(debug_assertions) && entry.result != result {
                            // For diagnostic purposes.
                            eprintln!("warning: tried to finalize consensus with {result:?} when {:?} was already finalized", entry.result);
                        }

                        // Send `Confirm` regardless; the view number gives the
                        // client enough information to retry if needed.
                        return Some(Message::<U, T>::Confirm(Confirm {
                            op_id,
                            view: sync.view.clone(),
                        }));
                    }
                }
            }
            Message::<U, T>::DoViewChange(msg) => {
                //eprintln!("{:?} receiving dvt {:?} and have {:?} / {:?}", self.inner.transport.address(), msg.view.number, sync.view.number, sync.status);
                if msg.view.number > sync.view.number
                    || (msg.view.number == sync.view.number && sync.status.is_view_changing())
                {
                    if msg.view.number > sync.view.number {
                        sync.view = msg.view.clone();
                        if sync.status.is_normal() {
                            sync.status = Status::ViewChanging;
                        }
                        self.persist_view_info(&*sync);
                        Self::broadcast_do_view_change(
                            &self.inner.transport,
                            &mut *sync,
                        );
                    }

                    /*
                    eprintln!(
                        "index = {:?} , leader index = {:?}",
                        self.index,
                        sync.view.leader_index()
                    );
                    */

                    if self.inner.transport.address() == sync.view.leader() && msg.addendum.is_some() {
                        let msg_view_number = msg.view.number;
                        match sync.outstanding_do_view_changes.entry(address) {
                            Entry::Vacant(vacant) => {
                                vacant.insert(msg);
                            }
                            Entry::Occupied(mut occupied) => {
                                if msg.view.number < occupied.get().view.number {
                                    return None;
                                }
                                occupied.insert(msg);
                            }
                        }

                        let threshold = sync.latest_normal_view.membership.size().f();
                        let matching = sync
                            .outstanding_do_view_changes
                            .iter()
                            .filter(|(address, other)|
                                sync.latest_normal_view.membership.contains(**address)
                                && other.view.number == sync.view.number
                            );

                        if matching.clone().count() >= threshold {
                            eprintln!("{:?} DOING VIEW CHANGE", self.inner.transport.address());
                            {
                                let latest_normal_view =
                                    matching
                                        .clone()
                                        .map(|(_, r)| {
                                            &r.addendum.as_ref().unwrap().latest_normal_view
                                        })
                                        .chain(std::iter::once(&sync.latest_normal_view))
                                        .max_by_key(|v| v.number)
                                        .unwrap()
                                ;
                                let mut latest_records = matching
                                    .clone()
                                    .filter(|(_, r)| {
                                        r.addendum.as_ref().unwrap().latest_normal_view.number
                                            == latest_normal_view.number
                                    })
                                    .map(|(_, r)| r.addendum.as_ref().unwrap().record.clone())
                                    .collect::<Vec<_>>();
                                if sync.latest_normal_view.number == latest_normal_view.number {
                                    latest_records.push(sync.record.clone());
                                }
                                eprintln!(
                                    "have {} latest ({:?})",
                                    latest_records.len(),
                                    sync
                                        .outstanding_do_view_changes
                                        .iter()
                                        .map(|(a, dvt)| (*a, dvt.view.number, dvt.addendum.as_ref().unwrap().latest_normal_view.number))
                                        .chain(
                                            std::iter::once(
                                                (self.inner.transport.address(), sync.view.number, sync.latest_normal_view.number)
                                            )
                                        )
                                        .collect::<Vec<_>>()
                                );

                                #[allow(non_snake_case)]
                                let mut R = Record::<U>::default();
                                let mut entries_by_opid =
                                    HashMap::<OpId, Vec<RecordConsensusEntry<U::CO, U::CR>>>::new();
                                let mut finalized = HashSet::new();
                                for r in latest_records {
                                    for (op_id, entry) in r.inconsistent.clone() {
                                        match R.inconsistent.entry(op_id) {
                                            Entry::Vacant(vacant) => {
                                                // Mark as finalized as `sync` will execute it.
                                                vacant.insert(entry).state = RecordEntryState::Finalized(sync.view.number);
                                            }
                                            Entry::Occupied(mut occupied) => {
                                                if let RecordEntryState::Finalized(view) = entry.state {
                                                    let state = &mut occupied.get_mut().state;
                                                    *state = RecordEntryState::Finalized(view);
                                                }
                                            }
                                        }
                                    }
                                    for (op_id, entry) in r.consensus.clone() {
                                        match entry.state {
                                            RecordEntryState::Finalized(_) => {
                                                match R.consensus.entry(op_id) {
                                                    Entry::Vacant(vacant) => {
                                                        sync.upcalls.finalize_consensus(&entry.op, &entry.result);
                                                        vacant.insert(entry);
                                                    }
                                                    Entry::Occupied(mut occupied) => {
                                                        if occupied.get().state.is_tentative() {
                                                            sync.upcalls.finalize_consensus(&entry.op, &entry.result);
                                                            occupied.insert(entry);
                                                        } else {
                                                            debug_assert_eq!(occupied.get().result, entry.result);
                                                        }
                                                    }
                                                }
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
                                    debug_assert!(!finalized.contains(&op_id));

                                    let mut majority_result_in_d = None;

                                    for entry in &entries {
                                        debug_assert!(entry.state.is_tentative());

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
                                        eprintln!("merge majority replied {:?} to {op_id:?}", majority_result_in_d);
                                        d.insert(op_id, (entries[0].op.clone(), majority_result_in_d));
                                    } else {
                                        eprintln!("merge no majority for {op_id:?}; deciding among {:?}", entries.iter().map(|entry| (entry.result.clone(), entry.state)).collect::<Vec<_>>());
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
                                    let entries = entries_by_opid.get(&op_id).unwrap();
                                    let entry = &entries[0];
                                    sync.upcalls.finalize_consensus(&entry.op, &result);
                                    R.consensus.insert(
                                        op_id,
                                        RecordConsensusEntry {
                                            op: entry.op.clone(),
                                            result: result.clone(),
                                            state: RecordEntryState::Finalized(sync.view.number),
                                        },
                                    );
                                }

                                sync.record = R;
                            }
                            sync.changed_view_recently = true;
                            sync.status = Status::Normal;
                            sync.view.number = msg_view_number;
                            sync.latest_normal_view.number = msg_view_number;
                            self.persist_view_info(&*sync);
                            for address in &sync.view.membership {
                                if address == self.inner.transport.address() {
                                    continue;
                                }
                                self.inner.transport.do_send(
                                    address,
                                    Message::<U, T>::StartView(StartView {
                                        record: sync.record.clone(),
                                        view: sync.view.clone(),
                                    }),
                                );
                            }
                            self.inner.transport.persist(
                                &format!("checkpoint_{}", sync.view.number.0),
                                Some(&sync.upcalls),
                            );
                        }
                    }
                }
            }
            Message::<U, T>::StartView(StartView {
                record: new_record,
                view,
            }) => {
                if view.number > sync.view.number
                    || (view.number == sync.view.number || !sync.status.is_normal())
                {
                    eprintln!("{:?} starting view {view:?}", self.inner.transport.address());
                    sync.upcalls.sync(&sync.record, &new_record);
                    sync.record = new_record;
                    sync.status = Status::Normal;
                    sync.view = view.clone();
                    sync.latest_normal_view = view;
                    self.persist_view_info(&*sync);
                }
            }
            Message::<U, T>::AddMember(AddMember{address}) => {
                if sync.status.is_normal() && sync.view.membership.get_index(address).is_none() {
                    sync.view.number.0 += 3;

                    // Add the node.
                    let mut next = View{
                        membership: Membership::new(
                            sync.view.membership
                                .iter()
                                .chain(std::iter::once(address))
                                .collect()
                            ),
                        number: sync.view.number
                    };

                    // Become the leader before and after new node is added.
                    while (sync.view.leader() != self.inner.transport.address())
                        || (next.leader() != self.inner.transport.address()) {
                        sync.view.number.0 += 1;
                        next.number.0 += 1;
                        debug_assert_eq!(sync.view.number, next.number);
                    }
                    sync.view = next;

                    // Election.
                    Self::broadcast_do_view_change(&self.inner.transport, sync);
                }
            }
            _ => {
                eprintln!("unexpected message");
            }
        }
        None
    }
}
