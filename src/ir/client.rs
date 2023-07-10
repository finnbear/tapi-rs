use super::{
    Confirm, DoViewChange, FinalizeConsensus, FinalizeInconsistent, Membership, MembershipSize,
    Message, OpId, ProposeConsensus, ProposeInconsistent, ReplicaUpcalls, ReplyConsensus,
    ReplyInconsistent, ReplyUnlogged, RequestUnlogged, View, ViewNumber,
};
use crate::{
    util::{join, Join},
    Transport,
};
use futures::{stream::FuturesUnordered, StreamExt};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    hash::Hash,
    marker::PhantomData,
    sync::{Arc, Mutex},
    task::Context,
    time::Duration,
};
use tokio::select;

/// Randomly chosen id, unique to each IR client.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Id(pub u64);

impl Id {
    fn new() -> Self {
        Self(thread_rng().gen())
    }
}

impl Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "C({})", self.0)
    }
}

/// IR client, capable of invoking operations on an IR replica group.
pub struct Client<U: ReplicaUpcalls, T: Transport<U>> {
    id: Id,
    inner: Arc<Inner<U, T>>,
    _spooky: PhantomData<U>,
}

impl<U: ReplicaUpcalls, T: Transport<U>> Clone for Client<U, T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            inner: Arc::clone(&self.inner),
            _spooky: PhantomData,
        }
    }
}

struct Inner<U: ReplicaUpcalls, T: Transport<U>> {
    transport: T,
    sync: Mutex<SyncInner<U, T>>,
}

struct SyncInner<U: ReplicaUpcalls, T: Transport<U>> {
    operation_counter: u64,
    view: View<T::Address>,
}

impl<U: ReplicaUpcalls, T: Transport<U>> SyncInner<U, T> {
    fn next_number(&mut self) -> u64 {
        let ret = self.operation_counter;
        self.operation_counter += 1;
        ret
    }
}

impl<U: ReplicaUpcalls, T: Transport<U>> Client<U, T> {
    pub fn new(membership: Membership<T::Address>, transport: T) -> Self {
        Self {
            id: Id::new(),
            inner: Arc::new(Inner {
                transport,
                sync: Mutex::new(SyncInner {
                    operation_counter: 0,
                    view: View {
                        membership,
                        number: ViewNumber(0),
                    },
                }),
            }),
            _spooky: PhantomData,
        }
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn transport(&self) -> &T {
        &self.inner.transport
    }

    /// Updates own view and those of lagging replicas.
    ///
    /// `Index`'s in `views` must correspond to `sync.view`.
    fn update_view<'a>(
        transport: &T,
        sync: &mut SyncInner<U, T>,
        views: impl IntoIterator<Item = (T::Address, &'a View<T::Address>)> + Clone,
    ) {
        if let Some(latest_view) = views
            .clone()
            .into_iter()
            .map(|(_, n)| n)
            .max_by_key(|v| v.number)
        {
            for (address, view) in views {
                if view.number < latest_view.number {
                    transport.do_send(
                        address,
                        Message::<U, T>::DoViewChange(DoViewChange {
                            view: latest_view.clone(),
                            addendum: None,
                        }),
                    )
                }
            }
            if latest_view.number > sync.view.number {
                sync.view = latest_view.clone();
            }
        }
    }

    /// Unlogged request against any single replica.
    pub fn invoke_unlogged(&self, op: U::UO) -> impl Future<Output = U::UR> {
        let inner = Arc::clone(&self.inner);
        let (index, count) = {
            let sync = inner.sync.lock().unwrap();
            let count = sync.view.membership.len();
            (thread_rng().gen_range(0..count), count)
        };

        async move {
            let mut futures = FuturesUnordered::new();

            loop {
                debug_assert!(futures.len() < count);
                {
                    let sync = inner.sync.lock().unwrap();
                    let address = sync
                        .view
                        .membership
                        .get((index + futures.len()) % count)
                        .unwrap();
                    drop(sync);
                    let future = inner.transport.send::<ReplyUnlogged<U::UR, T::Address>>(
                        address,
                        RequestUnlogged { op: op.clone() },
                    );
                    futures.push(future);
                }

                let timeout = std::pin::pin!(T::sleep(Duration::from_millis(250)));

                let response = select! {
                    _ = timeout, if futures.len() < count => {
                        continue;
                    }
                    response = futures.next() => {
                        response.unwrap()
                    }
                };
                let mut sync = inner.sync.lock().unwrap();
                if response.view.number > sync.view.number {
                    sync.view = response.view;
                }
                return response.result;
            }
        }
    }

    /// Returns a `Join` over getting an unlogged response from all replicas.
    ///
    /// A consenSUS operation; can get a quorum but doesn't preserve decisions.
    pub fn invoke_unlogged_joined(
        &self,
        op: U::UO,
    ) -> (
        Join<T::Address, impl Future<Output = ReplyUnlogged<U::UR, T::Address>>>,
        MembershipSize,
    ) {
        let sync = self.inner.sync.lock().unwrap();
        let membership_size = sync.view.membership.size();

        let future = join(sync.view.membership.iter().map(|address| {
            (
                address,
                self.inner
                    .transport
                    .send::<ReplyUnlogged<U::UR, T::Address>>(
                        address,
                        RequestUnlogged { op: op.clone() },
                    ),
            )
        }));

        (future, membership_size)
    }

    /// Returns when the inconsistent operation is finalized, retrying indefinitely.
    pub fn invoke_inconsistent(&self, op: U::IO) -> impl Future<Output = ()> {
        let inner = Arc::clone(&self.inner);

        let op_id = {
            let mut sync = inner.sync.lock().unwrap();

            OpId {
                client_id: self.id,
                number: sync.next_number(),
            }
        };

        fn has_ancient<A>(results: &HashMap<A, ReplyInconsistent<A>>) -> bool {
            results.values().any(|v| v.state.is_none())
        }

        fn has_quorum<A>(
            membership: MembershipSize,
            results: &HashMap<A, ReplyInconsistent<A>>,
            check_views: bool,
        ) -> bool {
            if check_views {
                for result in results.values() {
                    let matching = results
                        .values()
                        .filter(|other| other.view.number == result.view.number)
                        .count();
                    if matching >= result.view.membership.size().f_plus_one() {
                        return true;
                    }
                }
                false
            } else {
                results.len() >= membership.f_plus_one()
            }
        }

        async move {
            loop {
                let (membership_size, future) = {
                    let sync = inner.sync.lock().unwrap();
                    let membership_size = sync.view.membership.size();

                    let future = join(sync.view.membership.iter().map(|address| {
                        (
                            address,
                            inner.transport.send::<ReplyInconsistent<T::Address>>(
                                address,
                                ProposeInconsistent {
                                    op_id,
                                    op: op.clone(),
                                    recent: sync.view.number,
                                },
                            ),
                        )
                    }));
                    (membership_size, future)
                };

                let mut soft_timeout = std::pin::pin!(T::sleep(Duration::from_millis(250)));

                // E.g. the replica group got smaller and we can't get a response from a majority of the old size.
                let mut hard_timeout = std::pin::pin!(T::sleep(Duration::from_millis(5000)));

                let results = future
                    .until(
                        move |results: &HashMap<T::Address, ReplyInconsistent<T::Address>>,
                              cx: &mut Context<'_>| {
                            has_ancient(results)
                                || has_quorum(
                                    membership_size,
                                    results,
                                    soft_timeout.as_mut().poll(cx).is_ready(),
                                )
                                || hard_timeout.as_mut().poll(cx).is_ready()
                        },
                    )
                    .await;

                let mut sync = inner.sync.lock().unwrap();
                Self::update_view(
                    &inner.transport,
                    &mut *sync,
                    results.iter().map(|(i, r)| (*i, &r.view)),
                );

                if has_ancient(&results) || !has_quorum(membership_size, &results, true) {
                    continue;
                }

                // println!("finalizing to membership: {:?}", sync.membership);
                for address in &sync.view.membership {
                    inner
                        .transport
                        .do_send(address, FinalizeInconsistent { op_id });
                }
                return;
            }
        }
    }

    /// Returns the consensus result, retrying indefinitely.
    pub fn invoke_consensus(
        &self,
        op: U::CO,
        decide: impl Fn(HashMap<U::CR, usize>, MembershipSize) -> U::CR + Send,
    ) -> impl Future<Output = U::CR> + Send {
        fn has_ancient<R, A>(replies: &HashMap<A, ReplyConsensus<R, A>>) -> bool {
            replies.values().any(|v| v.result_state.is_none())
        }

        fn get_finalized<R, A>(replies: &HashMap<A, ReplyConsensus<R, A>>) -> Option<&R> {
            replies
                .values()
                .find(|r| r.result_state.as_ref().unwrap().1.is_finalized())
                .map(|r| &r.result_state.as_ref().unwrap().0)
        }

        fn get_quorum<R: PartialEq + Debug, A>(
            membership: MembershipSize,
            replies: &HashMap<A, ReplyConsensus<R, A>>,
            matching_results: bool,
        ) -> Option<(&View<A>, &R)> {
            let required = if matching_results {
                membership.three_over_two_f_plus_one()
            } else {
                membership.f_plus_one()
            };
            for candidate in replies.values() {
                let matches = replies
                    .values()
                    .filter(|other| {
                        other.view.number == candidate.view.number
                            && (!matching_results
                                || other.result_state.as_ref().unwrap().0
                                    == candidate.result_state.as_ref().unwrap().0)
                    })
                    .count();
                if matches >= required {
                    return Some((&candidate.view, &candidate.result_state.as_ref().unwrap().0));
                }
            }
            None
        }

        let client_id = self.id;
        let inner = Arc::clone(&self.inner);

        async move {
            loop {
                let (membership_size, op_id, future) = {
                    let mut sync = inner.sync.lock().unwrap();
                    let number = sync.next_number();
                    let op_id = OpId { client_id, number };
                    let membership_size = sync.view.membership.size();

                    let future = join(sync.view.membership.iter().map(|address| {
                        (
                            address,
                            inner.transport.send::<ReplyConsensus<U::CR, T::Address>>(
                                address,
                                ProposeConsensus {
                                    op_id,
                                    op: op.clone(),
                                    recent: sync.view.number,
                                },
                            ),
                        )
                    }));
                    (membership_size, op_id, future)
                };

                let mut soft_timeout = std::pin::pin!(T::sleep(Duration::from_millis(250)));
                let mut hard_timeout = std::pin::pin!(T::sleep(Duration::from_millis(500)));

                let results = future
                    .until(
                        move |results: &HashMap<T::Address, ReplyConsensus<U::CR, T::Address>>,
                              cx: &mut Context<'_>| {
                            has_ancient(results)
                                || get_finalized(results).is_some()
                                || get_quorum(
                                    membership_size,
                                    results,
                                    soft_timeout.as_mut().poll(cx).is_pending(),
                                )
                                .is_some()
                                || hard_timeout.as_mut().poll(cx).is_ready()
                        },
                    )
                    .await;

                fn get_quorum_view<A>(results: &HashMap<A, Confirm<A>>) -> Option<&View<A>> {
                    for result in results.values() {
                        let matching = results
                            .values()
                            .filter(|other| other.view.number == result.view.number)
                            .count();
                        if matching >= result.view.membership.size().f_plus_one() {
                            return Some(&result.view);
                        }
                    }
                    None
                }

                let next = {
                    let mut sync = inner.sync.lock().unwrap();

                    Self::update_view(
                        &inner.transport,
                        &mut *sync,
                        results.iter().map(|(i, r)| (*i, &r.view)),
                    );

                    if has_ancient(&results) {
                        // Our view number was very old.
                        continue;
                    }

                    let membership_size = sync.view.membership.size();
                    let finalized = get_finalized(&results);
                    //println!("checking quorum: {}", finalized.is_some());
                    if finalized.is_none() && let Some((_, result)) = get_quorum(membership_size, &results, true) {
                        // Fast path.
                        // eprintln!("doing fast path");
                        for address in &sync.view.membership {
                            inner.transport.do_send(
                                address,
                                FinalizeConsensus {
                                    op_id,
                                    result: result.clone(),
                                },
                            );
                        }
                        return result.clone();
                    }

                    if let Some((result, reply_consensus_view)) =
                        finalized.map(|f| (f.clone(), None)).or_else(|| {
                            let view = get_quorum(membership_size, &results, false).map(|(v, _)| v);
                            view.cloned().map(|view| {
                                let results = results
                                    .into_values()
                                    .filter(|rc| rc.view.number == view.number)
                                    .map(|rc| rc.result_state.unwrap().0);
                                let mut totals = HashMap::new();
                                for result in results {
                                    *totals.entry(result).or_default() += 1;
                                }
                                (decide(totals, view.membership.size()), Some(view.number))
                            })
                        })
                    {
                        // Slow path.
                        /*
                        eprintln!(
                            "doing SLOW path finalized={}",
                            reply_consensus_view.is_none()
                        );
                        */
                        let future = join(sync.view.membership.iter().map(|address| {
                            (
                                address,
                                inner.transport.send::<Confirm<T::Address>>(
                                    address,
                                    FinalizeConsensus {
                                        op_id,
                                        result: result.clone(),
                                    },
                                ),
                            )
                        }));

                        Some((result, reply_consensus_view, future))
                    } else {
                        // println!("no quorum");
                        None
                    }
                };

                if let Some((result, reply_consensus_view, future)) = next {
                    let mut soft_timeout = std::pin::pin!(T::sleep(Duration::from_millis(250)));
                    let mut hard_timeout = std::pin::pin!(T::sleep(Duration::from_millis(500)));
                    let results = future
                        .until(
                            |results: &HashMap<T::Address, Confirm<T::Address>>,
                             cx: &mut Context<'_>| {
                                get_quorum_view(results).is_some()
                                    || (soft_timeout.as_mut().poll(cx).is_ready()
                                        && results.len() >= membership_size.f_plus_one())
                                    || hard_timeout.as_mut().poll(cx).is_ready()
                            },
                        )
                        .await;
                    let mut sync = inner.sync.lock().unwrap();
                    Self::update_view(
                        &inner.transport,
                        &mut *sync,
                        results.iter().map(|(i, r)| (*i, &r.view)),
                    );
                    if let Some(quorum_view) = get_quorum_view(&results) && reply_consensus_view.map(|reply_consensus_view| quorum_view.number == reply_consensus_view).unwrap_or(true) {
                        return result;
                    }
                }
            }
        }
    }
}
