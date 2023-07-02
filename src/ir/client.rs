use super::{
    Confirm, DoViewChange, FinalizeConsensus, FinalizeInconsistent, Membership, MembershipSize,
    Message, OpId, ProposeConsensus, ProposeInconsistent, ReplicaIndex, ReplicaUpcalls,
    ReplyConsensus, ReplyInconsistent, ReplyUnlogged, RequestUnlogged, ViewNumber,
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
pub struct Client<U: ReplicaUpcalls, T: Transport> {
    id: Id,
    inner: Arc<Inner<T>>,
    _spooky: PhantomData<U>,
}

impl<U: ReplicaUpcalls, T: Transport> Clone for Client<U, T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            inner: Arc::clone(&self.inner),
            _spooky: PhantomData,
        }
    }
}

struct Inner<T: Transport> {
    transport: T,
    sync: Mutex<SyncInner<T>>,
}

struct SyncInner<T: Transport> {
    operation_counter: u64,
    membership: Membership<T>,
    recent: ViewNumber,
}

impl<T: Transport> SyncInner<T> {
    fn next_number(&mut self) -> u64 {
        let ret = self.operation_counter;
        self.operation_counter += 1;
        ret
    }
}

impl<U: ReplicaUpcalls, T: Transport<Message = Message<U>>> Client<U, T> {
    pub fn new(membership: Membership<T>, transport: T) -> Self {
        Self {
            id: Id::new(),
            inner: Arc::new(Inner {
                transport,
                sync: Mutex::new(SyncInner {
                    operation_counter: 0,
                    membership,
                    recent: ViewNumber(0),
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

    fn notify_old_views(
        transport: &T,
        sync: &mut SyncInner<T>,
        views: impl IntoIterator<Item = (ReplicaIndex, ViewNumber)> + Clone,
    ) {
        if let Some(latest_view) = views.clone().into_iter().map(|(_, n)| n).max() {
            sync.recent = sync.recent.max(latest_view);
            for (index, view_number) in views {
                if view_number < latest_view {
                    transport.do_send(
                        sync.membership.get(index).unwrap(),
                        Message::<U>::DoViewChange(DoViewChange {
                            view_number: latest_view,
                            addendum: None,
                        }),
                    )
                }
            }
        }
    }

    /// Unlogged request against any single replica.
    pub fn invoke_unlogged(&self, op: U::UO) -> impl Future<Output = U::UR> {
        let inner = Arc::clone(&self.inner);
        let (index, count) = {
            let sync = inner.sync.lock().unwrap();
            let count = sync.membership.len();
            (thread_rng().gen_range(0..count), count)
        };

        async move {
            let mut futures = FuturesUnordered::new();

            loop {
                debug_assert!(futures.len() < count);
                {
                    let sync = inner.sync.lock().unwrap();
                    let address = sync
                        .membership
                        .get(ReplicaIndex((index + futures.len()) % count))
                        .unwrap();
                    drop(sync);
                    let future = inner
                        .transport
                        .send::<ReplyUnlogged<U::UR>>(address, RequestUnlogged { op: op.clone() });
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
                sync.recent = sync.recent.max(response.view_number);
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
        Join<ReplicaIndex, impl Future<Output = ReplyUnlogged<U::UR>>>,
        MembershipSize,
    ) {
        let sync = self.inner.sync.lock().unwrap();
        let membership_size = sync.membership.size();

        let future = join(sync.membership.iter().map(|(index, address)| {
            (
                index,
                self.inner
                    .transport
                    .send::<ReplyUnlogged<U::UR>>(address, RequestUnlogged { op: op.clone() }),
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

        fn has_ancient(results: &HashMap<ReplicaIndex, ReplyInconsistent>) -> bool {
            results.values().any(|v| v.state.is_none())
        }

        fn has_quorum(
            membership: MembershipSize,
            results: &HashMap<ReplicaIndex, ReplyInconsistent>,
            check_views: bool,
        ) -> bool {
            let threshold = membership.f_plus_one();
            if check_views {
                for result in results.values() {
                    let matching = results
                        .values()
                        .filter(|other| other.view_number == result.view_number)
                        .count();
                    if matching >= threshold {
                        return true;
                    }
                }
                false
            } else {
                results.len() >= threshold
            }
        }

        async move {
            loop {
                let (membership_size, future) = {
                    let sync = inner.sync.lock().unwrap();
                    let membership_size = sync.membership.size();

                    let future = join(sync.membership.iter().map(|(index, address)| {
                        (
                            index,
                            inner.transport.send::<ReplyInconsistent>(
                                address,
                                ProposeInconsistent {
                                    op_id,
                                    op: op.clone(),
                                    recent: sync.recent,
                                },
                            ),
                        )
                    }));
                    (membership_size, future)
                };

                let mut timeout = std::pin::pin!(T::sleep(Duration::from_millis(250)));

                let results = future
                    .until(
                        move |results: &HashMap<ReplicaIndex, ReplyInconsistent>,
                              cx: &mut Context<'_>| {
                            has_ancient(results)
                                || has_quorum(
                                    membership_size,
                                    results,
                                    timeout.as_mut().poll(cx).is_ready(),
                                )
                        },
                    )
                    .await;

                let mut sync = inner.sync.lock().unwrap();
                Self::notify_old_views(
                    &inner.transport,
                    &mut *sync,
                    results.iter().map(|(i, r)| (*i, r.view_number)),
                );

                if has_ancient(&results) || !has_quorum(membership_size, &results, true) {
                    continue;
                }

                // println!("finalizing to membership: {:?}", sync.membership);
                for (_, address) in &sync.membership {
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
        fn has_ancient<R>(replies: &HashMap<ReplicaIndex, ReplyConsensus<R>>) -> bool {
            replies.values().any(|v| v.result_state.is_none())
        }

        fn get_finalized<R>(replies: &HashMap<ReplicaIndex, ReplyConsensus<R>>) -> Option<&R> {
            replies
                .values()
                .find(|r| r.result_state.as_ref().unwrap().1.is_finalized())
                .map(|r| &r.result_state.as_ref().unwrap().0)
        }

        fn get_quorum<R: PartialEq + Debug>(
            membership: MembershipSize,
            replies: &HashMap<ReplicaIndex, ReplyConsensus<R>>,
            matching_results: bool,
        ) -> Option<(ViewNumber, &R)> {
            let required = if matching_results {
                membership.three_over_two_f_plus_one()
            } else {
                membership.f_plus_one()
            };
            for candidate in replies.values() {
                let matches = replies
                    .values()
                    .filter(|other| {
                        other.view_number == candidate.view_number
                            && (!matching_results
                                || other.result_state.as_ref().unwrap().0
                                    == candidate.result_state.as_ref().unwrap().0)
                    })
                    .count();
                if matches >= required {
                    return Some((
                        candidate.view_number,
                        &candidate.result_state.as_ref().unwrap().0,
                    ));
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
                    let membership_size = sync.membership.size();

                    let future = join(sync.membership.iter().map(|(index, address)| {
                        (
                            index,
                            inner.transport.send::<ReplyConsensus<U::CR>>(
                                address,
                                ProposeConsensus {
                                    op_id,
                                    op: op.clone(),
                                    recent: sync.recent,
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
                        move |results: &HashMap<ReplicaIndex, ReplyConsensus<U::CR>>,
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

                fn get_quorum_view(
                    membership: MembershipSize,
                    results: &HashMap<ReplicaIndex, Confirm>,
                ) -> Option<ViewNumber> {
                    let threshold = membership.f_plus_one();
                    for result in results.values() {
                        let matching = results
                            .values()
                            .filter(|other| other.view_number == result.view_number)
                            .count();
                        if matching >= threshold {
                            return Some(result.view_number);
                        }
                    }
                    None
                }

                let next = {
                    let mut sync = inner.sync.lock().unwrap();

                    Self::notify_old_views(
                        &inner.transport,
                        &mut *sync,
                        results.iter().map(|(i, r)| (*i, r.view_number)),
                    );

                    if has_ancient(&results) {
                        // Our view number was very old.
                        continue;
                    }

                    let membership_size = sync.membership.size();
                    let finalized = get_finalized(&results);
                    //println!("checking quorum: {}", finalized.is_some());
                    if finalized.is_none() && let Some((_, result)) = get_quorum(membership_size, &results, true) {
                        // Fast path.
                        // eprintln!("doing fast path");
                        for (_, address) in &sync.membership {
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
                            let view_number =
                                get_quorum(membership_size, &results, false).map(|(v, _)| v);
                            view_number.map(|view_number| {
                                let results = results
                                    .into_values()
                                    .filter(|rc| rc.view_number == view_number)
                                    .map(|rc| rc.result_state.unwrap().0);
                                let mut totals = HashMap::new();
                                for result in results {
                                    *totals.entry(result).or_default() += 1;
                                }
                                (decide(totals, membership_size), Some(view_number))
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
                        let future = join(sync.membership.iter().map(|(index, address)| {
                            (
                                index,
                                inner.transport.send::<Confirm>(
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
                            |results: &HashMap<ReplicaIndex, Confirm>, cx: &mut Context<'_>| {
                                get_quorum_view(membership_size, results).is_some()
                                    || (soft_timeout.as_mut().poll(cx).is_ready()
                                        && results.len() >= membership_size.f_plus_one())
                                    || hard_timeout.as_mut().poll(cx).is_ready()
                            },
                        )
                        .await;
                    let mut sync = inner.sync.lock().unwrap();
                    Self::notify_old_views(
                        &inner.transport,
                        &mut *sync,
                        results.iter().map(|(i, r)| (*i, r.view_number)),
                    );
                    if let Some(quorum_view) = get_quorum_view(membership_size, &results) && reply_consensus_view.map(|reply_consensus_view| quorum_view == reply_consensus_view).unwrap_or(true) {
                        return result;
                    }
                }
            }
        }
    }
}
