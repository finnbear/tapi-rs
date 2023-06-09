use super::{
    error::Error, FinalizeInconsistent, Membership, MembershipSize, Message, OpId,
    ProposeConsensus, ProposeInconsistent, ReplicaIndex, ViewNumber,
};
use crate::{
    ir::{membership, Confirm, FinalizeConsensus, Replica, ReplyConsensus, ReplyInconsistent},
    util::join_until,
    Transport,
};
use rand::{thread_rng, Rng};
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct Id(u64);

impl Id {
    fn new() -> Self {
        Self(thread_rng().gen())
    }
}

pub(crate) struct Client<T: Transport<Message = Message<O, R>>, O, R> {
    id: Id,
    inner: Arc<Inner<T>>,
    _spooky: PhantomData<(O, R)>,
}

struct Inner<T: Transport> {
    transport: T,
    sync: Mutex<SyncInner<T>>,
}

struct SyncInner<T: Transport> {
    operation_counter: u64,
    membership: Membership<T>,
}

impl<T: Transport> SyncInner<T> {
    fn next_number(&mut self) -> u64 {
        let mut ret = self.operation_counter;
        self.operation_counter += 1;
        ret
    }
}

impl<T: Transport<Message = Message<O, R>>, O: Clone, R: Clone + PartialEq + Debug>
    Client<T, O, R>
{
    pub(crate) fn new(membership: Membership<T>, transport: T) -> Self {
        Self {
            id: Id::new(),
            inner: Arc::new(Inner {
                transport,
                sync: Mutex::new(SyncInner {
                    operation_counter: 0,
                    membership,
                }),
            }),
            _spooky: PhantomData,
        }
    }

    pub(crate) fn invoke_inconsistent(&self, op: O) -> impl Future<Output = Result<(), Error>> {
        let client_id = self.id;
        let inner = Arc::clone(&self.inner);

        let mut sync = inner.sync.lock().unwrap();
        let number = sync.next_number();
        let op_id = OpId {
            client_id: self.id,
            number,
        };
        drop(sync);

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
                let sync = inner.sync.lock().unwrap();
                let membership_size = sync.membership.size();
                let future = join_until(
                    sync.membership.iter().map(|(index, address)| {
                        (
                            index,
                            inner.transport.send(
                                address,
                                ProposeInconsistent {
                                    op_id,
                                    op: op.clone(),
                                },
                            ),
                        )
                    }),
                    move |results: &HashMap<ReplicaIndex, ReplyInconsistent>, timeout: bool| {
                        has_quorum(membership_size, results, !timeout)
                    },
                    Some(Duration::from_secs(1)),
                );
                drop(sync);

                let results = future.await;
                if !has_quorum(membership_size, &results, true) {
                    continue;
                }

                let sync = inner.sync.lock().unwrap();
                for (_, address) in &sync.membership {
                    inner
                        .transport
                        .do_send(address, FinalizeInconsistent { op_id });
                }
                return Ok(());
            }
        }
    }

    pub(crate) fn invoke_consensus(
        &self,
        op: O,
        decide: impl Fn(Vec<R>) -> R,
    ) -> impl Future<Output = R> {
        fn get_quorum<R: PartialEq>(
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
                            && (!matching_results || other.result == candidate.result)
                    })
                    .count();
                if matches >= required {
                    return Some((candidate.view_number, &candidate.result));
                }
            }
            None
        }

        let client_id = self.id;
        let inner = Arc::clone(&self.inner);

        async move {
            'retry: loop {
                let mut sync = inner.sync.lock().unwrap();
                let number = sync.next_number();
                let op_id = OpId { client_id, number };
                let membership_size = sync.membership.size();

                let future = join_until(
                    sync.membership.iter().map(|(index, address)| {
                        (
                            index,
                            inner.transport.send::<ReplyConsensus<R>>(
                                address,
                                ProposeConsensus {
                                    op_id,
                                    op: op.clone(),
                                },
                            ),
                        )
                    }),
                    move |results: &HashMap<ReplicaIndex, ReplyConsensus<R>>, timeout: bool| {
                        // TODO: Liveness
                        get_quorum(membership_size, results, !timeout).is_some()
                    },
                    Some(Duration::from_secs(1)),
                );
                drop(sync);

                let results = future.await;

                let sync = inner.sync.lock().unwrap();
                let membership_size = sync.membership.size();
                if let Some((_, result)) = get_quorum(membership_size, &results, true) {
                    // Fast path.
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
                } else if let Some((view_number, _)) = get_quorum(membership_size, &results, false)
                {
                    // Slow path.
                    let result = decide(
                        results
                            .into_values()
                            .filter(|rc| rc.view_number == view_number)
                            .map(|rc| rc.result)
                            .collect::<Vec<_>>(),
                    );
                    let future = join_until(
                        sync.membership.iter().map(|(index, address)| {
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
                        }),
                        membership_size.f_plus_one(),
                        None,
                    );
                    drop(sync);
                    future.await;
                    return result;
                }
            }
        }
    }

    pub(crate) fn receive(
        &mut self,
        sender: T::Address,
        message: Message<O, R>,
    ) -> Option<Message<O, R>> {
        None
    }
}
