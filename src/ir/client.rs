use super::{
    error::Error, FinalizeInconsistent, Membership, Message, OpId, ProposeConsensus,
    ProposeInconsistent, ReplicaIndex,
};
use crate::{
    ir::{FinalizeConsensus, Replica, ReplyConsensus},
    util::join_until,
    Transport,
};
use rand::{thread_rng, Rng};
use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct Id(u64);

impl Id {
    fn new() -> Self {
        Self(thread_rng().gen())
    }
}

pub(crate) struct Client<T: Transport<Message = Message<O, R>>, O, R> {
    transport: T,
    id: Id,
    membership: Membership<T>,
    operation_counter: AtomicU64,
    _spooky: PhantomData<(O, R)>,
}

impl<T: Transport<Message = Message<O, R>>, O: Clone, R: Clone + PartialEq> Client<T, O, R> {
    pub(crate) fn new(membership: Membership<T>, transport: T) -> Self {
        Self {
            transport,
            id: Id::new(),
            membership,
            operation_counter: AtomicU64::new(0),
            _spooky: PhantomData,
        }
    }

    fn next_number(&self) -> u64 {
        self.operation_counter.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn invoke_inconsistent(&self, op: O) -> impl Future<Output = Result<(), Error>> {
        let number = self.next_number();
        let op_id = OpId {
            client_id: self.id,
            number,
        };
        let mut replies = HashMap::<ReplicaIndex, R>::new();
        let results = join_until(
            self.membership.iter().map(|(index, address)| {
                (
                    index,
                    self.transport.send(
                        address,
                        ProposeInconsistent {
                            op_id,
                            op: op.clone(),
                        },
                    ),
                )
            }),
            self.membership.f_plus_one(),
        );

        let membership = self.membership.clone();
        let transport = self.transport.clone();

        async move {
            let results = results.await;
            for (_, result) in results {
                assert!(matches!(result, Message::ReplyInconsistent(_)));
            }
            for (_, address) in membership {
                transport.do_send(address, FinalizeInconsistent { op_id });
            }
            Ok(())
        }
    }

    pub(crate) fn invoke_consensus(
        &mut self,
        op: O,
        decide: impl Fn(Vec<R>) -> R,
    ) -> impl Future<Output = Result<R, Error>> {
        let number = self.next_number();
        let op_id = OpId {
            client_id: self.id,
            number,
        };

        fn matching<R: PartialEq>(replies: &HashMap<ReplicaIndex, ReplyConsensus<R>>) -> bool {
            let (_, one) = replies.iter().next().unwrap();
            replies.iter().all(|(_, other)| other.result == one.result)
        }

        let f_plus_one = self.membership.f_plus_one();
        let three_over_two_f_plus_one = self.membership.three_over_two_f_plus_one();

        let results = join_until(
            self.membership.iter().map(|(index, address)| {
                (
                    index,
                    self.transport.send::<ReplyConsensus<R>>(
                        address,
                        ProposeConsensus {
                            op_id,
                            op: op.clone(),
                        },
                    ),
                )
            }),
            move |results: &HashMap<ReplicaIndex, ReplyConsensus<R>>| {
                (results.len() >= three_over_two_f_plus_one && matching(results))
                    || results.len() >= f_plus_one
            },
        );

        let membership = self.membership.clone();
        let transport = self.transport.clone();

        async move {
            let results = results.await;
            if matching(&results) {
                // Fast path.
                let result = results.into_iter().next().unwrap().1.result;
                for (_, address) in membership {
                    transport.do_send(
                        address,
                        FinalizeConsensus {
                            op_id,
                            result: result.clone(),
                        },
                    );
                }
                return Ok(result);
            }

            todo!()

            //Ok(())
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
