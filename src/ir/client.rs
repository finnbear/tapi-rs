use super::{error::Error, Message, OpId, Propose, ReplicaIndex};
use crate::Transport;
use rand::{thread_rng, Rng};
use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct Id(u64);

pub(crate) struct Client<T: Transport<Message = Message<O, R>>, O, R> {
    transport: T,
    id: Id,
    membership: HashMap<ReplicaIndex, T::Address>,
    operation_counter: AtomicU64,
    _spooky: PhantomData<(O, R)>,
}

impl<T: Transport<Message = Message<O, R>>, O: Clone, R> Client<T, O, R> {
    pub(crate) fn new(transport: T, membership: HashMap<ReplicaIndex, T::Address>) -> Self {
        Self {
            transport,
            id: Id(thread_rng().gen()),
            membership,
            operation_counter: AtomicU64::new(0),
            _spooky: PhantomData,
        }
    }

    pub(crate) fn invoke_inconsistent(&self, op: O) -> impl Future<Output = Result<(), Error>> {
        let number = self.operation_counter.fetch_add(1, Ordering::Relaxed);
        let mut replies = HashMap::<ReplicaIndex, R>::new();
        for &address in self.membership.values() {
            self.transport.send(
                address,
                Message::Propose(Propose {
                    op_id: OpId {
                        client_id: self.id,
                        number,
                    },
                    op: op.clone(),
                }),
            );
        }
        std::future::ready(Ok(()))
    }

    pub(crate) fn invoke_consensus(
        &mut self,
        op: O,
        decide: impl Fn(Vec<R>) -> R,
    ) -> impl Future<Output = Result<R, Error>> {
        std::future::ready(todo!())
    }

    pub(crate) fn receive(
        &mut self,
        sender: T::Address,
        message: Message<O, R>,
    ) -> Option<Message<O, R>> {
        None
    }
}
