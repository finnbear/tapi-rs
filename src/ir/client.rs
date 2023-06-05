use crate::{Transport, TransportMessage};
use rand::{thread_rng, Rng};
use std::{future::Future, marker::PhantomData};

use super::error::Error;

pub(crate) struct Client<T: Transport, O: TransportMessage, R: TransportMessage> {
    transport: T,
    client_id: u64,
    operation_counter: u64,
    _spooky: PhantomData<(O, R)>,
}

impl<T: Transport, O: TransportMessage, R: TransportMessage> Client<T, O, R> {
    pub(crate) fn new(transport: T) -> Self {
        Self {
            transport,
            client_id: thread_rng().gen(),
            operation_counter: 0,
            _spooky: PhantomData,
        }
    }

    pub(crate) fn invoke_inconsistent(&mut self, op: O) -> impl Future<Output = Result<(), Error>> {
        std::future::ready(todo!())
    }

    pub(crate) fn invoke_consensus(
        &mut self,
        op: O,
        decide: impl Fn(Vec<R>) -> R,
    ) -> impl Future<Output = Result<R, Error>> {
        std::future::ready(todo!())
    }
}
