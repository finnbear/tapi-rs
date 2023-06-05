use crate::transport::Transport;
use std::marker::PhantomData;

use super::error::Error;

pub(crate) struct Client<T: Transport, O: TransportMessage, R: TransportMessage> {
    transport: T,
    client_id: u64,
    operation_counter: u64,
    _spooky: PhantomData<(O, R)>,
}

impl<T: Transport, O: TransportMessage, R: TransportMessage> Client<O, R> {
    pub(crate) fn invoke_inconsistent(&mut self, op: O) -> impl Future<Item = Result<(), Error>> {}

    pub(crate) fn invoke_consensus(
        &mut self,
        op: O,
        decide: Fn(Vec<R>) -> R,
    ) -> impl Future<Item = Result<R, Error>> {
    }
}
