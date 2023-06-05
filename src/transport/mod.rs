mod error;
mod message;
use std::future::Future;

pub(crate) use error::Error;
pub(crate) use message::Message;

pub(crate) trait Transport {
    type Address;

    fn send<M: Message>(&self, message: M);
    fn receive<M>(&self) -> impl Future<Output = Result<M, Error>>;
}
