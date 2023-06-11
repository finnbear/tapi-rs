mod channel;
mod error;
mod message;

pub(crate) use channel::{Channel, Registry as ChannelRegistry};
pub(crate) use error::Error;
pub(crate) use message::Message;
use std::{fmt::Debug, future::Future, time::Duration};

pub(crate) trait Transport: Clone + Send + Sync + 'static {
    type Address: Copy + Debug + Send + 'static;
    type Sleep: Future<Output = ()> + Send;
    type Message: Message;

    /// Get own address.
    fn address(&self) -> Self::Address;

    fn sleep(duration: Duration) -> Self::Sleep;

    /// Send/retry, ignoring any errors, until there is a reply.
    fn send<R: TryFrom<Self::Message>>(
        &self,
        address: Self::Address,
        message: impl Into<Self::Message> + Debug,
    ) -> impl Future<Output = R> + Send + 'static;

    /// Send once and don't wait for a reply.
    fn do_send(&self, address: Self::Address, message: impl Into<Self::Message> + Debug);
}
