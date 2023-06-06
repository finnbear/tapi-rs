mod channel;
mod error;
mod message;

pub(crate) use channel::{Channel, Registry as ChannelRegistry};
pub(crate) use error::Error;
pub(crate) use message::Message;
use std::future::Future;

pub(crate) trait Transport {
    type Address: 'static;
    type Message: Message;

    fn address(&self) -> Self::Address;
    fn send(
        &self,
        address: Self::Address,
        message: Self::Message,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'static;
    fn do_send(&self, address: Self::Address, message: Self::Message) {
        tokio::spawn(self.send(address, message));
    }
}
