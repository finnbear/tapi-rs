mod channel;
mod message;

pub use channel::{Channel, Registry as ChannelRegistry};
pub use message::Message;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt::Debug,
    future::Future,
    time::{Duration, SystemTime},
};

pub trait Transport: Clone + Send + Sync + 'static {
    type Address: Copy + Debug + Send + 'static;
    type Sleep: Future<Output = ()> + Send;
    type Message: Message;

    /// Get own address.
    fn address(&self) -> Self::Address;

    /// Get time (nanos since epoch).
    fn time(&self) -> u64 {
        use rand::Rng;
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    /// Sleep for duration.
    fn sleep(duration: Duration) -> Self::Sleep;

    /// Synchronously persist a key-value pair.
    fn persist<T: Serialize>(&self, key: &str, value: Option<&T>);

    /// Synchronously load a persisted key-value pair.
    fn persisted<T: DeserializeOwned>(&self, key: &str) -> Option<T>;

    /// Send/retry, ignoring any errors, until there is a reply.
    fn send<R: TryFrom<Self::Message> + Send + Debug>(
        &self,
        address: Self::Address,
        message: impl Into<Self::Message> + Debug,
    ) -> impl Future<Output = R> + Send + 'static;

    /// Send once and don't wait for a reply.
    fn do_send(&self, address: Self::Address, message: impl Into<Self::Message> + Debug);
}
