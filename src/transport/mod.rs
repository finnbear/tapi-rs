pub use channel::{Channel, Registry as ChannelRegistry};
pub use message::Message;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt::Debug,
    future::Future,
    time::{Duration, SystemTime},
};

mod channel;
mod message;
pub trait Transport: Clone + Send + Sync + 'static {
    type Address: Copy + Eq + Debug + Send + Serialize + DeserializeOwned + 'static;
    type Sleep: Future<Output = ()> + Send;

    /// Get own address.
    fn address(&self) -> Self::Address;

    /// Get time (nanos since epoch).
    fn time(&self) -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    fn time_offset(&self, offset: i64) -> u64 {
        self.time()
            .saturating_add_signed(offset.saturating_mul(1000 * 1000))
    }

    /// Sleep for duration.
    fn sleep(duration: Duration) -> Self::Sleep;

    /// Synchronously persist a key-value pair. Any future calls
    /// to `persisted` should return this value unless/until it
    /// is overwritten.
    fn persist<T: Serialize>(&self, key: &str, value: Option<&T>);

    /// Synchronously load a persisted key-value pair.
    fn persisted<T: DeserializeOwned>(&self, key: &str) -> Option<T>;

    /// Receive sent messages except replies (replaces any previously sent callback).
    fn receive<M: Debug + Serialize + DeserializeOwned>(
        &self,
        callback: impl Fn(usize, M) -> Option<M> + Send + Sync + 'static,
    );

    /// Send/retry, ignoring any errors, until there is a reply.
    fn send<M: Debug + Serialize + Deserialize>(
        &self,
        address: Self::Address,
        message: &M,
    ) -> impl Future<Output = M> + Send + 'static;

    /// Send once and don't wait for a reply.
    fn do_send<M: Debug + Serialize>(&self, address: Self::Address, message: &M);
}
