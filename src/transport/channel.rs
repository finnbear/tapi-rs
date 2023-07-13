use super::{TapirTransport, Transport};
use crate::{
    tapir::{Key, Value},
    IrMembership, IrMessage, IrReplicaUpcalls, ShardNumber, TapirReplica,
};
use rand::{thread_rng, Rng};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    ops::Range,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, SystemTime},
};

const LOG: bool = true;

pub struct Registry<U: IrReplicaUpcalls> {
    inner: Arc<RwLock<Inner<U>>>,
}

impl<M: IrReplicaUpcalls> Default for Registry<M> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

struct Inner<U: IrReplicaUpcalls> {
    #[allow(clippy::type_complexity)]
    callbacks: Vec<
        Arc<
            dyn Fn(usize, IrMessage<U, Channel<U>>) -> Option<IrMessage<U, Channel<U>>>
                + Send
                + Sync,
        >,
    >,
    shards: HashMap<ShardNumber, IrMembership<usize>>,
}

impl<U: IrReplicaUpcalls> Default for Inner<U> {
    fn default() -> Self {
        Self {
            callbacks: Vec::new(),
            shards: Default::default(),
        }
    }
}

impl<U: IrReplicaUpcalls> Registry<U> {
    pub fn channel(
        &self,
        callback: impl Fn(usize, IrMessage<U, Channel<U>>) -> Option<IrMessage<U, Channel<U>>>
            + Send
            + Sync
            + 'static,
    ) -> Channel<U> {
        let mut inner = self.inner.write().unwrap();
        let address = inner.callbacks.len();
        inner.callbacks.push(Arc::new(callback));
        Channel {
            address,
            persistent: Default::default(),
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn put_shard_addresses(&self, shard: ShardNumber, membership: IrMembership<usize>) {
        let mut inner = self.inner.write().unwrap();
        inner.shards.insert(shard, membership);
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().callbacks.len()
    }
}

pub struct Channel<U: IrReplicaUpcalls> {
    address: usize,
    persistent: Arc<Mutex<HashMap<String, String>>>,
    inner: Arc<RwLock<Inner<U>>>,
}

impl<U: IrReplicaUpcalls> Clone for Channel<U> {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            persistent: Arc::clone(&self.persistent),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<U: IrReplicaUpcalls> Channel<U> {
    #[allow(unused_variables)]
    fn should_drop(from: usize, to: usize) -> bool {
        //return false;
        //return (from == 1) ^ (to == 1);

        rand::thread_rng().gen_bool(1.0 / 5.0)
    }

    fn random_delay(range: Range<u64>) -> <Self as Transport<U>>::Sleep {
        Self::sleep(std::time::Duration::from_millis(
            thread_rng().gen_range(range),
        ))
    }
}

impl<U: IrReplicaUpcalls> Transport<U> for Channel<U> {
    type Address = usize;
    type Sleep = tokio::time::Sleep;

    fn address(&self) -> Self::Address {
        self.address
    }

    fn time(&self) -> u64 {
        self.time_offset(rand::thread_rng().gen_range(0..100))
    }

    fn time_offset(&self, offset: i64) -> u64 {
        (SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64)
            .saturating_add_signed(offset.saturating_mul(1000 * 1000 / 10))
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        tokio::time::sleep(duration / 10)
    }

    fn persist<T: Serialize>(&self, key: &str, value: Option<&T>) {
        let mut persistent = self.persistent.lock().unwrap();
        if let Some(value) = value {
            let string = serde_json::to_string(&value).unwrap();
            let display = if string.len() > 200 {
                let with_bc = bitcode::serialize(&value).unwrap();
                format!("<{} bytes ({} with bitcode)>", string.len(), with_bc.len())
            } else {
                string.clone()
            };
            eprintln!("{} persisting {key} = {display}", self.address);
            persistent.insert(key.to_owned(), string);
        } else {
            persistent.remove(key);
            unreachable!();
        }
    }

    fn persisted<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.persistent
            .lock()
            .unwrap()
            .get(key)
            .and_then(|value| serde_json::from_str(value).ok())
    }

    fn send<R: TryFrom<IrMessage<U, Self>> + Send + Debug>(
        &self,
        address: Self::Address,
        message: impl Into<IrMessage<U, Self>> + Debug,
    ) -> impl Future<Output = R> + 'static {
        let from: usize = self.address;
        if LOG {
            println!("{from} sending {message:?} to {address}");
        }
        let message = message.into();
        let inner = Arc::clone(&self.inner);
        async move {
            loop {
                Self::random_delay(10..50).await;
                let callback = {
                    let inner = inner.read().unwrap();
                    inner.callbacks.get(address).map(Arc::clone)
                };
                if let Some(callback) = callback.as_ref() {
                    if Self::should_drop(from, address) {
                        continue;
                    }
                    let result = callback(from, message.clone())
                        .map(|r| r.try_into().unwrap_or_else(|_| panic!()));
                    if let Some(result) = result {
                        let should_drop = Self::should_drop(address, from);
                        if LOG {
                            println!(
                                "{address} replying {result:?} to {from} (drop = {should_drop})"
                            );
                        }
                        if !should_drop {
                            //Self::random_delay(1..50).await;
                            break result;
                        }
                    }
                } else {
                    eprintln!("unknown address {address:?}");
                }
            }
        }
    }

    fn do_send(&self, address: Self::Address, message: impl Into<IrMessage<U, Self>> + Debug) {
        let from = self.address;
        let should_drop = Self::should_drop(self.address, address);
        if LOG {
            println!("{from} do-sending {message:?} to {address} (drop = {should_drop})");
        }
        let message = message.into();
        let inner = self.inner.read().unwrap();
        let callback = inner.callbacks.get(address).map(Arc::clone);
        drop(inner);
        if let Some(callback) = callback {
            if !should_drop {
                tokio::spawn(async move {
                    Self::random_delay(1..50).await;
                    callback(from, message);
                });
            }
        }
    }
}

impl<K: Key, V: Value> TapirTransport<K, V> for Channel<TapirReplica<K, V>> {
    fn shard_addresses(
        &self,
        shard: ShardNumber,
    ) -> impl Future<Output = IrMembership<Self::Address>> + Send + 'static {
        let inner = Arc::clone(&self.inner);
        async move {
            loop {
                {
                    let inner = inner.read().unwrap();
                    if let Some(membership) = inner.shards.get(&shard) {
                        break membership.clone();
                    }
                }

                <Self as Transport<TapirReplica<K, V>>>::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
