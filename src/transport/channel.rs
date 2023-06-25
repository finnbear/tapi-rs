use rand::{thread_rng, Rng};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::{Message, Transport};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::ops::Range;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

pub struct Registry<M> {
    inner: Arc<RwLock<Inner<M>>>,
}

impl<M> Default for Registry<M> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

struct Inner<M> {
    #[allow(clippy::type_complexity)]
    callbacks: Vec<Arc<dyn Fn(usize, M) -> Option<M> + Send + Sync>>,
}

impl<M> Default for Inner<M> {
    fn default() -> Self {
        Self {
            callbacks: Vec::new(),
        }
    }
}

impl<M> Registry<M> {
    pub fn channel(
        &self,
        callback: impl Fn(usize, M) -> Option<M> + Send + Sync + 'static,
    ) -> Channel<M> {
        let mut inner = self.inner.write().unwrap();
        let address = inner.callbacks.len();
        inner.callbacks.push(Arc::new(callback));
        Channel {
            address,
            persistent: Default::default(),
            inner: Arc::clone(&self.inner),
        }
    }
}

pub struct Channel<M> {
    address: usize,
    persistent: Arc<Mutex<HashMap<String, String>>>,
    inner: Arc<RwLock<Inner<M>>>,
}

impl<M> Clone for Channel<M> {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            persistent: Arc::clone(&self.persistent),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<M: Message> Channel<M> {
    fn should_drop(from: usize, to: usize) -> bool {
        //return false;
        // return from == 1 || to == 1;

        use rand::Rng;
        rand::thread_rng().gen_bool(1.0 / 5.0)
    }

    fn random_delay(range: Range<u64>) -> <Self as Transport>::Sleep {
        Self::sleep(std::time::Duration::from_millis(
            thread_rng().gen_range(range),
        ))
    }
}

impl<M: Message> Transport for Channel<M> {
    type Address = usize;
    type Sleep = tokio::time::Sleep;
    type Message = M;

    fn address(&self) -> Self::Address {
        self.address
    }

    fn time(&self) -> u64 {
        use rand::Rng;
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
            + rand::thread_rng().gen_range(0..10 * 1000 * 1000)
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        tokio::time::sleep(duration / 10)
    }

    fn persist<T: Serialize>(&self, key: &str, value: Option<&T>) {
        let mut persistent = self.persistent.lock().unwrap();
        if let Some(value) = value {
            let string = serde_json::to_string(&value).unwrap();
            let display = if string.len() > 100 {
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

    fn send<R: TryFrom<M> + Send + Debug>(
        &self,
        address: Self::Address,
        message: impl Into<Self::Message> + Debug,
    ) -> impl Future<Output = R> + 'static {
        let from: usize = self.address;
        //println!("{from} sending {message:?} to {address}");
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
                        // println!("{address} replying {result:?} to {from} (drop = {should_drop})");
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

    fn do_send(&self, address: Self::Address, message: impl Into<Self::Message> + Debug) {
        let from = self.address;
        let should_drop = Self::should_drop(self.address, address);
        //println!("{from} do-sending {message:?} to {address} (drop = {should_drop})");
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
