use super::{Error, Message};
use std::future::Future;
use std::sync::{Arc, RwLock};

pub(crate) struct Registry<M> {
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
    pub(crate) fn channel(
        &self,
        callback: impl Fn(usize, M) -> Option<M> + Send + Sync + 'static,
    ) -> Channel<M> {
        let mut inner = self.inner.write().unwrap();
        let address = inner.callbacks.len();
        inner.callbacks.push(Arc::new(callback));
        Channel {
            address,
            inner: Arc::clone(&self.inner),
        }
    }
}

pub(crate) struct Channel<M> {
    address: usize,
    inner: Arc<RwLock<Inner<M>>>,
}

impl<M> Clone for Channel<M> {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<M> Channel<M> {
    fn should_drop(_from: usize, _to: usize) -> bool {
        use rand::Rng;
        rand::thread_rng().gen_bool(0.1)
    }
}

impl<M: Message> super::Transport for Channel<M> {
    type Address = usize;
    type Message = M;

    fn address(&self) -> Self::Address {
        self.address
    }

    fn send<R: TryFrom<M>>(
        &self,
        address: Self::Address,
        message: impl Into<Self::Message>,
    ) -> impl Future<Output = R> + 'static {
        let message = message.into();
        let inner = self.inner.read().unwrap();
        let callback = inner.callbacks.get(address).map(Arc::clone);
        drop(inner);
        let from = self.address;
        async move {
            loop {
                if let Some(callback) = callback.as_ref() {
                    if Self::should_drop(from, address) {
                        continue;
                    }
                    let reply = callback(from, message.clone());
                    if let Some(reply) = reply {
                        if let Ok(result) = reply.try_into() {
                            if !Self::should_drop(address, from) {
                                break result;
                            }
                        } else {
                            println!("unexpected type");
                        }
                    }
                } else {
                    println!("unknown address {address:?}");
                }
                tokio::time::sleep(std::time::Duration::from_millis(100));
            }
        }
    }

    fn do_send(&self, address: Self::Address, message: impl Into<Self::Message>) {
        let inner = self.inner.read().unwrap();
        let callback = inner.callbacks.get(address).map(Arc::clone);
        drop(inner);
        if let Some(callback) = callback {
            if !Self::should_drop(self.address, address) {
                callback(self.address, message.into());
            }
        }
    }
}
