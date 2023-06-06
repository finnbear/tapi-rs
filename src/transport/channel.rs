use super::{Error, Message};
use std::future::Future;
use std::sync::{Arc, Mutex};

pub(crate) struct Registry<M> {
    inner: Arc<Mutex<Inner<M>>>,
}

impl<M> Default for Registry<M> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

struct Inner<M> {
    callbacks: Vec<Arc<dyn Fn(usize, M) + Send + Sync>>,
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
        callback: impl Fn(usize, M) + Send + Sync + 'static,
    ) -> Channel<M> {
        let mut inner = self.inner.lock().unwrap();
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
    inner: Arc<Mutex<Inner<M>>>,
}

impl<M: Message> Channel<M> {
    fn send_impl(&self, address: usize, message: M) -> Result<(), Error> {
        let inner = self.inner.lock().unwrap();
        if let Some(callback) = inner.callbacks.get(address) {
            let callback = Arc::clone(&callback);
            let from = self.address;
            std::thread::spawn(move || {
                callback(from, message);
            });
            Ok(())
        } else {
            Err(Error::Unreachable)
        }
    }
}

impl<M: Message> super::Transport for Channel<M> {
    type Address = usize;
    type Message = M;

    fn address(&self) -> Self::Address {
        self.address
    }

    fn send(
        &self,
        address: Self::Address,
        message: Self::Message,
    ) -> impl Future<Output = Result<(), Error>> + 'static {
        std::future::ready(self.send_impl(address, message))
    }

    fn do_send(&self, address: Self::Address, message: Self::Message) {
        let _ = self.send_impl(address, message);
    }
}
