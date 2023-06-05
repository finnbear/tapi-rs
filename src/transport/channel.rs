use super::{Error, Message};
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

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
    senders: Vec<mpsc::Sender<(usize, M)>>,
}

impl<M> Default for Inner<M> {
    fn default() -> Self {
        Self {
            senders: Vec::new(),
        }
    }
}

impl<M> Registry<M> {
    pub(crate) fn channel(&self) -> Channel<M> {
        let (sender, receiver) = mpsc::channel(10);

        let mut inner = self.inner.lock().unwrap();
        let address = inner.senders.len();
        inner.senders.push(sender);
        Channel {
            address,
            inner: Arc::clone(&self.inner),
            receiver,
        }
    }
}

pub(crate) struct Channel<M> {
    address: usize,
    inner: Arc<Mutex<Inner<M>>>,
    receiver: mpsc::Receiver<(usize, M)>,
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
        let inner = self.inner.lock().unwrap();
        let send = inner
            .senders
            .get(address)
            .map(|sender| sender.try_send((self.address(), message)));
        drop(inner);
        std::future::ready(if let Some(send) = send {
            send.map_err(|_| Error::Unreachable)
        } else {
            Err(Error::Unreachable)
        })
    }

    fn receive(
        &mut self,
    ) -> impl Future<Output = Result<(Self::Address, Self::Message), Error>> + '_ {
        let recv = self.receiver.recv();
        async move { recv.await.ok_or(Error::Timeout) }
    }
}
