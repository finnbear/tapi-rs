use std::fmt::Debug;

use crate::{transport::Transport, IrClient, IrMembership, IrMessage};

use super::{Reply, Request};

pub(crate) struct Client<K, V, T: Transport<Message = IrMessage<Request<K, V>, Reply<V>>>> {
    inner: IrClient<T, Request<K, V>, Reply<V>>,
}

impl<
        K: Debug + Clone,
        V: PartialEq + Debug + Clone,
        T: Transport<Message = IrMessage<Request<K, V>, Reply<V>>>,
    > Client<K, V, T>
{
    pub(crate) fn new(membership: IrMembership<T>, transport: T) -> Self {
        Self {
            inner: IrClient::new(membership, transport),
        }
    }
}
