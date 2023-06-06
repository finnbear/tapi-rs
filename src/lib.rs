#![allow(incomplete_features)]
#![feature(return_position_impl_trait_in_trait)]
#![allow(unused)]

mod ir;
mod transport;

pub(crate) use ir::{
    Client as IrClient, Message as IrMessage, Replica as IrReplica, ReplicaIndex as IrReplicaIndex,
};
pub(crate) use transport::{
    Channel as ChannelTransport, ChannelRegistry, Error as TransportError,
    Message as TransportMessage, Transport,
};
