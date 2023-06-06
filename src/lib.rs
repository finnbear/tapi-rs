#![allow(incomplete_features)]
#![feature(return_position_impl_trait_in_trait)]
#![allow(unused)]

mod ir;
mod transport;
pub(crate) mod util;

pub(crate) use ir::{
    Client as IrClient, Message as IrMessage, OpId as IrOpId, Record as IrRecord,
    Replica as IrReplica, ReplicaIndex as IrReplicaIndex, ReplicaUpcalls as IrReplicaUpcalls,
};
pub(crate) use transport::{
    Channel as ChannelTransport, ChannelRegistry, Error as TransportError,
    Message as TransportMessage, Transport,
};
