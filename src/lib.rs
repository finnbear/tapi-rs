#![allow(incomplete_features)]
#![feature(return_position_impl_trait_in_trait)]
#![feature(int_roundings)]
#![feature(let_chains)]
#![allow(unused)]

mod ir;
mod mvcc;
mod occ;
mod transport;
pub(crate) mod util;

pub(crate) use ir::{
    Client as IrClient, ClientId as IrClientId, Membership as IrMembership,
    MembershipSize as IrMembershipSize, Message as IrMessage, OpId as IrOpId, Record as IrRecord,
    RecordEntry as IrRecordEntry, Replica as IrReplica, ReplicaIndex as IrReplicaIndex,
    ReplicaUpcalls as IrReplicaUpcalls,
};
pub(crate) use transport::{
    Channel as ChannelTransport, ChannelRegistry, Error as TransportError,
    Message as TransportMessage, Transport,
};
