#![allow(incomplete_features)]
#![feature(return_position_impl_trait_in_trait)]
#![feature(int_roundings)]
#![feature(let_chains)]
#![feature(btree_cursors)]
#![allow(unused)]

mod ir;
mod mvcc;
mod occ;
mod tapir;
mod transport;
pub(crate) mod util;

pub(crate) use ir::{
    Client as IrClient, ClientId as IrClientId, Membership as IrMembership,
    MembershipSize as IrMembershipSize, Message as IrMessage, OpId as IrOpId, Record as IrRecord,
    RecordEntry as IrRecordEntry, Replica as IrReplica, ReplicaIndex as IrReplicaIndex,
    ReplicaUpcalls as IrReplicaUpcalls,
};
pub(crate) use mvcc::Store as MvccStore;
pub(crate) use occ::{
    PrepareResult as OccPrepareResult, Store as OccStore, Transaction as OccTransaction,
    TransactionId as OccTransactionId,
};
pub(crate) use transport::{
    Channel as ChannelTransport, ChannelRegistry, Error as TransportError,
    Message as TransportMessage, Transport,
};
