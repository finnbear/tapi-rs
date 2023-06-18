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
pub mod util;

pub use ir::{
    Client as IrClient, ClientId as IrClientId, Membership as IrMembership,
    MembershipSize as IrMembershipSize, Message as IrMessage, OpId as IrOpId, Record as IrRecord,
    RecordConsensusEntry as IrRecordConsensusEntry, Replica as IrReplica,
    ReplicaIndex as IrReplicaIndex, ReplicaUpcalls as IrReplicaUpcalls,
};
pub use mvcc::Store as MvccStore;
pub use occ::{
    PrepareResult as OccPrepareResult, Store as OccStore, Timestamp as OccTimestamp,
    Transaction as OccTransaction, TransactionId as OccTransactionId,
};
pub use tapir::{Client as TapirClient, Replica as TapirReplica};
pub use transport::{Channel as ChannelTransport, ChannelRegistry};
pub use transport::{Message as TransportMessage, Transport};
