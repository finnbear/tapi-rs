mod client;
mod error;
mod message;
mod op;
mod record;
mod replica;

#[cfg(test)]
mod tests;

pub(crate) use client::{Client, Id as ClientId};
pub(crate) use error::Error;
pub(crate) use message::{
    Confirm, FinalizeConsensus, FinalizeInconsistent, Message, ProposeConsensus,
    ProposeInconsistent, ReplyConsensus, ReplyInconsistent,
};
pub(crate) use op::Id as OpId;
pub(crate) use record::{Entry as RecordEntry, Record, State as RecordState};
pub(crate) use replica::{
    Index as ReplicaIndex, Replica, State as ReplicaState, Upcalls as ReplicaUpcalls,
};
