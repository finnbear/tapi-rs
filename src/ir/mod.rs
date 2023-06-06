mod client;
mod error;
mod membership;
mod message;
mod op;
mod record;
mod replica;
mod view;

#[cfg(test)]
mod tests;

pub(crate) use client::{Client, Id as ClientId};
pub(crate) use error::Error;
pub(crate) use membership::Membership;
pub(crate) use message::{
    Confirm, FinalizeConsensus, FinalizeInconsistent, Message, ProposeConsensus,
    ProposeInconsistent, ReplyConsensus, ReplyInconsistent,
};
pub(crate) use op::Id as OpId;
pub(crate) use record::{Consistency, Entry as RecordEntry, Record, State as RecordEntryState};
pub(crate) use replica::{
    Index as ReplicaIndex, Replica, State as ReplicaState, Upcalls as ReplicaUpcalls,
};
pub(crate) use view::{Number as ViewNumber, View};
