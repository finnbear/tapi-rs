#![allow(incomplete_features)]
#![feature(return_position_impl_trait_in_trait)]

mod ir;
mod transport;

pub(crate) use ir::{Client as IrClient, Replica as IrReplica};
pub(crate) use transport::{Error as TransportError, Message as TransportMessage, Transport};
