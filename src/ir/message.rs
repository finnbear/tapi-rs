use super::{
    record::RecordImpl, OpId, RecordEntryState, ReplicaIndex, ReplicaUpcalls, View, ViewNumber,
};
use crate::Transport;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub type Message<U, T> = MessageImpl<
    <U as ReplicaUpcalls>::UO,
    <U as ReplicaUpcalls>::UR,
    <U as ReplicaUpcalls>::IO,
    <U as ReplicaUpcalls>::CO,
    <U as ReplicaUpcalls>::CR,
    <T as Transport<U>>::Address,
>;

#[derive(Clone, derive_more::From, derive_more::TryInto, Serialize, Deserialize)]
pub enum MessageImpl<UO, UR, IO, CO, CR, A> {
    RequestUnlogged(RequestUnlogged<UO>),
    ReplyUnlogged(ReplyUnlogged<UR, A>),
    ProposeInconsistent(ProposeInconsistent<IO>),
    ProposeConsensus(ProposeConsensus<CO>),
    ReplyInconsistent(ReplyInconsistent<A>),
    ReplyConsensus(ReplyConsensus<CR, A>),
    FinalizeInconsistent(FinalizeInconsistent),
    FinalizeConsensus(FinalizeConsensus<CR>),
    Confirm(Confirm<A>),
    DoViewChange(DoViewChange<IO, CO, CR, A>),
    StartView(StartView<IO, CO, CR, A>),
}

impl<UO: Debug, UR: Debug, IO: Debug, CO: Debug, CR: Debug, A: Debug> Debug
    for MessageImpl<UO, UR, IO, CO, CR, A>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RequestUnlogged(r) => Debug::fmt(r, f),
            Self::ReplyUnlogged(r) => Debug::fmt(r, f),
            Self::ProposeInconsistent(r) => Debug::fmt(r, f),
            Self::ProposeConsensus(r) => Debug::fmt(r, f),
            Self::ReplyInconsistent(r) => Debug::fmt(r, f),
            Self::ReplyConsensus(r) => Debug::fmt(r, f),
            Self::FinalizeInconsistent(r) => Debug::fmt(r, f),
            Self::FinalizeConsensus(r) => Debug::fmt(r, f),
            Self::Confirm(r) => Debug::fmt(r, f),
            Self::DoViewChange(r) => Debug::fmt(r, f),
            Self::StartView(r) => Debug::fmt(r, f),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestUnlogged<UO> {
    pub op: UO,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyUnlogged<UR, A> {
    pub result: UR,
    pub view: View<A>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeInconsistent<IO> {
    pub op_id: OpId,
    pub op: IO,
    /// Highest view number known to the client,
    /// used for identifying old messages and
    /// starting view changes.
    pub recent: ViewNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeConsensus<CO> {
    pub op_id: OpId,
    pub op: CO,
    /// Highest view number known to the client,
    /// used for identifying old messages and
    /// starting view changes.
    pub recent: ViewNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyInconsistent<A> {
    pub op_id: OpId,
    pub view: View<A>,
    /// If `None`, the request couldn't be processed because
    /// `recent` wasn't recent.
    pub state: Option<RecordEntryState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyConsensus<CR, A> {
    pub op_id: OpId,
    pub view: View<A>,
    /// If `None`, the request couldn't be processed because
    /// `recent` wasn't recent.
    pub result_state: Option<(CR, RecordEntryState)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeInconsistent {
    pub op_id: OpId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeConsensus<CR> {
    pub op_id: OpId,
    pub result: CR,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Confirm<A> {
    pub op_id: OpId,
    pub view: View<A>,
}

/// Informs a replica about a new view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoViewChange<IO, CO, CR, A> {
    /// View number to change to.
    pub view_number: ViewNumber,
    /// Is `Some` when sent from replica to new leader.
    pub addendum: Option<ViewChangeAddendum<IO, CO, CR, A>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ViewChangeAddendum<IO, CO, CR, A> {
    /// Sender replica's index.
    pub replica_index: ReplicaIndex,
    /// Sender replica's record.
    pub record: RecordImpl<IO, CO, CR>,
    /// Latest view in which sender replica had a normal state.
    pub latest_normal_view: View<A>,
}

impl<IO, CO, CR, A: Debug> Debug for ViewChangeAddendum<IO, CO, CR, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Addendum")
            .field("replica_index", &self.replica_index)
            .field("latest_normal_view", &self.latest_normal_view)
            .finish_non_exhaustive()
    }
}

/// From leader to inform a replica that a new view has begun.
#[derive(Clone, Serialize, Deserialize)]
pub struct StartView<IO, CO, CR, A> {
    /// Leader's merged record.
    pub record: RecordImpl<IO, CO, CR>,
    /// New view.
    pub view: View<A>,
}

impl<IO, CO, CR, A: Debug> Debug for StartView<IO, CO, CR, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartView")
            .field("view", &self.view)
            .finish_non_exhaustive()
    }
}
