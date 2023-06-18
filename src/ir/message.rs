use super::{
    record::RecordImpl, OpId, Record, RecordEntryState, ReplicaIndex, ReplicaUpcalls, ViewNumber,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub type Message<U> = MessageImpl<
    <U as ReplicaUpcalls>::UO,
    <U as ReplicaUpcalls>::UR,
    <U as ReplicaUpcalls>::IO,
    <U as ReplicaUpcalls>::CO,
    <U as ReplicaUpcalls>::CR,
>;

#[derive(Clone, derive_more::From, derive_more::TryInto, Serialize, Deserialize)]
pub enum MessageImpl<UO, UR, IO, CO, CR> {
    RequestUnlogged(RequestUnlogged<UO>),
    ReplyUnlogged(ReplyUnlogged<UR>),
    ProposeInconsistent(ProposeInconsistent<IO>),
    ProposeConsensus(ProposeConsensus<CO>),
    ReplyInconsistent(ReplyInconsistent),
    ReplyConsensus(ReplyConsensus<CR>),
    FinalizeInconsistent(FinalizeInconsistent),
    FinalizeConsensus(FinalizeConsensus<CR>),
    Confirm(Confirm),
    DoViewChange(DoViewChange<IO, CO, CR>),
    StartView(StartView<IO, CO, CR>),
}

impl<UO: Debug, UR: Debug, IO: Debug, CO: Debug, CR: Debug> Debug
    for MessageImpl<UO, UR, IO, CO, CR>
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
pub struct ReplyUnlogged<UR> {
    pub result: UR,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeInconsistent<IO> {
    pub op_id: OpId,
    pub op: IO,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeConsensus<CO> {
    pub op_id: OpId,
    pub op: CO,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyInconsistent {
    pub op_id: OpId,
    pub view_number: ViewNumber,
    pub state: RecordEntryState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyConsensus<CR> {
    pub op_id: OpId,
    pub view_number: ViewNumber,
    pub result: CR,
    pub state: RecordEntryState,
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
pub struct Confirm {
    pub op_id: OpId,
    pub view_number: ViewNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoViewChange<IO, CO, CR> {
    pub view_number: ViewNumber,
    pub addendum: Option<ViewChangeAddendum<IO, CO, CR>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ViewChangeAddendum<IO, CO, CR> {
    pub replica_index: ReplicaIndex,
    pub record: RecordImpl<IO, CO, CR>,
    pub latest_normal_view: ViewNumber,
}

impl<IO, CO, CR> Debug for ViewChangeAddendum<IO, CO, CR> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Addendum")
            .field("replica_index", &self.replica_index)
            .field("latest_normal_view", &self.latest_normal_view)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct StartView<IO, CO, CR> {
    pub record: RecordImpl<IO, CO, CR>,
    pub view_number: ViewNumber,
}

impl<IO, CO, CR> Debug for StartView<IO, CO, CR> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartView")
            .field("view_number", &self.view_number)
            .finish_non_exhaustive()
    }
}
