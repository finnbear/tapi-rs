use serde::{Deserialize, Serialize};

use super::{OpId, Record, RecordEntryState, ReplicaIndex, ViewNumber};

#[derive(Debug, Clone, derive_more::From, derive_more::TryInto, Serialize, Deserialize)]
pub enum Message<O, R> {
    RequestUnlogged(RequestUnlogged<O>),
    ReplyUnlogged(ReplyUnlogged<R>),
    ProposeInconsistent(ProposeInconsistent<O>),
    ProposeConsensus(ProposeConsensus<O>),
    ReplyInconsistent(ReplyInconsistent),
    ReplyConsensus(ReplyConsensus<R>),
    FinalizeInconsistent(FinalizeInconsistent),
    FinalizeConsensus(FinalizeConsensus<R>),
    Confirm(Confirm),
    DoViewChange(DoViewChange<O, R>),
    StartView(StartView<O, R>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestUnlogged<O> {
    pub op: O,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyUnlogged<R> {
    pub result: R,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeInconsistent<O> {
    pub op_id: OpId,
    pub op: O,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeConsensus<O> {
    pub op_id: OpId,
    pub op: O,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyInconsistent {
    pub op_id: OpId,
    pub view_number: ViewNumber,
    pub state: RecordEntryState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyConsensus<R> {
    pub op_id: OpId,
    pub view_number: ViewNumber,
    pub result: R,
    pub state: RecordEntryState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeInconsistent {
    pub op_id: OpId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeConsensus<R> {
    pub op_id: OpId,
    pub result: R,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Confirm {
    pub op_id: OpId,
    pub view_number: ViewNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoViewChange<O, R> {
    pub view_number: ViewNumber,
    pub addendum: Option<ViewChangeAddendum<O, R>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewChangeAddendum<O, R> {
    pub replica_index: ReplicaIndex,
    pub record: Record<O, R>,
    pub latest_normal_view: ViewNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartView<O, R> {
    pub record: Record<O, R>,
    pub view_number: ViewNumber,
}
