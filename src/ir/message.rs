use super::{OpId, Record, RecordEntryState, ReplicaIndex, ViewNumber};

#[derive(Debug, Clone, derive_more::From, derive_more::TryInto)]
pub(crate) enum Message<O, R> {
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

#[derive(Debug, Clone)]
pub(crate) struct RequestUnlogged<O> {
    pub(crate) op: O,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplyUnlogged<R> {
    pub(crate) result: R,
}

#[derive(Debug, Clone)]
pub(crate) struct ProposeInconsistent<O> {
    pub(crate) op_id: OpId,
    pub(crate) op: O,
}

#[derive(Debug, Clone)]
pub(crate) struct ProposeConsensus<O> {
    pub(crate) op_id: OpId,
    pub(crate) op: O,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplyInconsistent {
    pub(crate) op_id: OpId,
    pub(crate) view_number: ViewNumber,
    pub(crate) state: RecordEntryState,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplyConsensus<R> {
    pub(crate) op_id: OpId,
    pub(crate) view_number: ViewNumber,
    pub(crate) result: R,
    pub(crate) state: RecordEntryState,
}

#[derive(Debug, Clone)]
pub(crate) struct FinalizeInconsistent {
    pub(crate) op_id: OpId,
}

#[derive(Debug, Clone)]
pub(crate) struct FinalizeConsensus<R> {
    pub(crate) op_id: OpId,
    pub(crate) result: R,
}

#[derive(Debug, Clone)]
pub(crate) struct Confirm {
    pub(crate) op_id: OpId,
    pub(crate) view_number: ViewNumber,
}

#[derive(Debug, Clone)]
pub(crate) struct DoViewChange<O, R> {
    pub(crate) view_number: ViewNumber,
    pub(crate) addendum: Option<ViewChangeAddendum<O, R>>,
}

#[derive(Debug, Clone)]
pub(crate) struct ViewChangeAddendum<O, R> {
    pub(crate) replica_index: ReplicaIndex,
    pub(crate) record: Record<O, R>,
    pub(crate) latest_normal_view: ViewNumber,
}

#[derive(Debug, Clone)]
pub(crate) struct StartView<O, R> {
    pub(crate) record: Record<O, R>,
    pub(crate) view_number: ViewNumber,
}
