use super::{OpId, RecordEntryState, ViewNumber};

#[derive(Debug, Clone, derive_more::From, derive_more::TryInto)]
pub(crate) enum Message<O, R> {
    ProposeInconsistent(ProposeInconsistent<O>),
    ProposeConsensus(ProposeConsensus<O>),
    ReplyInconsistent(ReplyInconsistent),
    ReplyConsensus(ReplyConsensus<R>),
    FinalizeInconsistent(FinalizeInconsistent),
    FinalizeConsensus(FinalizeConsensus<R>),
    Confirm(Confirm),
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
