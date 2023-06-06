use super::OpId;

#[derive(Debug, Clone)]
pub(crate) enum Message<O, R> {
    Propose(Propose<O>),
    Reply(Reply<R>),
    Finalize(Finalize<R>),
    Confirm(Confirm),
}

#[derive(Debug, Clone)]
pub(crate) struct Propose<O> {
    pub(crate) op_id: OpId,
    pub(crate) op: O,
}

#[derive(Debug, Clone)]
pub(crate) struct Reply<R> {
    pub(crate) op_id: OpId,
    pub(crate) result: R,
}

#[derive(Debug, Clone)]
pub(crate) struct Finalize<R> {
    pub(crate) op_id: OpId,
    pub(crate) result: R,
}

#[derive(Debug, Clone)]
pub(crate) struct Confirm {
    pub(crate) op_id: OpId,
}
