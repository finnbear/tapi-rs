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
    op_id: OpId,
    op: O,
}

#[derive(Debug, Clone)]
pub(crate) struct Reply<R> {
    op_id: OpId,
    result: R,
}

#[derive(Debug, Clone)]
pub(crate) struct Finalize<R> {
    op_id: OpId,
    result: R,
}

#[derive(Debug, Clone)]
pub(crate) struct Confirm {
    op_id: OpId,
}
