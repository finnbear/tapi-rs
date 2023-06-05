use super::OpId;

pub(crate) enum Message<O, R> {
    Propose(Propose<O>),
    Reply(Reply<R>),
    Finalize(Finalize<R>),
    Confirm(Confirm),
}

pub(crate) struct Propose<O> {
    op_id: OpId,
    op: O,
}

pub(crate) struct Reply<R> {
    op_id: OpId,
    result: R,
}

pub(crate) struct Finalize<R> {
    op_id: OpId,
    result: R,
}

pub(crate) struct Confirm {
    op_id: OpId,
}
