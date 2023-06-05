pub(crate) enum State {
    Tentative,
    Finalized,
}

pub(crate) struct Record<O, R> {
    operation: O,
    result: Option<R>,
    state: State,
}
