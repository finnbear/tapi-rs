use crate::{ChannelTransport, IrClient};

#[test]
fn lock_server() {
    #[derive(Debug, Clone)]
    enum Op {
        Lock,
        Unlock,
    }

    #[derive(Debug, Clone)]
    enum Res {
        Ok,
        No,
    }

    let client = IrClient::<ChannelTransport, Op, Res>::new();
}
