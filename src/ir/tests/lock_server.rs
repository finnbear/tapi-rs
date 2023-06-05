use crate::{ChannelRegistry, ChannelTransport, IrClient};

#[tokio::test]
async fn lock_server() {
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

    let registry = ChannelRegistry::default();

    let client_channel = registry.channel();
    let mut client = IrClient::<ChannelTransport<_>, Op, Res>::new(client_channel);

    client.invoke_consensus(Op::Lock, |results| Res::Ok).await;
}
