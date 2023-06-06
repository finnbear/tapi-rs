use crate::{ChannelRegistry, ChannelTransport, IrClient, IrMessage, IrReplicaIndex, Transport};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

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

    type Message = IrMessage<Op, Res>;

    let registry = ChannelRegistry::default();

    fn create_client(
        registry: &ChannelRegistry<Message>,
        membership: &HashMap<IrReplicaIndex, <ChannelTransport<Message> as Transport>::Address>,
    ) -> Arc<Mutex<IrClient<ChannelTransport<Message>, Op, Res>>> {
        Arc::new_cyclic(
            |weak: &std::sync::Weak<Mutex<IrClient<ChannelTransport<Message>, Op, Res>>>| {
                let weak = weak.clone();
                let channel = registry.channel(move |from, message| {
                    weak.upgrade()?.lock().unwrap().receive(from, message)
                });
                Mutex::new(IrClient::new(channel, membership.clone()))
            },
        )
    }

    let client = create_client(&registry, &HashMap::new());

    client
        .lock()
        .unwrap()
        .invoke_consensus(Op::Lock, |results| Res::Ok)
        .await;
}
