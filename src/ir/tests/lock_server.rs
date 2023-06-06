use crate::{
    ChannelRegistry, ChannelTransport, IrClient, IrMessage, IrReplica, IrReplicaIndex, Transport,
};
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

    const REPLICAS: usize = 3;

    let membership = (0..REPLICAS)
        .map(|i| (IrReplicaIndex(i), i))
        .collect::<HashMap<_, _>>();

    fn create_replica(
        index: IrReplicaIndex,
        registry: &ChannelRegistry<Message>,
        membership: &HashMap<IrReplicaIndex, <ChannelTransport<Message> as Transport>::Address>,
    ) -> Arc<Mutex<IrReplica<ChannelTransport<Message>, Op, Res>>> {
        Arc::new_cyclic(
            |weak: &std::sync::Weak<Mutex<IrReplica<ChannelTransport<Message>, Op, Res>>>| {
                let weak = weak.clone();
                let channel = registry.channel(move |from, message| {
                    weak.upgrade()?.lock().unwrap().receive(from, message)
                });
                Mutex::new(IrReplica::new(index, membership.clone(), channel))
            },
        )
    }

    let replicas = (0..REPLICAS)
        .map(|i| create_replica(IrReplicaIndex(i), &registry, &membership))
        .collect::<Vec<_>>();

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
                Mutex::new(IrClient::new(membership.clone(), channel))
            },
        )
    }

    let client = create_client(&registry, &membership);

    client.lock().unwrap().invoke_inconsistent(Op::Lock).await;
}
