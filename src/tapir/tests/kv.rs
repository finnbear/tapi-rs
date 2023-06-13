use crate::{
    ChannelRegistry, ChannelTransport, IrClient, IrClientId, IrMembership, IrMembershipSize,
    IrMessage, IrOpId, IrRecord, IrRecordEntry, IrReplica, IrReplicaIndex, IrReplicaUpcalls,
    TapirClient, TapirReplica, TapirReply, TapirRequest, Transport,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

#[tokio::test]
async fn test_kv() {
    kv(3).await;
}

async fn kv(num_replicas: usize) {
    type K = Vec<u8>;
    type V = Vec<u8>;
    type Op = TapirRequest<K, V>;
    type Res = TapirReply<V>;
    type Message = IrMessage<Op, Res>;
    type Transport = ChannelTransport<Message>;
    let registry = ChannelRegistry::default();
    let membership = IrMembership::new((0..num_replicas).collect::<Vec<_>>());

    fn create_replica(
        index: IrReplicaIndex,
        registry: &ChannelRegistry<Message>,
        membership: &IrMembership<ChannelTransport<Message>>,
    ) -> Arc<IrReplica<TapirReplica<K, V>, ChannelTransport<Message>>> {
        Arc::new_cyclic(
            |weak: &std::sync::Weak<IrReplica<TapirReplica<K, V>, ChannelTransport<Message>>>| {
                let weak = weak.clone();
                let channel =
                    registry.channel(move |from, message| weak.upgrade()?.receive(from, message));
                let upcalls = TapirReplica::new(true);
                IrReplica::new(index, membership.clone(), upcalls, channel)
            },
        )
    }

    let replicas = (0..num_replicas)
        .map(|i| create_replica(IrReplicaIndex(i), &registry, &membership))
        .collect::<Vec<_>>();

    fn create_client(
        registry: &ChannelRegistry<Message>,
        membership: &IrMembership<ChannelTransport<Message>>,
    ) -> Arc<TapirClient<K, V, ChannelTransport<Message>>> {
        Arc::new_cyclic(
            |weak: &std::sync::Weak<TapirClient<K, V, ChannelTransport<Message>>>| {
                let weak = weak.clone();
                let channel = registry.channel(move |from, message| unreachable!());
                TapirClient::new(membership.clone(), channel)
            },
        )
    }

    let clients = (0..2)
        .map(|_| create_client(&registry, &membership))
        .collect::<Vec<_>>();

    let txn = clients[0].begin();
    assert_eq!(txn.get(vec![0]).await, None);
    txn.put(vec![1], Some(vec![2]));
    assert!(txn.commit().await);

    Transport::sleep(Duration::from_secs(1)).await;

    let txn = clients[1].begin();
    assert_eq!(txn.get(vec![1]).await, Some(vec![2]));
    assert!(txn.commit().await);
}
