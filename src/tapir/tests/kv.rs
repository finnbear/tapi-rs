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
    kv(false, 3).await;

    return;

    for _ in 0..5 {
        for linearizable in [false, true] {
            for replicas in (3..=9).step_by(2) {
                kv(linearizable, replicas).await;
            }
        }
    }
}

async fn kv(linearizable: bool, num_replicas: usize) {
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
        linearizable: bool,
    ) -> Arc<IrReplica<TapirReplica<K, V>, ChannelTransport<Message>>> {
        Arc::new_cyclic(
            |weak: &std::sync::Weak<IrReplica<TapirReplica<K, V>, ChannelTransport<Message>>>| {
                let weak = weak.clone();
                let channel =
                    registry.channel(move |from, message| weak.upgrade()?.receive(from, message));
                let upcalls = TapirReplica::new(linearizable);
                IrReplica::new(index, membership.clone(), upcalls, channel)
            },
        )
    }

    let replicas = (0..num_replicas)
        .map(|i| create_replica(IrReplicaIndex(i), &registry, &membership, linearizable))
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
    let first = txn.commit().await.unwrap();

    Transport::sleep(Duration::from_millis(10)).await;

    if linearizable {
        let txn = clients[1].begin();
        let result = txn.get(vec![1]).await;
        if result.is_none() {
            // We read stale data so shouldn't be able to commit.
            assert_eq!(txn.commit().await, None, "prev = {first:?}");
        } else {
            // Up to date, should be able to commit.
            assert!(txn.commit().await.is_some());
        }
    } else {
        let txn = clients[1].begin();
        let result = txn.get(vec![1]).await;
        if let Some(commit) = txn.commit().await {
            if result.is_none() {
                assert!(commit < first);
            } else {
                assert_eq!(result, Some(vec![2]));
                assert!(commit > first);
            }
        }
    }
}
