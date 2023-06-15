use futures::future::join_all;

use crate::{
    ChannelRegistry, ChannelTransport, IrClient, IrClientId, IrMembership, IrMembershipSize,
    IrMessage, IrOpId, IrRecord, IrRecordEntry, IrReplica, IrReplicaIndex, IrReplicaUpcalls,
    TapirClient, TapirReplica, TapirReply, TapirRequest, Transport as _,
};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    sync::Arc,
    time::Duration,
};

#[tokio::test]
async fn test_kv() {
    for _ in 0..500 {
        for replicas in (3..=7/* 11 */).step_by(2) {
            increment_parallel(replicas).await;
            increment_sequential(replicas).await;
            for linearizable in [false, true] {
                rwr(linearizable, replicas).await;
            }
        }
    }
}

type K = i64;
type V = i64;
type Op = TapirRequest<K, V>;
type Res = TapirReply<V>;
type Message = IrMessage<Op, Res>;
type Transport = ChannelTransport<Message>;

fn build_kv(
    linearizable: bool,
    num_replicas: usize,
) -> (
    Vec<Arc<IrReplica<TapirReplica<K, V>, ChannelTransport<Message>>>>,
    Vec<Arc<TapirClient<K, V, ChannelTransport<Message>>>>,
) {
    println!("---------------------------");
    println!(" linearizable={linearizable} num_replicas={num_replicas}");
    println!("---------------------------");

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

    (replicas, clients)
}

async fn rwr(linearizable: bool, num_replicas: usize) {
    let (replicas, clients) = build_kv(linearizable, num_replicas);

    let txn = clients[0].begin();
    assert_eq!(txn.get(0).await, None);
    txn.put(1, Some(2));
    let first = txn.commit().await.unwrap();

    Transport::sleep(Duration::from_millis(10)).await;

    if linearizable {
        let txn = clients[1].begin();
        let result = txn.get(1).await;
        if result.is_none() {
            // We read stale data so shouldn't be able to commit.
            assert_eq!(txn.commit().await, None, "prev = {first:?}");
        } else {
            // Up to date, should be able to commit.
            //assert!(txn.commit().await.is_some());
        }
    } else {
        let txn = clients[1].begin();
        let result = txn.get(1).await;
        if let Some(commit) = txn.commit().await {
            if result.is_none() {
                assert!(commit < first, "{commit:?} {first:?}");
            } else {
                assert_eq!(result, Some(2));
                assert!(commit > first);
            }
        }
    }
}

#[tokio::test]
async fn increment_sequential_3() {
    increment_sequential(3).await;
}

#[tokio::test]
async fn increment_sequential_7() {
    for _ in 0..1000 {
        increment_sequential(7).await;
    }
}

async fn increment_sequential(num_replicas: usize) {
    let (replicas, clients) = build_kv(true, num_replicas);

    let mut committed = 0;
    for _ in 0..10 {
        println!("^^^^^^^^^^^^^^^^^^^ BEGINNING TXN");
        let txn = clients[0].begin();
        let old = txn.get(0).await.unwrap_or_default();
        txn.put(0, Some(old + 1));
        if txn.commit().await.is_some() {
            assert_eq!(committed, old);
            committed += 1;
        }

        Transport::sleep(Duration::from_millis(100)).await;
    }

    println!("committed = {committed}");
    assert!(committed > 0);
}

#[tokio::test]
async fn increment_parallel_3() {
    increment_parallel(3).await;
}

#[tokio::test]
async fn increment_parallel_7() {
    for _ in 0..1000 {
        increment_parallel(7).await;
    }
}

async fn increment_parallel(num_replicas: usize) {
    let (replicas, clients) = build_kv(true, num_replicas);

    let add = || async {
        let txn = clients[0].begin();
        let old = txn.get(0).await.unwrap_or_default();
        txn.put(0, Some(old + 1));
        txn.commit().await.is_some()
    };

    let committed = join_all((0..5).map(|_| add()))
        .await
        .into_iter()
        .filter(|ok| *ok)
        .count() as i64;

    Transport::sleep(Duration::from_secs(1)).await;

    let txn = clients[1].begin();
    let result = txn.get(0).await.unwrap_or_default();
    eprintln!("INCREMENT TEST result={result} committed={committed}");
    println!("{} {}", txn.commit().await.is_some(), result == committed);
}
