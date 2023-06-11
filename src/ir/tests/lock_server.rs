use crate::{
    ChannelRegistry, ChannelTransport, IrClient, IrClientId, IrMembership, IrMessage, IrOpId,
    IrRecord, IrRecordEntry, IrReplica, IrReplicaIndex, IrReplicaUpcalls, Transport,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[tokio::test]
async fn lock_server() {
    #[derive(Debug, Clone)]
    enum Op {
        Lock(IrClientId),
        Unlock(IrClientId),
    }

    #[derive(Debug, Clone, PartialEq)]
    enum Res {
        Ok,
        No,
    }

    type Message = IrMessage<Op, Res>;

    struct Upcalls {
        locked: Option<IrClientId>,
    }

    impl IrReplicaUpcalls for Upcalls {
        type Op = Op;
        type Result = Res;

        fn exec_unlogged(&mut self, op: Self::Op) -> Self::Result {
            unreachable!();
        }
        fn exec_inconsistent(&mut self, op: &Self::Op) {
            match op {
                &Op::Unlock(client_id) if Some(client_id) == self.locked => {
                    self.locked = None;
                }
                _ => panic!(),
            }
        }
        fn exec_consensus(&mut self, op: &Self::Op) -> Self::Result {
            match op {
                &Op::Lock(client_id) => {
                    if self.locked.is_none() || self.locked == Some(client_id) {
                        self.locked = Some(client_id);
                        Res::Ok
                    } else {
                        Res::No
                    }
                }
                _ => panic!(),
            }
        }
        fn sync(&mut self, record: &IrRecord<Self::Op, Self::Result>) {}
        fn merge(
            &mut self,
            d: HashMap<IrOpId, Vec<IrRecordEntry<Self::Op, Self::Result>>>,
            u: HashMap<IrOpId, Vec<IrRecordEntry<Self::Op, Self::Result>>>,
            majority_results_in_d: HashMap<IrOpId, Self::Result>,
        ) -> HashMap<IrOpId, Self::Result> {
            Default::default()
        }
    }

    let registry = ChannelRegistry::default();

    const REPLICAS: usize = 5;

    let membership = IrMembership::new((0..REPLICAS).collect::<Vec<_>>());

    fn create_replica(
        index: IrReplicaIndex,
        registry: &ChannelRegistry<Message>,
        membership: &IrMembership<ChannelTransport<Message>>,
    ) -> Arc<IrReplica<Upcalls, ChannelTransport<Message>>> {
        Arc::new_cyclic(
            |weak: &std::sync::Weak<IrReplica<Upcalls, ChannelTransport<Message>>>| {
                let weak = weak.clone();
                let channel =
                    registry.channel(move |from, message| weak.upgrade()?.receive(from, message));
                let upcalls = Upcalls { locked: None };
                IrReplica::new(index, membership.clone(), upcalls, channel)
            },
        )
    }

    let replicas = (0..REPLICAS)
        .map(|i| create_replica(IrReplicaIndex(i), &registry, &membership))
        .collect::<Vec<_>>();

    fn create_client(
        registry: &ChannelRegistry<Message>,
        membership: &IrMembership<ChannelTransport<Message>>,
    ) -> Arc<IrClient<ChannelTransport<Message>, Op, Res>> {
        Arc::new_cyclic(
            |weak: &std::sync::Weak<IrClient<ChannelTransport<Message>, Op, Res>>| {
                let weak = weak.clone();
                let channel =
                    registry.channel(move |from, message| weak.upgrade()?.receive(from, message));
                IrClient::new(membership.clone(), channel)
            },
        )
    }

    let clients = (0..2)
        .map(|_| create_client(&registry, &membership))
        .collect::<Vec<_>>();

    let decide_lock = |results: Vec<Res>| {
        let ok = results.iter().filter(|&r| r == &Res::Ok).count();
        //println!("deciding {ok} of {} : {results:?}", results.len());
        if ok >= (REPLICAS - 1) / 2 + 1 {
            Res::Ok
        } else {
            Res::No
        }
    };

    for _ in 0..2 {
        assert_eq!(
            clients[0]
                .invoke_consensus(Op::Lock(clients[0].id()), &decide_lock)
                .await,
            Res::Ok
        );
        assert_eq!(
            clients[1]
                .invoke_consensus(Op::Lock(clients[1].id()), &decide_lock)
                .await,
            Res::No
        );
    }

    clients[0]
        .invoke_inconsistent(Op::Unlock(clients[0].id()))
        .await;

    tokio::time::sleep(Duration::from_secs(6)).await;

    assert_eq!(
        clients[1]
            .invoke_consensus(Op::Lock(clients[1].id()), &decide_lock)
            .await,
        Res::Ok
    );
}
