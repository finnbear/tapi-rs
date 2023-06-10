use crate::{
    ChannelRegistry, ChannelTransport, IrClient, IrMembership, IrMessage, IrOpId, IrRecord,
    IrRecordEntry, IrReplica, IrReplicaIndex, IrReplicaUpcalls, Transport,
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
        Lock,
        Unlock,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum Res {
        Ok,
        No,
    }

    type Message = IrMessage<Op, Res>;

    struct Upcalls {
        locked: bool,
    }

    impl IrReplicaUpcalls for Upcalls {
        type Op = Op;
        type Result = Res;

        fn exec_unlogged(&mut self, op: Self::Op) -> Self::Result {
            unreachable!();
        }
        fn exec_inconsistent(&mut self, op: &Self::Op) {
            match op {
                Op::Unlock => {
                    self.locked = false;
                }
                _ => panic!(),
            }
        }
        fn exec_consensus(&mut self, op: &Self::Op) -> Self::Result {
            match op {
                Op::Lock => {
                    if self.locked {
                        Res::No
                    } else {
                        self.locked = true;
                        Res::Ok
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
                let upcalls = Upcalls { locked: false };
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

    let client = create_client(&registry, &membership);

    let decide_lock = |results: Vec<Res>| {
        let ok = results.iter().filter(|&r| r == &Res::Ok).count();
        //println!("deciding {ok} of {} : {results:?}", results.len());
        if ok >= (REPLICAS - 1) / 2 + 1 {
            Res::Ok
        } else {
            Res::No
        }
    };

    assert_eq!(
        client.invoke_consensus(Op::Lock, &decide_lock).await,
        Res::Ok
    );
    assert_eq!(
        client.invoke_consensus(Op::Lock, &decide_lock).await,
        Res::No
    );
    client.invoke_inconsistent(Op::Unlock).await;
    assert_eq!(
        client.invoke_consensus(Op::Lock, &decide_lock).await,
        Res::Ok
    );

    tokio::time::sleep(Duration::from_secs(10)).await;
}
