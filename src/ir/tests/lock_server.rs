use crate::{
    ChannelRegistry, ChannelTransport, IrClient, IrMembership, IrMessage, IrOpId, IrRecord,
    IrReplica, IrReplicaIndex, IrReplicaUpcalls, Transport,
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

    struct Upcalls {
        locked: bool,
    }

    impl IrReplicaUpcalls for Upcalls {
        type Op = Op;
        type Result = Res;

        fn exec_inconsistent(&mut self, op: &Self::Op) {
            match op {
                Op::Unlock => {
                    self.locked = false;
                }
                _ => panic!(),
            }
        }
        fn exec_consensus(&mut self, op: &Self::Op) {
            match op {
                Op::Lock => {
                    self.locked = true;
                }
                _ => panic!(),
            }
        }
        fn sync(&mut self, record: IrRecord<Self::Op, Self::Result>) {}
        fn merge(
            &mut self,
            d: HashMap<IrOpId, Self::Op>,
            u: HashMap<IrOpId, Self::Op>,
        ) -> IrRecord<Self::Op, Self::Result> {
            Default::default()
        }
    }

    let registry = ChannelRegistry::default();

    const REPLICAS: usize = 3;

    let membership = IrMembership::new((0..REPLICAS).collect::<Vec<_>>());

    fn create_replica(
        index: IrReplicaIndex,
        registry: &ChannelRegistry<Message>,
        membership: &IrMembership<ChannelTransport<Message>>,
    ) -> Arc<Mutex<IrReplica<Upcalls, ChannelTransport<Message>>>> {
        Arc::new_cyclic(
            |weak: &std::sync::Weak<Mutex<IrReplica<Upcalls, ChannelTransport<Message>>>>| {
                let weak = weak.clone();
                let channel = registry.channel(move |from, message| {
                    weak.upgrade()?.lock().unwrap().receive(from, message)
                });
                let upcalls = Upcalls { locked: false };
                Mutex::new(IrReplica::new(index, membership.clone(), upcalls, channel))
            },
        )
    }

    let replicas = (0..REPLICAS)
        .map(|i| create_replica(IrReplicaIndex(i), &registry, &membership))
        .collect::<Vec<_>>();

    fn create_client(
        registry: &ChannelRegistry<Message>,
        membership: &IrMembership<ChannelTransport<Message>>,
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

    client
        .lock()
        .unwrap()
        .invoke_consensus(Op::Lock, |results| Res::No)
        .await;
    client.lock().unwrap().invoke_inconsistent(Op::Unlock).await;
}
