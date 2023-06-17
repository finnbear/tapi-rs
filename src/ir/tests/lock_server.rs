use crate::{
    ChannelRegistry, ChannelTransport, IrClient, IrClientId, IrMembership, IrMembershipSize,
    IrMessage, IrOpId, IrRecord, IrRecordConsensusEntry, IrReplica, IrReplicaIndex,
    IrReplicaUpcalls, Transport,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

#[tokio::test]
async fn test_lock_server() {
    lock_server(3).await;

    /*
    for _ in 0..10 {
        for r in (3..=5).step_by(2) {
            lock_server(r).await;
        }
    }
    */
}

async fn lock_server(num_replicas: usize) {
    #[derive(Debug, Clone)]
    enum Op {
        Lock(IrClientId),
        Unlock(IrClientId),
    }

    #[derive(Debug, Clone, Eq, PartialEq, Hash)]
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
                &Op::Unlock(client_id) => {
                    if Some(client_id) == self.locked {
                        self.locked = None;
                    }
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
        fn sync(
            &mut self,
            _: &IrRecord<Self::Op, Self::Result>,
            record: &IrRecord<Self::Op, Self::Result>,
        ) {
            self.locked = None;

            let mut locked = HashSet::<IrClientId>::new();
            let mut unlocked = HashSet::<IrClientId>::new();
            for (op_id, entry) in &record.inconsistent {
                match entry.op {
                    Op::Unlock(client_id) => {
                        unlocked.insert(client_id);
                    }
                    _ => unreachable!(),
                }
            }
            for (op_id, entry) in &record.consensus {
                match entry.op {
                    Op::Lock(client_id) => {
                        if matches!(entry.result, Res::Ok) {
                            locked.insert(client_id);
                        }
                    }
                    _ => unreachable!(),
                }
            }

            for client_id in locked {
                if !unlocked.contains(&client_id) {
                    if self.locked.is_some() {
                        panic!();
                    }
                    self.locked = Some(client_id);
                }
            }
        }
        fn merge(
            &mut self,
            d: HashMap<IrOpId, (Self::Op, Self::Result)>,
            u: Vec<(IrOpId, Self::Op, Self::Result)>,
        ) -> HashMap<IrOpId, Self::Result> {
            let mut results = HashMap::<IrOpId, Self::Result>::new();

            for (op_id, (request, reply)) in &d {
                let Op::Lock(client_id) = request else {
                    panic!();
                };
                let successful = matches!(reply, Res::Ok);

                results.insert(
                    *op_id,
                    if successful && self.locked.is_none() {
                        self.locked = Some(*client_id);
                        Res::Ok
                    } else {
                        Res::No
                    },
                );
            }

            for (op_id, _, _) in &u {
                results.insert(*op_id, Res::No);
            }

            results
        }
    }

    let registry = ChannelRegistry::default();
    let membership = IrMembership::new((0..num_replicas).collect::<Vec<_>>());

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

    let replicas = (0..num_replicas)
        .map(|i| create_replica(IrReplicaIndex(i), &registry, &membership))
        .collect::<Vec<_>>();

    fn create_client(
        registry: &ChannelRegistry<Message>,
        membership: &IrMembership<ChannelTransport<Message>>,
    ) -> Arc<IrClient<ChannelTransport<Message>, Op, Res>> {
        Arc::new_cyclic(
            |weak: &std::sync::Weak<IrClient<ChannelTransport<Message>, Op, Res>>| {
                let weak = weak.clone();
                let channel = registry.channel(move |_, _| unreachable!());
                IrClient::new(membership.clone(), channel)
            },
        )
    }

    let clients = (0..2)
        .map(|_| create_client(&registry, &membership))
        .collect::<Vec<_>>();

    let decide_lock = |results: HashMap<Res, usize>, membership: IrMembershipSize| {
        //println!("deciding {ok} of {} : {results:?}", results.len());
        if results.get(&Res::Ok).copied().unwrap_or_default() >= membership.f_plus_one() {
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

    for i in 0..5 {
        ChannelTransport::<Message>::sleep(Duration::from_secs(8)).await;

        eprintln!("@@@@@ INVOKE {replicas:?}");
        if clients[1]
            .invoke_consensus(Op::Lock(clients[1].id()), &decide_lock)
            .await
            == Res::Ok
        {
            return;
        }
    }

    panic!();
}
