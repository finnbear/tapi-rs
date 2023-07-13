use crate::{
    ChannelRegistry, ChannelTransport, IrClient, IrClientId, IrMembership, IrMembershipSize,
    IrOpId, IrRecord, IrReplica, IrReplicaUpcalls, Transport,
};
use rand::{seq::IteratorRandom, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

#[tokio::test]
async fn lock_server_1() {
    timeout_lock_server(1).await;
}

#[tokio::test]
async fn lock_server_2() {
    timeout_lock_server(2).await;
}

#[tokio::test]
async fn lock_server_3() {
    timeout_lock_server(3).await;
}

#[tokio::test]
async fn lock_server_4() {
    timeout_lock_server(4).await;
}

#[tokio::test]
async fn lock_server_5() {
    timeout_lock_server(5).await;
}

#[tokio::test]
async fn lock_server_7() {
    timeout_lock_server(7).await;
}

#[tokio::test]
async fn lock_server_9() {
    timeout_lock_server(9).await;
}

#[ignore]
#[tokio::test]
async fn lock_server_loop() {
    loop {
        timeout_lock_server(3).await;
    }
}

async fn timeout_lock_server(num_replicas: usize) {
    tokio::time::timeout(
        Duration::from_secs((num_replicas as u64 + 10) * 10),
        lock_server(num_replicas),
    )
    .await
    .unwrap();
}

async fn lock_server(num_replicas: usize) {
    println!("testing lock server with {num_replicas} replicas");

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct Lock(IrClientId);

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct Unlock(IrClientId);

    #[derive(Debug, Clone, Eq, PartialEq, Hash)]
    enum LockResult {
        Ok,
        No,
    }

    #[derive(Serialize, Deserialize)]
    struct Upcalls {
        locked: Option<IrClientId>,
    }

    impl IrReplicaUpcalls for Upcalls {
        type UO = ();
        type UR = ();
        type IO = Unlock;
        type CO = Lock;
        type CR = LockResult;

        fn exec_unlogged(&mut self, op: Self::UO) -> Self::UR {
            let _ = op;
            unreachable!();
        }

        fn exec_inconsistent(&mut self, op: &Self::IO) {
            if Some(op.0) == self.locked {
                self.locked = None;
            }
        }

        fn exec_consensus(&mut self, op: &Self::CO) -> Self::CR {
            if self.locked.is_none() || self.locked == Some(op.0) {
                self.locked = Some(op.0);
                LockResult::Ok
            } else {
                LockResult::No
            }
        }

        fn sync(&mut self, _: &IrRecord<Self>, record: &IrRecord<Self>) {
            self.locked = None;

            let mut locked = HashSet::<IrClientId>::new();
            let mut unlocked = HashSet::<IrClientId>::new();
            for entry in record.inconsistent.values() {
                unlocked.insert(entry.op.0);
            }
            for entry in record.consensus.values() {
                if matches!(entry.result, LockResult::Ok) {
                    locked.insert(entry.op.0);
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
            d: HashMap<IrOpId, (Self::CO, Self::CR)>,
            u: Vec<(IrOpId, Self::CO, Self::CR)>,
        ) -> HashMap<IrOpId, Self::CR> {
            let mut results = HashMap::<IrOpId, Self::CR>::new();

            for (op_id, (request, reply)) in &d {
                let successful = matches!(reply, LockResult::Ok);

                results.insert(
                    *op_id,
                    if successful && (self.locked.is_none() || self.locked == Some(request.0)) {
                        self.locked = Some(request.0);
                        LockResult::Ok
                    } else {
                        LockResult::No
                    },
                );
            }

            for (op_id, op, _) in &u {
                results.insert(*op_id, self.exec_consensus(op));
            }

            results
        }
    }

    let registry = ChannelRegistry::default();
    let membership = IrMembership::new((0..num_replicas).collect::<Vec<_>>());

    fn create_replica(
        registry: &ChannelRegistry<Upcalls>,
        membership: &IrMembership<usize>,
    ) -> Arc<IrReplica<Upcalls, ChannelTransport<Upcalls>>> {
        Arc::new_cyclic(
            |weak: &std::sync::Weak<IrReplica<Upcalls, ChannelTransport<Upcalls>>>| {
                let weak = weak.clone();
                let channel =
                    registry.channel(move |from, message| weak.upgrade()?.receive(from, message));
                let upcalls = Upcalls { locked: None };
                IrReplica::new(membership.clone(), upcalls, channel, None)
            },
        )
    }

    let mut replicas = std::iter::repeat_with(|| create_replica(&registry, &membership))
        .take(num_replicas)
        .collect::<Vec<_>>();

    fn create_client(
        registry: &ChannelRegistry<Upcalls>,
        membership: &IrMembership<usize>,
    ) -> Arc<IrClient<Upcalls, ChannelTransport<Upcalls>>> {
        let channel = registry.channel(move |_, _| unreachable!());
        Arc::new(IrClient::new(membership.clone(), channel))
    }

    let clients = std::iter::repeat_with(|| create_client(&registry, &membership))
        .take(2)
        .collect::<Vec<_>>();

    let decide_lock = |results: HashMap<LockResult, usize>, membership: IrMembershipSize| {
        //println!("deciding {ok} of {} : {results:?}", results.len());
        if results.get(&LockResult::Ok).copied().unwrap_or_default() >= membership.f_plus_one() {
            LockResult::Ok
        } else {
            LockResult::No
        }
    };

    fn add_replica(
        replicas: &mut Vec<Arc<IrReplica<Upcalls, ChannelTransport<Upcalls>>>>,
        registry: &ChannelRegistry<Upcalls>,
        membership: &IrMembership<usize>,
    ) {
        let new = create_replica(&registry, &membership);
        for d in &*replicas {
            new.transport().do_send(
                d.address(),
                crate::ir::AddMember {
                    address: new.address(),
                },
            );
        }
        replicas.push(new);
    }

    for i in 0..8 {
        assert_eq!(
            clients[0]
                .invoke_consensus(Lock(clients[0].id()), &decide_lock)
                .await,
            LockResult::Ok,
            "{i}"
        );

        assert_eq!(
            clients[1]
                .invoke_consensus(Lock(clients[1].id()), &decide_lock)
                .await,
            LockResult::No,
            "{i}"
        );

        for _ in 0..2 {
            if thread_rng().gen() {
                let to_remove = replicas
                    .iter()
                    .map(|r| r.address())
                    .choose(&mut thread_rng())
                    .unwrap();
                for r in replicas.iter() {
                    clients[0]
                        .transport()
                        .do_send(r.address(), crate::ir::RemoveMember { address: to_remove });
                }
            }
            if thread_rng().gen() {
                add_replica(&mut replicas, &registry, &membership);
            }
        }
    }

    clients[0]
        .invoke_inconsistent(Unlock(clients[0].id()))
        .await;

    for _ in 0..(replicas.len() + 1) * 20 {
        ChannelTransport::<Upcalls>::sleep(Duration::from_secs(5)).await;

        eprintln!("@@@@@ INVOKE {replicas:?}");
        if clients[1]
            .invoke_consensus(Lock(clients[1].id()), &decide_lock)
            .await
            == LockResult::Ok
        {
            return;
        }
    }

    panic!();
}
