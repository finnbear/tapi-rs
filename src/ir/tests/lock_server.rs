use serde::{Deserialize, Serialize};

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
    #[derive(Debug, Clone, Eq, PartialEq)]
    struct Lock(IrClientId);

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct Unlock(IrClientId);

    #[derive(Debug, Clone, Eq, PartialEq, Hash)]
    enum LockResult {
        Ok,
        No,
    }

    type Message = IrMessage<Upcalls>;

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
            for (op_id, entry) in &record.inconsistent {
                unlocked.insert(entry.op.0);
            }
            for (op_id, entry) in &record.consensus {
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
                    if successful && self.locked.is_none() {
                        self.locked = Some(request.0);
                        LockResult::Ok
                    } else {
                        LockResult::No
                    },
                );
            }

            for (op_id, _, _) in &u {
                results.insert(*op_id, LockResult::No);
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
    ) -> Arc<IrClient<Upcalls, ChannelTransport<Message>>> {
        Arc::new_cyclic(
            |weak: &std::sync::Weak<IrClient<Upcalls, ChannelTransport<Message>>>| {
                let weak = weak.clone();
                let channel = registry.channel(move |_, _| unreachable!());
                IrClient::new(membership.clone(), channel)
            },
        )
    }

    let clients = (0..2)
        .map(|_| create_client(&registry, &membership))
        .collect::<Vec<_>>();

    let decide_lock = |results: HashMap<LockResult, usize>, membership: IrMembershipSize| {
        //println!("deciding {ok} of {} : {results:?}", results.len());
        if results.get(&LockResult::Ok).copied().unwrap_or_default() >= membership.f_plus_one() {
            LockResult::Ok
        } else {
            LockResult::No
        }
    };

    for _ in 0..2 {
        assert_eq!(
            clients[0]
                .invoke_consensus(Lock(clients[0].id()), &decide_lock)
                .await,
            LockResult::Ok
        );
        assert_eq!(
            clients[1]
                .invoke_consensus(Lock(clients[1].id()), &decide_lock)
                .await,
            LockResult::No
        );
    }

    clients[0]
        .invoke_inconsistent(Unlock(clients[0].id()))
        .await;

    for i in 0..5 {
        ChannelTransport::<Message>::sleep(Duration::from_secs(8)).await;

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
