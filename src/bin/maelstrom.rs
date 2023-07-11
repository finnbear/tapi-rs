#![feature(return_position_impl_trait_in_trait)]
#![feature(let_chains)]
#![allow(incomplete_features)]

use async_maelstrom::msg::Body::{self};
use async_maelstrom::msg::{Error, LinKv, Msg, MsgId};
use async_maelstrom::process::{ProcNet, Process};
use async_maelstrom::runtime::Runtime;
use async_maelstrom::{Id, Status};
use async_trait::async_trait;
use log::info;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tapirs::{
    IrMembership, IrMessage, IrReplica, TapirClient, TapirReplica, TapirTransport, Transport,
};
use tokio::spawn;

type K = String;
type V = String;
type Message = IrMessage<TapirReplica<K, V>, Maelstrom>;

#[derive(Default)]
struct KvNode {
    inner: Option<(Maelstrom, KvNodeInner)>,
}

#[derive(Clone)]
enum KvNodeInner {
    Replica(Arc<IrReplica<TapirReplica<K, V>, Maelstrom>>),
    App(Arc<TapirClient<K, V, Maelstrom>>),
}

impl Maelstrom {
    fn next_msg_id(&self) -> MsgId {
        self.inner.msg_id.fetch_add(1, SeqCst)
    }
}

#[derive(Clone)]
struct Maelstrom {
    id: IdEnum,
    inner: Arc<Inner>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Wrapper {
    message: Message,
    is_reply_to: Option<u64>,
    do_reply_to: Option<u64>,
}

struct Inner {
    requests: Mutex<HashMap<u64, tokio::sync::oneshot::Sender<Message>>>,
    msg_id: AtomicU64,
    net: ProcNet<LinKv, Wrapper>,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
enum IdEnum {
    Replica(usize),
    App(usize),
    Client(usize),
}

impl Display for IdEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Replica(n) => write!(f, "n{}", n + 1),
            Self::App(n) => write!(f, "n{}", n + 3 + 1),
            Self::Client(n) => write!(f, "c{}", n + 1),
        }
    }
}

impl FromStr for IdEnum {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(if let Some(n) = s.strip_prefix('n') {
            let n = usize::from_str(n).map_err(|_| ())? - 1;
            if n < 3 {
                Self::Replica(n)
            } else {
                Self::App(n - 3)
            }
        } else {
            let n = s.strip_prefix('c').ok_or(())?;
            let n = usize::from_str(n).map_err(|_| ())? - 1;
            Self::Client(n)
        })
    }
}

impl Transport<TapirReplica<K, V>> for Maelstrom {
    type Address = IdEnum;
    type Sleep = tokio::time::Sleep;

    fn address(&self) -> Self::Address {
        self.id
    }

    fn persist<T: serde::Serialize>(&self, _key: &str, _value: Option<&T>) {
        // no-op.
    }

    fn persisted<T: serde::de::DeserializeOwned>(&self, _key: &str) -> Option<T> {
        None
    }

    fn time(&self) -> u64 {
        use std::time::SystemTime;
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
            + rand::thread_rng().gen_range(0..1000 * 1000 * 1000)
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        tokio::time::sleep(duration)
    }

    fn send<R: TryFrom<IrMessage<TapirReplica<K, V>, Self>> + Send + std::fmt::Debug>(
        &self,
        address: Self::Address,
        message: impl Into<IrMessage<TapirReplica<K, V>, Self>> + std::fmt::Debug,
    ) -> impl futures::Future<Output = R> + Send + 'static {
        let id = self.id;
        let (sender, mut receiver) = tokio::sync::oneshot::channel();
        let reply = thread_rng().gen();
        self.inner.requests.lock().unwrap().insert(reply, sender);
        let message = Wrapper {
            message: message.into(),
            is_reply_to: None,
            do_reply_to: Some(reply),
        };
        eprintln!("{id} sending {message:?} to {address}");
        let inner = Arc::clone(&self.inner);
        async move {
            loop {
                let _ = inner
                    .net
                    .txq
                    .send(Msg {
                        src: id.to_string(),
                        dest: address.to_string(),
                        body: Body::Application(message.clone()),
                    })
                    .await;

                let sleep = Self::sleep(Duration::from_secs(1));

                tokio::select! {
                    Ok(received) = &mut receiver => {
                        return received.try_into().unwrap_or_else(|_| panic!());
                    }
                    _ = sleep => {
                        continue;
                    }
                }
            }
        }
    }

    fn do_send(
        &self,
        address: Self::Address,
        message: impl Into<IrMessage<TapirReplica<K, V>, Self>> + std::fmt::Debug,
    ) {
        let message = Wrapper {
            message: message.into(),
            do_reply_to: None,
            is_reply_to: None,
        };
        //eprintln!("{} do-sending {message:?} to {address}", self.id);
        let src = self.id.to_string();
        let txq = self.inner.net.txq.clone();
        tokio::spawn(async move {
            let _ = txq
                .send(Msg {
                    src,
                    dest: address.to_string(),
                    body: Body::Application(message),
                })
                .await;
        });
    }
}

impl TapirTransport<K, V> for Maelstrom {
    fn shard_addresses(
        &self,
        shard: tapirs::ShardNumber,
    ) -> impl futures::Future<Output = IrMembership<Self::Address>> + Send + 'static {
        assert_eq!(shard.0, 0);
        std::future::ready(IrMembership::new(vec![
            IdEnum::Replica(0),
            IdEnum::Replica(1),
            IdEnum::Replica(2),
        ]))
    }
}

#[async_trait]
impl Process<LinKv, Wrapper> for KvNode {
    fn init(
        &mut self,
        _args: Vec<String>,
        net: ProcNet<LinKv, Wrapper>,
        id: Id,
        _ids: Vec<Id>,
        start_msg_id: MsgId,
    ) {
        let ids = (0..3).map(IdEnum::Replica).collect::<Vec<_>>();
        let membership = IrMembership::new(ids);
        let id = IdEnum::from_str(&id).unwrap();
        let transport = Maelstrom {
            id,
            inner: Arc::new(Inner {
                requests: Default::default(),
                msg_id: AtomicU64::new(start_msg_id),
                net,
            }),
        };
        self.inner = Some((
            transport.clone(),
            match id {
                IdEnum::Replica(_) => KvNodeInner::Replica(Arc::new(IrReplica::new(
                    membership,
                    TapirReplica::new(tapirs::ShardNumber(0), true),
                    transport,
                ))),
                IdEnum::App(_) => KvNodeInner::App(Arc::new(TapirClient::new(transport))),
                id => panic!("{id}"),
            },
        ));
    }

    async fn run(&self) -> Status {
        fn commit_fault() -> Option<Duration> {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            if rng.gen_bool(0.02) {
                Some(Duration::from_millis(rng.gen_range(0..100)))
            } else {
                None
            }
        }

        let (transport, inner) = self.inner.as_ref().unwrap();
        loop {
            eprintln!("RECEIVING");
            match transport.inner.net.rxq.recv().await {
                Ok(Msg { src, body, .. }) => {
                    eprintln!("received {body:?} from {src}");
                    let transport = transport.clone();
                    let inner = inner.clone();
                    tokio::spawn(async move {
                        match body {
                            Body::Application(app) => {
                                if let Some(reply) = app.is_reply_to {
                                    let mut requests = transport.inner.requests.lock().unwrap();
                                    if let Some(sender) = requests.remove(&reply) {
                                        eprintln!("is reply");
                                        let _ = sender.send(app.message);
                                    } else {
                                        eprintln!("duplicate reply");
                                    }
                                } else if let KvNodeInner::Replica(replica) = &inner {
                                    if let Some(response) =
                                        replica.receive(src.parse::<IdEnum>().unwrap(), app.message)
                                    {
                                        let response = Msg {
                                            src: transport.id.to_string(),
                                            dest: src.clone(),
                                            body: Body::Application(Wrapper {
                                                message: response,
                                                do_reply_to: None,
                                                is_reply_to: app.do_reply_to,
                                            }),
                                        };
                                        eprintln!("sending response {response:?}");
                                        let _ =
                                            transport.inner.net.txq.send(response).await.unwrap();
                                    } else {
                                        eprintln!("NO RESPONSE");
                                    }
                                } else {
                                    eprintln!("(was unsolicited)");
                                }
                            }
                            Body::Workload(work) => {
                                if let KvNodeInner::App(app) = &inner {
                                    let txn = app.begin();
                                    match work {
                                        LinKv::Cas {
                                            msg_id,
                                            key,
                                            from,
                                            to,
                                        } => {
                                            let key = serde_json::to_string(&key).unwrap();
                                            let old = txn
                                                .get(key.clone())
                                                .await
                                                .map(|s| serde_json::from_str(&s).unwrap());
                                            let swap = old == Some(from);
                                            if swap {
                                                let to = serde_json::to_string(&to).unwrap();
                                                txn.put(key, Some(to));
                                            }

                                            if txn.commit2(commit_fault()).await.is_some() {
                                                if old.is_none() {
                                                    let _ = transport
                                                        .inner
                                                        .net
                                                        .txq
                                                        .send(Msg {
                                                            src: transport.id.to_string(),
                                                            dest: src,
                                                            body: Body::Error(Error {
                                                                in_reply_to: msg_id,
                                                                text: String::from(
                                                                    "cas key not found",
                                                                ),
                                                                code: 20,
                                                            }),
                                                        })
                                                        .await;
                                                } else if swap {
                                                    let _ = transport
                                                        .inner
                                                        .net
                                                        .txq
                                                        .send(Msg {
                                                            src: transport.id.to_string(),
                                                            dest: src,
                                                            body: Body::Workload(LinKv::CasOk {
                                                                in_reply_to: msg_id,
                                                                msg_id: Some(
                                                                    transport.next_msg_id(),
                                                                ),
                                                            }),
                                                        })
                                                        .await;
                                                } else {
                                                    let _ = transport
                                                        .inner
                                                        .net
                                                        .txq
                                                        .send(Msg {
                                                            src: transport.id.to_string(),
                                                            dest: src,
                                                            body: Body::Error(Error {
                                                                in_reply_to: msg_id,
                                                                text: String::from(
                                                                    "cas precondition failed",
                                                                ),
                                                                code: 22,
                                                            }),
                                                        })
                                                        .await;
                                                }
                                            } else {
                                                let _ = transport
                                                    .inner
                                                    .net
                                                    .txq
                                                    .send(Msg {
                                                        src: transport.id.to_string(),
                                                        dest: src,
                                                        body: Body::Error(Error {
                                                            in_reply_to: msg_id,
                                                            text: String::from("cas txn conflict"),
                                                            code: 30,
                                                        }),
                                                    })
                                                    .await;
                                            }
                                        }
                                        LinKv::Read { msg_id, key } => {
                                            let key = serde_json::to_string(&key).unwrap();
                                            let old = txn.get(key.clone()).await.map(|s| {
                                                serde_json::from_str::<serde_json::Value>(&s)
                                                    .unwrap()
                                            });
                                            if txn.commit2(commit_fault()).await.is_some() {
                                                if let Some(old) = old {
                                                    let _ = transport
                                                        .inner
                                                        .net
                                                        .txq
                                                        .send(Msg {
                                                            src: transport.id.to_string(),
                                                            dest: src,
                                                            body: Body::Workload(LinKv::ReadOk {
                                                                in_reply_to: msg_id,
                                                                msg_id: Some(
                                                                    transport.next_msg_id(),
                                                                ),
                                                                value: old,
                                                            }),
                                                        })
                                                        .await;
                                                } else {
                                                    let _ = transport
                                                        .inner
                                                        .net
                                                        .txq
                                                        .send(Msg {
                                                            src: transport.id.to_string(),
                                                            dest: src,
                                                            body: Body::Error(Error {
                                                                in_reply_to: msg_id,
                                                                text: String::from("not found"),
                                                                code: 20,
                                                            }),
                                                        })
                                                        .await;
                                                }
                                            } else {
                                                let _ = transport
                                                    .inner
                                                    .net
                                                    .txq
                                                    .send(Msg {
                                                        src: transport.id.to_string(),
                                                        dest: src,
                                                        body: Body::Error(Error {
                                                            in_reply_to: msg_id,
                                                            text: String::from("read txn conflict"),
                                                            code: 30,
                                                        }),
                                                    })
                                                    .await;
                                            }
                                        }
                                        LinKv::Write { msg_id, key, value } => {
                                            let key = serde_json::to_string(&key).unwrap();
                                            let value = serde_json::to_string(&value).unwrap();
                                            txn.put(key, Some(value));
                                            if txn.commit2(commit_fault()).await.is_some() {
                                                let _ = transport
                                                    .inner
                                                    .net
                                                    .txq
                                                    .send(Msg {
                                                        src: transport.id.to_string(),
                                                        dest: src,
                                                        body: Body::Workload(LinKv::WriteOk {
                                                            in_reply_to: msg_id,
                                                        }),
                                                    })
                                                    .await;
                                            } else {
                                                let _ = transport
                                                    .inner
                                                    .net
                                                    .txq
                                                    .send(Msg {
                                                        src: transport.id.to_string(),
                                                        dest: src,
                                                        body: Body::Error(Error {
                                                            in_reply_to: msg_id,
                                                            text: String::from(
                                                                "write txn conflict",
                                                            ),
                                                            code: 30,
                                                        }),
                                                    })
                                                    .await;
                                            }
                                        }
                                        _ => unreachable!(),
                                    }
                                } else {
                                    // Proxy...
                                    eprintln!("Proxying...");
                                    let _ = transport
                                        .inner
                                        .net
                                        .txq
                                        .send(Msg {
                                            src: src.clone(),
                                            dest: IdEnum::App({
                                                let n = thread_rng().gen::<usize>();
                                                n % 2
                                            })
                                            .to_string(),
                                            body: Body::Workload(work),
                                        })
                                        .await;
                                };
                            }
                            body => unreachable!("{body:?}"),
                        }
                    });
                }
                Err(_) => {
                    eprintln!("shutting down recv");
                    return Ok(());
                } // Runtime is shutting down.
            };
        }
    }
}

#[tokio::main]
async fn main() -> Status {
    // Log to stderr where Maelstrom will capture it
    env_logger::init();
    info!("starting");

    let process: KvNode = Default::default();
    let r = Arc::new(Runtime::new(env::args().collect(), process).await?);

    // Drive the runtime, and ...
    let (r1, r2, r3) = (r.clone(), r.clone(), r.clone());
    let t1 = spawn(async move { r1.run_io_egress().await });
    let t2 = spawn(async move { r2.run_io_ingress().await });
    let t3 = spawn(async move { r3.run_process().await });

    // ... wait until the Maelstrom system closes stdin and stdout
    info!("running");
    let _ignored = tokio::join!(t1, t2, t3);

    info!("stopped");

    Ok(())
}
