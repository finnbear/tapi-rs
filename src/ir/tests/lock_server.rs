use crate::{ChannelRegistry, ChannelTransport, IrClient, IrMessage};
use std::sync::{Arc, Mutex};

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

    let registry = ChannelRegistry::default();

    let mut client = Arc::new(Mutex::new(
        Option::<IrClient<ChannelTransport<IrMessage<Op, Res>>, Op, Res>>::None,
    ));
    let client_channel = {
        let client = Arc::clone(&client);
        registry.channel(move |from, message| {
            client
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .receive(from, message)
        })
    };
    *client.lock().unwrap() = Some(IrClient::new(client_channel));

    client
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .invoke_consensus(Op::Lock, |results| Res::Ok)
        .await;
}
