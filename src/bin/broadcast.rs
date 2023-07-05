use async_trait::async_trait;
use maelstrom::protocol::{Message, MessageBody};
use maelstrom::{done, Node, RPCResult, Result, Runtime};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::time::Duration;
use tokio_context::context::Context;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Default)]
struct Handler {
    seen_messages: Mutex<HashSet<i64>>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        if req.get_type() == "broadcast" {
            let mut response = req.body.clone().with_type("broadcast_ok");
            response.extra.remove("message");
            self.seen_messages
                .lock()
                .expect("could not lock")
                .insert(req.body.extra["message"].as_i64().expect("not an integer"));
            runtime.reply(req.clone(), response).await?;

            if runtime.is_client(&req.src) {
                let mut send_futures = VecDeque::<(String, RPCResult)>::new();
                let mut forward_body = MessageBody::new().with_type("broadcast");
                forward_body
                    .extra
                    .insert("message".to_string(), req.body.extra["message"].clone());
                for node_id in runtime.nodes() {
                    send_futures.push_back((
                        node_id.to_string(),
                        runtime.rpc(node_id, forward_body.clone()).await?,
                    ));
                }

                while !send_futures.is_empty() {
                    let (node_id, mut first_future) = send_futures.pop_front().unwrap();
                    let (ctx, _handle) = Context::with_timeout(Duration::from_millis(250));
                    let rpc_result = first_future.done_with(ctx).await;
                    if rpc_result.is_err() {
                        eprintln!(
                            "failed to send with error {:?}, retrying {:?} {:?}",
                            rpc_result, node_id, forward_body
                        );
                        send_futures.push_back((
                            node_id.to_string(),
                            runtime.rpc(node_id, forward_body.clone()).await?,
                        ));
                    }
                }
            }
            return Ok(());
        } else if req.get_type() == "read" {
            let mut response = req.body.clone().with_type("read_ok");

            let messages: Vec<_> = self.seen_messages.lock().unwrap().iter().copied().collect();
            response.extra.insert("messages".into(), messages.into());

            return runtime.reply(req, response).await;
        } else if req.get_type() == "topology" {
            let mut response = req.body.clone().with_type("topology_ok");
            response.extra.remove("topology");
            return runtime.reply(req, response).await;
        }

        done(runtime, req)
    }
}
