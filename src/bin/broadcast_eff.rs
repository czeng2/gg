use async_trait::async_trait;
use maelstrom::protocol::{Message, MessageBody};
use maelstrom::{done, Node, Result, Runtime};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::time::Duration;
use tokio_context::context::Context;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;

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

const LEADER_ID: &str = "n1";

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        if req.get_type() == "broadcast" {
            let value = req.body.extra["message"].as_i64().expect("not an integer");
            self.seen_messages
                .lock()
                .expect("could not lock")
                .insert(value);

            let mut response_body = req.body.clone().with_type("broadcast_ok");
            response_body.extra.remove("message");
            runtime.reply(req.clone(), response_body).await?;

            if runtime.is_client(&req.src) && runtime.node_id() != LEADER_ID {
                let mut forward_body = MessageBody::new().with_type("broadcast");
                forward_body
                    .extra
                    .insert("message".to_string(), req.body.extra["message"].clone());

                // start
                let send_rpc = || async {
                    // send the value to the leader
                    let mut rpc_handle = runtime.rpc(LEADER_ID, forward_body.clone()).await?;

                    // wait for a reply or time out
                    let (ctx, _handle) = Context::with_timeout(Duration::from_millis(250));
                    rpc_handle.done_with(ctx).await
                };
                // end

                let retry_strategy = ExponentialBackoff::from_millis(2)
                    .factor(100)
                    .max_delay(Duration::from_secs(1))
                    .map(jitter); // add jitter to delays

                let _retry_result = Retry::spawn(retry_strategy, send_rpc).await?;

                // let mut send_futures = VecDeque::<(String, RPCResult)>::new();
                // for node_id in runtime.nodes() {
                //     send_futures.push_back((node_id.to_string(), runtime.rpc(node_id, forward_body.clone()).await?));
                // }

                // while !send_futures.is_empty() {
                //     let (node_id, mut first_future) = send_futures.pop_front().unwrap();
                //     let (ctx, _handle) = Context::with_timeout(Duration::from_millis(1000));
                //     let rpc_result = first_future.done_with(ctx).await;
                //     if rpc_result.is_err() {
                //         eprintln!("failed to send with error {:?}, retrying", rpc_result);
                //         send_futures.push_back((node_id.to_string(), runtime.rpc(node_id, forward_body.clone()).await?));
                //     }
                // }
            }

            let mut response_body = req.body.clone().with_type("broadcast_ok");
            response_body.extra.remove("message");
            runtime.reply(req.clone(), response_body).await?;

            return Ok(());
        } else if req.get_type() == "read" {
            // if we're a follower, ask the leader for the latest set; update ours
            // (allow this to time out)
            if runtime.node_id() != LEADER_ID {
                let forward_body = MessageBody::new().with_type("read");
                let mut rpc_handle = runtime.rpc(LEADER_ID, forward_body.clone()).await?;

                // wait for a reply (todo: timeout=?)
                let (ctx, _handle) = Context::with_timeout(Duration::from_millis(250));
                let resp = rpc_handle.done_with(ctx).await;

                if let Ok(ok_resp) = resp {
                    let mut seen = self.seen_messages.lock().unwrap();
                    for value in ok_resp.body.extra["messages"].as_array().unwrap() {
                        seen.insert(value.as_i64().unwrap());
                    }
                }
            }

            // reply with our freshest data
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
