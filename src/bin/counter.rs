use async_trait::async_trait;
use maelstrom::kv::{seq_kv, Storage, KV};
use maelstrom::protocol::{Message, MessageBody};
use maelstrom::{done, Node, Result, Runtime};
use std::sync::Arc;
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
struct Handler {}

async fn read_or_zero(kvstore: &Storage, node_id: String) -> Result<i64> {
    let (ctx, _handle) = Context::with_timeout(Duration::from_millis(250));
    let get_response = kvstore.get(ctx, node_id).await;
    if let Err(e) = &get_response {
        if let Some(maelstrom::Error::KeyDoesNotExist) = e.downcast_ref::<maelstrom::Error>() {
            return Ok(0);
        }
    }
    get_response
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let kvstore = seq_kv(runtime.clone());

        if req.get_type() == "add" {
            let delta = req.body.extra["delta"].as_i64().expect("not an integer");

            let kvstore = seq_kv(runtime.clone());
            let old_value = read_or_zero(&kvstore, runtime.node_id().to_string()).await?;

            let (ctx, _handle) = Context::with_timeout(Duration::from_millis(250));
            let new_value = old_value + delta;
            kvstore
                .cas(
                    ctx,
                    runtime.node_id().to_string(),
                    old_value,
                    new_value,
                    true,
                )
                .await?;

            let forward_body = MessageBody::new().with_type("add_ok");
            runtime.reply(req.clone(), forward_body).await?;

            return Ok(());
        } else if req.get_type() == "read" {
            let all_nodes = runtime.nodes();
            let mut sum = 0;
            for node in all_nodes {
                sum += read_or_zero(&kvstore, node.to_string()).await?;
            }

            // reply with our freshest data
            let mut response = req.body.clone().with_type("read_ok");
            let value = sum;
            response.extra.insert("value".into(), value.into());
            return runtime.reply(req, response).await;
        }

        done(runtime, req)
    }
}
