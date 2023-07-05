use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use std::sync::Arc;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct Handler {}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        if req.get_type() == "generate" {
            let mut echo = req.body.clone().with_type("generate_ok");
            echo.extra.insert(
                "id".to_string(),
                format!("{}-{}", req.src, req.body.msg_id).into(),
            );
            return runtime.reply(req, echo).await;
        }

        done(runtime, req)
    }
}
