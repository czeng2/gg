use async_trait::async_trait;
use maelstrom::protocol::{Message, MessageBody};
use maelstrom::{done, Node, Result, Runtime};
use serde_json::Value;
use std::collections::HashMap;
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
struct Handler {
    state: tokio::sync::Mutex<HandlerState>,
}

#[derive(Default)]
struct HandlerState {
    database: HashMap<i64, i64>,

    /// Leader-only
    committed_txns: Vec<Value>,
    // not sure if we need this
    // committed_ids: HashMap<String, usize>,
    /// Follower-only
    uncommitted_state: HashMap<i64, i64>,
    last_committed_count: usize,
}

const LEADER_ID: &str = "n1";

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        self.process_inner(runtime, req).await.unwrap();
        Ok(())
    }
}

fn apply_operations(database: &mut HashMap<i64, i64>, txn: &mut Value) {
    let op_array = txn.as_array_mut().unwrap();
    for op_list in op_array {
        let [op, key, value] = op_list.as_array_mut().unwrap().as_mut_slice() else {
            panic!("expected 3 values")
        };

        let int_key = key.as_i64().unwrap();
        match op.as_str().unwrap() {
            "r" => {
                if let Some(&db_value) = database.get(&int_key) {
                    if !value.is_null() {
                        assert_eq!(value.as_i64().unwrap(), db_value);
                    } else {
                        *value = db_value.into();
                    }
                }
            }
            "w" => {
                database.insert(int_key, value.as_i64().unwrap());
            }
            _ => panic!("expected r or w"),
        }
    }

}

impl Handler {
    async fn process_inner(&self, runtime: Runtime, mut req: Message) -> Result<()> {
        if req.get_type() == "txn" {
            if runtime.node_id() == LEADER_ID {
                let mut txn = req.body.extra.remove("txn").unwrap();

                let txn_list;
                {
                    let mut state = self.state.lock().await;
                    apply_operations(&mut state.database, &mut txn);

                    state.committed_txns.push(txn.clone());
                    // gather list of missing txns
                    let start_idx = req.body.extra["first_unknown_id"].as_i64().unwrap() as usize;
                    txn_list = state.committed_txns[start_idx..].to_vec();
                } // drop the lock

                let mut forward_body = MessageBody::new().with_type("txn_ok");
                forward_body.extra.insert("txn".into(), txn);
                if !runtime.is_client(&req.src) {
                    forward_body.extra.insert("new_txns".into(), txn_list.into());
                }
                runtime.reply(req.clone(), forward_body).await?;
            } else {
                {
                    let mut state = self.state.lock().await;

                    let mut forward_body = MessageBody::new().with_type("txn");
                    forward_body.extra.insert("first_unknown_id".into(), state.last_committed_count.into());
                    let mut rpc_handle = runtime.rpc(LEADER_ID, forward_body.clone()).await?;

                    // wait for a reply or time out
                    let (ctx, _handle) = Context::with_timeout(Duration::from_millis(250));
                    let resp = rpc_handle.done_with(ctx).await;
                    if let Ok(mut ok_resp) = resp {
                        // apply missing txns
                        let txn_list = ok_resp.body.extra["new_txns"].as_array_mut().unwrap();
                        state.last_committed_count += txn_list.len();
                        for txn in txn_list {
                            apply_operations(&mut state.database, txn);
                        }

                        let mut forward_body = MessageBody::new().with_type("txn_ok");
                        forward_body.extra.insert("txn".into(), ok_resp.body.extra.remove("txn").unwrap());
                        runtime.reply(req.clone(), forward_body).await?;
                    }
                }
            }
        }

        done(runtime, req)
    }
}
