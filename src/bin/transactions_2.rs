use async_trait::async_trait;
use maelstrom::protocol::{Message, MessageBody};
use maelstrom::{done, Node, Result, Runtime};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Default)]
struct Handler {
    state: Mutex<HandlerState>,
}

#[derive(Default)]
struct HandlerState {
    database: HashMap<i64, i64>,

    /// Leader-only
    committed_txns: Vec<Value>,

    /// Follower-only
    uncommitted_state: HashMap<i64, i64>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        self.process_inner(runtime, req).await.unwrap();
        Ok(())
    }
}

impl Handler {
    async fn process_inner(&self, runtime: Runtime, mut req: Message) -> Result<()> {
        if req.get_type() == "txn" {
            let mut txn = req.body.extra.remove("txn").unwrap();

            let op_array = txn.as_array_mut().unwrap();
            {
                let mut state = self.state.lock().expect("could not lock");
                for op_list in op_array {
                    let [op, key, value] = op_list.as_array_mut().unwrap().as_mut_slice() else {
                        panic!("expected 3 values")
                    };

                    let int_key = key.as_i64().unwrap();
                    match op.as_str().unwrap() {
                        "r" => {
                            if let Some(&db_value) = state.database.get(&int_key) {
                                *value = db_value.into();
                            }
                        }
                        "w" => {
                            state.database.insert(int_key, value.as_i64().unwrap());
                        }
                        _ => panic!("expected r or w"),
                    }
                }

                state.committed_txns.push(txn.clone());
            } // drop the lock

            let mut forward_body = MessageBody::new().with_type("txn_ok");
            forward_body.extra.insert("txn".into(), txn);
            runtime.reply(req.clone(), forward_body).await?;
        }

        done(runtime, req)
    }
}
