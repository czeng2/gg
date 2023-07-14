use async_trait::async_trait;
use maelstrom::kv::{lin_kv, Storage, KV};
use maelstrom::protocol::{Message, MessageBody};
use maelstrom::{done, Node, Result, Runtime};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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
    seen_messages: Mutex<HashMap<String, Vec<(i64, Value)>>>,
}

async fn get_next_offset(kvstore: &Storage, key: String) -> Result<i64> {
    let (ctx, _handle) = Context::with_timeout(Duration::from_millis(250));
    let get_response = kvstore.get(ctx, key.clone()).await;
    let old_offset = if is_maelstrom_error(&get_response, maelstrom::Error::KeyDoesNotExist) {
        -1
    } else {
        get_response?
    };

    let new_offset = old_offset + 1;
    let (ctx, _handle) = Context::with_timeout(Duration::from_millis(250));
    kvstore.cas(ctx, key, old_offset, new_offset, true).await?;
    Ok(new_offset)
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        self.process_inner(runtime, req).await.unwrap();
        Ok(())
    }
}

impl Handler {
    async fn process_inner(&self, runtime: Runtime, req: Message) -> Result<()> {
        let kvstore = lin_kv(runtime.clone());

        if req.get_type() == "send" {
            let key = req.body.extra["key"].as_str().unwrap().to_string();
            let msg = req.body.extra["msg"].clone();

            let mut loop_count = 0;
            let our_offset = loop {
                let offset_response = get_next_offset(&kvstore, key.clone()).await;
                if let Ok(offset) = offset_response {
                    break offset;
                }
                assert!(is_maelstrom_error(
                    &offset_response,
                    maelstrom::Error::PreconditionFailed
                ));
                loop_count += 1;
            };
            if loop_count > 0 {
                eprintln!("retried {} times to get next offset", loop_count);
            }

            let mut forward_body = MessageBody::new().with_type("send_ok");
            forward_body
                .extra
                .insert("offset".into(), our_offset.into());
            runtime.reply(req.clone(), forward_body).await?;

            self.seen_messages
                .lock()
                .expect("could not lock")
                .entry(key)
                .or_default()
                .push((our_offset, msg));

            let (ctx, _handle) = Context::with_timeout(Duration::from_millis(250));
            let kv_key = runtime.node_id();
            let msgs = self.seen_messages.lock().unwrap().clone();
            kvstore.put(ctx, kv_key.to_string(), msgs).await?;

            return Ok(());
        } else if req.get_type() == "poll" {
            // inputs: { (key -> offset) }
            // for each, ask KV store for that (key+offset)
            //  - if found: return list of one item pair (offset, message)
            //  - else: return empty list
            // respond with that entire map

            let mut all_messages = HashMap::<String, Vec<(i64, Value)>>::new();
            for node_id in runtime.nodes() {
                let (ctx, _handle) = Context::with_timeout(Duration::from_millis(250));
                let node_messages_result = kvstore.get(ctx, node_id.to_string()).await;
                if !is_maelstrom_error(&node_messages_result, maelstrom::Error::KeyDoesNotExist) {
                    let node_messages: Value = node_messages_result?;
                    for (key, msg_list) in node_messages.as_object().unwrap() {
                        let msg_list = msg_list.as_array().unwrap().iter().map(|pair| {
                            let pair = pair.as_array().unwrap();
                            let offset = pair[0].as_i64().unwrap();
                            (offset, pair[1].clone())
                        });
                        all_messages
                            .entry(key.clone())
                            .or_default()
                            .extend(msg_list);
                    }
                }
            }

            let mut map = Map::new();

            for (key, offset) in req.body.extra["offsets"].as_object().unwrap().iter() {
                let mut key_list = vec![];
                if let Some(key_messages) = all_messages.get_mut(key) {
                    key_messages.sort_unstable_by_key(|(offset, _)| *offset);
                    // find the offset within the list
                    let mut prev = -1;
                    for (key_offset, msg) in key_messages {
                        if *key_offset != prev + 1 {
                            break;
                        }
                        if *key_offset >= offset.as_i64().unwrap() {
                            key_list.push(json!([key_offset, msg]));
                        }
                        prev = *key_offset;
                    }
                    // let pair: Value = vec![offset.clone(), message.clone()].into();
                    // vec![pair]
                }
                map.insert(key.clone(), key_list.into());
            }

            let mut body = MessageBody::new().with_type("poll_ok");
            body.extra.insert("msgs".into(), map.into());
            return runtime.reply(req.clone(), body).await;
        } else if req.get_type() == "commit_offsets" {
            // key -> offset
            for (key, offset) in req.body.extra["offsets"].as_object().unwrap().iter() {
                let kv_key = commit_key(key);
                let new_offset = offset.as_i64().unwrap();
                loop {
                    let (ctx, _handle) = Context::with_timeout(Duration::from_millis(250));
                    let prev_offset_result = kvstore.get::<Value>(ctx, kv_key.clone()).await;
                    let prev_offset = if is_maelstrom_error(
                        &prev_offset_result,
                        maelstrom::Error::KeyDoesNotExist,
                    ) {
                        -1
                    } else {
                        prev_offset_result?.as_i64().unwrap()
                    };
                    if new_offset <= prev_offset {
                        break;
                    }
                    let (ctx, _handle) = Context::with_timeout(Duration::from_millis(250));
                    let cas_response = kvstore
                        .cas(ctx, kv_key.clone(), prev_offset, new_offset, true)
                        .await;
                    if !is_maelstrom_error(&cas_response, maelstrom::Error::PreconditionFailed) {
                        cas_response?;
                        break;
                    }
                }
            }

            let body = MessageBody::new().with_type("commit_offsets_ok");
            return runtime.reply(req.clone(), body).await;
        } else if req.get_type() == "list_committed_offsets" {
            let mut map = serde_json::Map::new();

            for key in req.body.extra["keys"].as_array().unwrap().iter() {
                let key = key.as_str().unwrap().to_string();
                let kv_key = commit_key(&key);
                let (ctx, _handle) = Context::with_timeout(Duration::from_millis(250));
                let offset_result = kvstore.get::<Value>(ctx, kv_key).await;
                if is_maelstrom_error(&offset_result, maelstrom::Error::KeyDoesNotExist) {
                    continue;
                }

                let offset = offset_result?;
                map.insert(key, offset);
            }

            let mut body = MessageBody::new().with_type("list_committed_offsets_ok");
            body.extra.insert("offsets".into(), map.into());
            return runtime.reply(req.clone(), body).await;
        }

        done(runtime, req)
    }
}

fn is_maelstrom_error<T>(result: &Result<T>, err_type: maelstrom::Error) -> bool {
    matches!(
        result,
        Err(e) if matches!(
            e.downcast_ref::<maelstrom::Error>(),
            Some(e) if *e == err_type,
        ),
    )
}

fn commit_key(key: &str) -> String {
    format!("commit:{}", key)
}
