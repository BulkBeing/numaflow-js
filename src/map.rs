use chrono::{DateTime, Utc};
use napi::Result;
use napi::{bindgen_prelude::*, threadsafe_function::ThreadsafeFunction};
use napi_derive::napi;
use numaflow::map;
use numaflow::shared::ServerExtras;
use std::sync::Arc;
use std::{collections::HashMap, sync::Mutex};

#[napi(object)]
pub struct Message {
  /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
  /// be an empty collection.
  pub keys: Option<Vec<String>>,
  /// Value is the value passed to the next vertex.
  pub value: Buffer,
  /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
  pub tags: Option<Vec<String>>,
}

#[napi]
pub fn message_to_drop() -> Message {
  Message {
    keys: None,
    value: Buffer::from(vec![]),
    tags: Some(vec![numaflow::shared::DROP.to_string()]),
  }
}

impl From<Message> for map::Message {
  fn from(value: Message) -> Self {
    Self {
      keys: value.keys,
      value: value.value.into(),
      tags: value.tags,
    }
  }
}

#[napi(object)]
pub struct Datum {
  /// Set of keys in the (key, value) terminology of map/reduce paradigm.
  pub keys: Vec<String>,
  /// The value in the (key, value) terminology of map/reduce paradigm.
  pub value: Buffer,
  /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a
  /// guarantee that we will not see an element older than this time.
  pub watermark: DateTime<Utc>,
  /// Time of the element as seen at source or aligned after a reduce operation.
  pub eventtime: DateTime<Utc>,
  /// Headers for the message.
  pub headers: HashMap<String, String>,
}

impl Clone for Datum {
  fn clone(&self) -> Self {
    Self {
      keys: self.keys.clone(),
      value: Buffer::from(self.value.to_vec()),
      watermark: self.watermark.clone(),
      eventtime: self.eventtime.clone(),
      headers: self.headers.clone(),
    }
  }
}

impl From<map::MapRequest> for Datum {
  fn from(value: map::MapRequest) -> Self {
    Self {
      keys: value.keys,
      value: value.value.into(),
      watermark: value.watermark,
      eventtime: value.eventtime,
      headers: value.headers,
    }
  }
}

#[napi]
pub struct MapAsyncServer {
  map_fn: Arc<ThreadsafeFunction<Datum, Promise<Vec<Message>>, Datum, Status, false, true>>,
  sock_file: Option<String>,
  info_file: Option<String>,
  shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[napi]
impl MapAsyncServer {
  #[napi(constructor)]
  pub fn new(
    map_fn: Arc<ThreadsafeFunction<Datum, Promise<Vec<Message>>, Datum, Status, false, true>>,
    sock_file: Option<String>,
    info_file: Option<String>,
  ) -> Self {
    Self {
      map_fn,
      sock_file,
      info_file,
      shutdown_tx: Mutex::new(None),
    }
  }

  #[napi]
  pub async fn start(&self) -> Result<()> {
    let js_mapper = JsMapper::new(Arc::clone(&self.map_fn));

    let mut server = map::Server::new(js_mapper);
    if let Some(sock_file) = &self.sock_file {
      server = server.with_socket_file(sock_file);
    }
    if let Some(info_file) = &self.info_file {
      server = server.with_server_info_file(info_file);
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    self.shutdown_tx.lock().unwrap().replace(tx);
    if let Err(e) = server.start_with_shutdown(rx).await {
      println!("Error running Numaflow: {e:?}");
    }
    println!("Numaflow Core has shutdown...");
    Ok(())
  }

  #[napi]
  pub fn stop(&self) -> Result<()> {
    let tx = { self.shutdown_tx.lock().unwrap().take() };
    if let Some(tx) = tx {
      let _ = tx.send(());
    }
    Ok(())
  }
}

struct JsMapper {
  map_fn: Arc<ThreadsafeFunction<Datum, Promise<Vec<Message>>, Datum, Status, false, true>>,
}

impl JsMapper {
  fn new(
    map_fn: Arc<ThreadsafeFunction<Datum, Promise<Vec<Message>>, Datum, Status, false, true>>,
  ) -> Self {
    Self { map_fn }
  }
}

#[async_trait::async_trait]
impl map::Mapper for JsMapper {
  async fn map(&self, datum: map::MapRequest) -> Vec<map::Message> {
    let datum: Datum = datum.into();
    let messages = self.map_fn.call_async(datum).await.unwrap().await.unwrap();
    let messages = messages.into_iter().map(|message| message.into()).collect();
    messages
  }
}
