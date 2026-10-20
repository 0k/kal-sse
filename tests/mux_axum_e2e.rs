#![cfg(feature = "axum")]
//! Spec cases 11--15: actual TCP/HTTP/SSE, not router-only mocks.
#[path = "support/async_source.rs"]
mod async_source;
mod support;

use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use kal_sse::mux::{Channel, Mux, MuxSource, SnapshotPart};
use serde_json::{json, Value};
use support::*;

struct Server {
    mux: Mux,
    a: Arc<Channel<u64>>,
    base: String,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start() -> Self {
        Self::start_with(None).await
    }

    async fn start_with(delayed: Option<Arc<dyn MuxSource>>) -> Self {
        let a = channel(32, 8, 1);
        let mut mux = mux(&[("a", a.clone()), ("room", channel(32, 8, 1))]);
        if let Some(delayed) = delayed {
            mux = mux
                .family("slow", move |_: Option<&str>| Ok(delayed.clone()))
                .unwrap();
        }
        let router = axum::Router::new().nest("/custom", kal_sse::mux_axum::router(mux.clone()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/custom", listener.local_addr().unwrap());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        Self {
            mux,
            a,
            base,
            stop: Some(stop),
            task,
        }
    }

    async fn reserve(&self, channels: Value) -> Value {
        let response = reqwest::Client::new()
            .post(&self.base)
            .json(&json!({"channels": channels}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201);
        response.json().await.unwrap()
    }

    async fn open(&self, conn: &str) -> Reader {
        let response = reqwest::get(format!("{}/stream?conn={conn}", self.base))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert!(response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream"));
        Reader {
            body: Box::pin(response.bytes_stream()),
            buf: String::new(),
        }
    }

    async fn change(&self, conn: &str, body: Value) -> reqwest::Response {
        reqwest::Client::new()
            .post(format!("{}/{conn}/channels", self.base))
            .json(&body)
            .send()
            .await
            .unwrap()
    }

    async fn finish(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(STEP, &mut self.task)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn delayed_snapshot_over_http_keeps_neighbor_live() {
    for after_load in [false, true] {
        let state = channel(32, 8, 1);
        let (source, load, parts) = if after_load {
            async_source::DelayedSource::after_load(state.clone())
        } else {
            async_source::DelayedSource::new(state.clone())
        };
        let s = Server::start_with(Some(source)).await;
        let r = s.reserve(json!(["slow", "a"])).await;
        let conn = r["conn"].as_str().unwrap();
        let mut reader = s.open(conn).await;
        assert_eq!(reader.event().await.0, "hello");
        assert_eq!(
            reader.event().await,
            (
                "snapshot".into(),
                json!({"ch":"a","gen":2,"cursor":0,"part":0,"last":true,"data":0})
            )
        );
        append(&s.a, 1);
        assert_eq!(
            reader.event().await,
            (
                "delta".into(),
                json!({"ch":"a","gen":2,"cursor":1,"data":1})
            )
        );
        load.send(Ok(())).unwrap();
        parts
            .send(Ok(SnapshotPart {
                data: "0".into(),
                last: true,
            }))
            .await
            .unwrap();
        assert_eq!(
            reader.event().await,
            (
                "snapshot".into(),
                json!({"ch":"slow","gen":1,"cursor":0,"part":0,"last":true,"data":0})
            )
        );
        append(&state, 1);
        assert_eq!(
            reader.event().await,
            (
                "delta".into(),
                json!({"ch":"slow","gen":1,"cursor":1,"data":1})
            )
        );
        drop(reader);
        s.mux.release(conn);
        s.finish().await;
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

struct Reader {
    body: Pin<Box<dyn Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>,
    buf: String,
}

impl Reader {
    async fn block(&mut self) -> String {
        tokio::time::timeout(STEP, async {
            loop {
                if let Some(end) = self.buf.find("\n\n") {
                    return self.buf.drain(..end + 2).collect();
                }
                let bytes = self.body.next().await.expect("body ended").unwrap();
                self.buf.push_str(std::str::from_utf8(&bytes).unwrap());
            }
        })
        .await
        .expect("HTTP frame timeout")
    }

    async fn event(&mut self) -> (String, Value) {
        loop {
            let block = self.block().await;
            let mut name = None;
            let mut data = None;
            for line in block.lines() {
                assert!(!line.starts_with("id:"), "mux must not send SSE id fields");
                if let Some(value) = line.strip_prefix("event: ") {
                    name = Some(value.into());
                }
                if let Some(value) = line.strip_prefix("data: ") {
                    data = Some(serde_json::from_str(value).unwrap());
                }
            }
            if let (Some(name), Some(data)) = (name, data) {
                return (name, data);
            }
        }
    }
}

#[tokio::test]
async fn case_11_reserve_stream_hello_snapshots_and_heartbeat() {
    let s = Server::start().await;
    let r = s.reserve(json!(["a", "room:hello"])).await;
    assert_eq!(r["gens"], json!({"a": 1, "room:hello": 2}));
    assert_eq!(
        uuid::Uuid::parse_str(r["conn"].as_str().unwrap())
            .unwrap()
            .get_version_num(),
        4
    );
    let mut reader = s.open(r["conn"].as_str().unwrap()).await;
    assert_eq!(
        reader.event().await,
        (
            "hello".into(),
            json!({"conn": r["conn"], "heartbeat_ms": 20})
        )
    );
    for (key, gen) in [("a", 1), ("room:hello", 2)] {
        assert_eq!(
            reader.event().await,
            (
                "snapshot".into(),
                json!({"ch": key, "gen": gen, "cursor": 0, "part": 0, "last": true, "data": 0})
            )
        );
    }
    assert_eq!(reader.block().await, ": ping\n\n");
    drop(reader);
    s.mux.release(r["conn"].as_str().unwrap());
    s.finish().await;
}

#[tokio::test]
async fn case_12_channels_before_open_uses_updated_set() {
    let s = Server::start().await;
    let r = s.reserve(json!(["a"])).await;
    let conn = r["conn"].as_str().unwrap();
    let response = s
        .change(conn, json!({"add": ["room:hello"], "remove": ["a"]}))
        .await;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({"gens": {"room:hello": 2}})
    );
    let mut reader = s.open(conn).await;
    reader.event().await;
    assert_eq!(reader.event().await.1["ch"], "room:hello");
    assert_eq!(reader.block().await, ": ping\n\n");
    let response = reqwest::Client::new()
        .delete(format!("{}/{conn}", s.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);
    drop(reader);
    s.finish().await;
}

#[tokio::test]
async fn case_13_unknown_family_reservation_is_all_or_nothing_with_reason() {
    let s = Server::start().await;
    let response = reqwest::Client::new()
        .post(&s.base)
        .json(&json!({"channels": ["a", "unknown"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["error"], "invalid_channel");
    assert_eq!(body["channel"], "unknown");
    assert!(!body["reason"].as_str().unwrap().is_empty());
    assert_eq!(s.mux.connection_count(), 0);
    s.finish().await;
}

#[tokio::test]
async fn case_14_unknown_connection_stream_and_channels_need_reservation() {
    let s = Server::start().await;
    let stream = reqwest::get(format!("{}/stream?conn=unknown", s.base))
        .await
        .unwrap();
    let change = s
        .change("unknown", json!({"add": ["a"], "remove": []}))
        .await;
    for response in [stream, change] {
        assert_eq!(response.status(), 409);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["error"], "needs_reservation");
        assert_eq!(body["conn"], "unknown");
        assert!(!body["reason"].as_str().unwrap().is_empty());
    }
    s.finish().await;
}

#[tokio::test]
async fn case_15_provider_refusal_is_a_channel_error_others_continue() {
    let s = Server::start().await;
    let r = s.reserve(json!(["a"])).await;
    let conn = r["conn"].as_str().unwrap();
    let mut reader = s.open(conn).await;
    reader.event().await;
    reader.event().await;
    let response = s
        .change(conn, json!({"add": ["room:denied"], "remove": []}))
        .await;
    assert_eq!(response.status(), 200);
    assert_eq!(
        reader.event().await,
        (
            "channel_error".into(),
            json!({"ch": "room:denied", "gen": 2, "reason": "unknown room"})
        )
    );
    append(&s.a, 1);
    assert_eq!(
        reader.event().await,
        (
            "delta".into(),
            json!({"ch": "a", "gen": 1, "cursor": 1, "data": 1})
        )
    );
    drop(reader);
    s.mux.release(conn);
    s.finish().await;
}

#[tokio::test]
async fn http_resume_query_replays_and_live_remove_confirms_its_generation() {
    let s = Server::start().await;
    let r = s.reserve(json!(["a"])).await;
    let conn = r["conn"].as_str().unwrap();
    let mut first = s.open(conn).await;
    first.event().await;
    first.event().await;
    append(&s.a, 3);
    for cursor in 1..=3 {
        assert_eq!(
            first.event().await,
            (
                "delta".into(),
                json!({"ch": "a", "gen": 1, "cursor": cursor, "data": cursor})
            )
        );
    }
    let response = reqwest::Client::new()
        .get(format!("{}/stream", s.base))
        .query(&[("conn", conn), ("resume", "{\"a\":[1,1]}")])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut resumed = Reader {
        body: Box::pin(response.bytes_stream()),
        buf: String::new(),
    };
    assert_eq!(resumed.event().await.0, "hello");
    for cursor in 2..=3 {
        assert_eq!(
            resumed.event().await,
            (
                "delta".into(),
                json!({"ch": "a", "gen": 1, "cursor": cursor, "data": cursor})
            )
        );
    }
    let overlap = s.change(conn, json!({"add": ["a"], "remove": ["a"]})).await;
    assert_eq!(overlap.status(), 400);
    assert!(!overlap.json::<Value>().await.unwrap()["reason"]
        .as_str()
        .unwrap()
        .is_empty());
    assert_eq!(s.change(conn, json!({"remove": ["a"]})).await.status(), 200);
    assert_eq!(s.a.update(|_, log| log.handle_count()), 0);
    assert_eq!(
        resumed.event().await,
        ("removed".into(), json!({"ch": "a", "gen": 1}))
    );
    drop(first);
    drop(resumed);
    s.mux.release(conn);
    s.finish().await;
}

#[tokio::test]
async fn malformed_http_inputs_carry_reasons_without_creating_connections() {
    let s = Server::start().await;
    let client = reqwest::Client::new();
    for body in ["{}", "[]", "{", "{\"channels\":[123]}"] {
        let response = client
            .post(&s.base)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        assert!(!response.json::<Value>().await.unwrap()["reason"]
            .as_str()
            .unwrap()
            .is_empty());
        assert_eq!(s.mux.connection_count(), 0);
    }
    let r = s.reserve(json!([])).await;
    let conn = r["conn"].as_str().unwrap();
    for resume in ["invalid", "[]", "{\"a\":[1]}", "{\"a\":[-1,0]}"] {
        let response = client
            .get(format!("{}/stream", s.base))
            .query(&[("conn", conn), ("resume", resume)])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        assert!(!response.json::<Value>().await.unwrap()["reason"]
            .as_str()
            .unwrap()
            .is_empty());
    }
    s.mux.release(conn);
    s.finish().await;
}
