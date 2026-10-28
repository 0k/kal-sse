//! Acceptance ledger:
//! - metadata/default reservation APIs: reservation_info_and_default_compatibility
//! - ordered desired channels, attach/detach/release/expiry and exact revisions:
//!   connection_lifecycle_notifies_only_for_connection_transitions
//! - failed/no-op commands and bounded-control rejection: refused_changes_do_not_notify
//! - HTTP headers, reserve/add open_for, device-specific sources, disconnect:
//!   http::reserve_headers_reach_initial_and_added_sources
mod support;

use std::collections::BTreeMap;
use std::time::Instant;

use kal_sse::mux::{ConnInfo, ConnSnapshot, Mux, MuxConfig, MuxError, ResumeHints};
use support::*;

fn keys(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).into()).collect()
}

fn info(device: &str) -> ConnInfo {
    ConnInfo {
        tags: BTreeMap::from([("device".into(), device.into())]),
    }
}

fn snapshot(conn: &str, info: ConnInfo, channels: &[&str], attached: bool) -> ConnSnapshot {
    ConnSnapshot {
        conn: conn.into(),
        info,
        channels: keys(channels),
        attached,
    }
}

fn revision(receiver: &mut tokio::sync::watch::Receiver<u64>, expected: u64) {
    assert_eq!(*receiver.borrow_and_update(), expected);
    assert!(!receiver.has_changed().unwrap());
}

#[test]
fn reservation_info_and_default_compatibility() {
    let mux = Mux::new(MuxConfig::default());
    let now = Instant::now();
    let tagged = mux.reserve_with(vec![], info("phone")).unwrap();
    let timed = mux.reserve_with_at(vec![], info("laptop"), now).unwrap();
    let default = mux.reserve(vec![]).unwrap();
    let default_timed = mux.reserve_at(vec![], now).unwrap();
    let mut expected = vec![
        snapshot(&tagged.conn, info("phone"), &[], false),
        snapshot(&timed.conn, info("laptop"), &[], false),
        snapshot(&default.conn, ConnInfo::default(), &[], false),
        snapshot(&default_timed.conn, ConnInfo::default(), &[], false),
    ];
    expected.sort_by(|a, b| a.conn.cmp(&b.conn));
    assert_eq!(mux.connections(), expected);
    // Revisions survive periods without subscribers and are shared by clones.
    let mut changes = mux.clone().subscribe_connections();
    assert_eq!(*changes.borrow_and_update(), 4);
    assert!(!changes.has_changed().unwrap());
}

#[tokio::test]
async fn connection_lifecycle_notifies_only_for_connection_transitions() {
    let a = channel(32, 8, 1);
    let mux = mux(&[("a", a.clone())]);
    let mut changes = mux.subscribe_connections();
    assert_eq!(*changes.borrow(), 0);
    let now = Instant::now();
    let r = mux
        .reserve_with_at(keys(&["a:z", "a:x", "a:z"]), info("phone"), now)
        .unwrap();
    revision(&mut changes, 1);
    assert_eq!(
        mux.connections(),
        vec![snapshot(&r.conn, info("phone"), &["a:z", "a:x"], false)]
    );
    let mut stream = mux.stream_at(&r.conn, ResumeHints::new(), now).unwrap();
    revision(&mut changes, 2);
    tokio::time::timeout(STEP, stream.next())
        .await
        .unwrap()
        .unwrap(); // hello
    stream.next().await.unwrap(); // rotate scheduling after just one snapshot
    assert_eq!(
        mux.connections(),
        vec![snapshot(&r.conn, info("phone"), &["a:z", "a:x"], true)]
    );
    append(&a, 1);
    stream.next().await.unwrap();
    stream.next().await.unwrap();
    revision(&mut changes, 2); // snapshots and deltas do not notify
    mux.change_at(&r.conn, keys(&["a:y", "a:y"]), keys(&["a:z"]), now)
        .unwrap();
    revision(&mut changes, 3); // one atomic change, not one notification per key
    assert_eq!(
        mux.connections(),
        vec![snapshot(&r.conn, info("phone"), &["a:x", "a:y"], true)]
    );
    mux.change_at(&r.conn, keys(&["a:x"]), keys(&["a:absent"]), now)
        .unwrap();
    revision(&mut changes, 3);
    let replacement = mux.stream_at(&r.conn, ResumeHints::new(), now).unwrap();
    revision(&mut changes, 4);
    drop(stream); // stale epoch cannot detach the replacement or notify
    revision(&mut changes, 4);
    mux.compact(now + TTL * 2); // attached streams never expire
    revision(&mut changes, 4);
    replacement.disconnect_at(now);
    revision(&mut changes, 5);
    assert_eq!(
        mux.connections(),
        vec![snapshot(&r.conn, info("phone"), &["a:x", "a:y"], false)]
    );
    let unpolled = mux.stream_at(&r.conn, ResumeHints::new(), now).unwrap();
    revision(&mut changes, 6);
    drop(unpolled);
    revision(&mut changes, 7);
    mux.release(&r.conn);
    revision(&mut changes, 8);
    assert_eq!(mux.connections(), vec![]);
    mux.release(&r.conn);
    revision(&mut changes, 8);
    let first = mux.reserve_at(keys(&["a"]), now).unwrap();
    let second = mux.reserve_at(vec![], now).unwrap();
    revision(&mut changes, 10); // watch may coalesce, but counts every mutation
    mux.compact(now + TTL);
    revision(&mut changes, 11); // one compaction batch
    assert_eq!(mux.connections(), vec![]);
    assert_eq!(mux.connection_count(), 0);
    assert_eq!(a.update(|_, log| log.handle_count()), 0);
    for conn in [&first.conn, &second.conn] {
        assert!(matches!(
            mux.stream_at(conn, ResumeHints::new(), now + TTL),
            Err(MuxError::NeedsReservation { .. })
        ));
    }
    mux.compact(now + TTL * 2);
    revision(&mut changes, 11);
}

#[test]
fn refused_changes_do_not_notify() {
    let a = channel(32, 8, 1);
    let source = a.clone();
    let mux = Mux::new(MuxConfig {
        control_capacity: 1,
        ..MuxConfig::default()
    })
    .family("a", move |_: Option<&str>| {
        Ok(source.clone() as std::sync::Arc<dyn kal_sse::mux::MuxSource>)
    })
    .unwrap();
    let mut changes = mux.subscribe_connections();
    assert!(mux.reserve_with(keys(&["missing"]), info("phone")).is_err());
    revision(&mut changes, 0);
    let r = mux.reserve(keys(&["a:x", "a:y"])).unwrap();
    let stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    mux.change(&r.conn, vec![], keys(&["a:x"])).unwrap();
    revision(&mut changes, 3);
    for (add, remove) in [
        (keys(&["missing"]), vec![]),
        (keys(&["a:y"]), keys(&["a:y"])),
        (vec![], keys(&["A"])),
    ] {
        assert!(mux.change(&r.conn, add, remove).is_err());
    }
    assert!(matches!(
        mux.change(&r.conn, keys(&["a:z"]), keys(&["a:y"])),
        Err(MuxError::Busy { .. })
    ));
    assert!(mux.change("unknown", vec![], vec![]).is_err());
    revision(&mut changes, 3);
    assert_eq!(
        mux.connections(),
        vec![snapshot(&r.conn, ConnInfo::default(), &["a:y"], true)]
    );
    mux.release(&r.conn);
    revision(&mut changes, 4);
    drop(stream); // release already removed it
    revision(&mut changes, 4);
}

#[test]
fn expired_connections_are_hidden_even_before_compaction() {
    let mux = Mux::new(MuxConfig {
        ttl: std::time::Duration::ZERO,
        ..MuxConfig::default()
    });
    mux.reserve(vec![]).unwrap();
    assert_eq!(mux.connections(), vec![]);
    let mut changes = mux.subscribe_connections();
    assert_eq!(*changes.borrow_and_update(), 1);
    mux.compact(Instant::now());
    revision(&mut changes, 2);
}

#[cfg(feature = "axum")]
mod http {
    use super::*;
    use futures::StreamExt;
    use kal_sse::mux::{MuxFamily, MuxSource};
    use serde_json::{json, Value};
    use std::sync::{Arc, Mutex};

    type OpenCalls = Arc<Mutex<Vec<(Option<String>, ConnInfo)>>>;

    struct TaggedFamily {
        calls: OpenCalls,
    }

    impl MuxFamily for TaggedFamily {
        fn open(&self, _: Option<&str>) -> Result<Arc<dyn MuxSource>, String> {
            panic!("mux must use open_for");
        }

        fn open_for(
            &self,
            param: Option<&str>,
            info: &ConnInfo,
        ) -> Result<Arc<dyn MuxSource>, String> {
            self.calls
                .lock()
                .unwrap()
                .push((param.map(str::to_owned), info.clone()));
            let device = info.tags.get("device").cloned().unwrap_or_default();
            Ok(Arc::new(kal_sse::mux::Channel::new(
                device,
                kal_sse::EventLog::new(TTL, 32, 8),
                |device| Ok(vec![json!(device).to_string()]),
            )))
        }
    }

    #[tokio::test]
    async fn reserve_headers_reach_initial_and_added_sources() {
        let calls = Arc::new(Mutex::new(vec![]));
        let mux = Mux::new(MuxConfig::default())
            .family(
                "room",
                TaggedFamily {
                    calls: calls.clone(),
                },
            )
            .unwrap();
        let router = kal_sse::mux_axum::router_with_tagger(mux.clone(), |headers| {
            headers
                .get("x-trusted-device")
                .map_or_else(ConnInfo::default, |device| info(device.to_str().unwrap()))
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        let client = reqwest::Client::new();
        for device in ["phone", "laptop"] {
            let response = client
                .post(format!("{base}/"))
                .header("x-trusted-device", device)
                .json(&json!({"channels":["room:initial"]}))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 201);
            let reservation: Value = response.json().await.unwrap();
            let conn = reservation["conn"].as_str().unwrap();
            assert_eq!(
                mux.connections(),
                vec![snapshot(conn, info(device), &["room:initial"], false)]
            );
            assert_eq!(*calls.lock().unwrap(), vec![]); // lazy-open compatibility
            let response = client
                .get(format!("{base}/stream?conn={conn}"))
                .header("x-trusted-device", "not-the-reserving-device")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            let mut body = response.bytes_stream();
            let mut received = String::new();
            tokio::time::timeout(STEP, async {
                while !received.contains("event: snapshot") {
                    received.push_str(
                        std::str::from_utf8(&body.next().await.unwrap().unwrap()).unwrap(),
                    );
                }
            })
            .await
            .unwrap();
            let response = client
                .post(format!("{base}/{conn}/channels"))
                .header("x-trusted-device", "also-not-the-device")
                .json(&json!({"add":["room:later"]}))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(
                response.json::<Value>().await.unwrap(),
                json!({"gens":{"room:later":2}})
            );
            tokio::time::timeout(STEP, async {
                while !received.contains("\"ch\":\"room:later\"") {
                    received.push_str(
                        std::str::from_utf8(&body.next().await.unwrap().unwrap()).unwrap(),
                    );
                }
            })
            .await
            .unwrap();
            let payloads: Vec<Value> = received
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .map(|data| serde_json::from_str(data).unwrap())
                .collect();
            assert_eq!(
                payloads,
                vec![
                    json!({"conn":conn,"heartbeat_ms":15000}),
                    json!({"ch":"room:initial","gen":1,"cursor":0,"part":0,"last":true,"data":device}),
                    json!({"ch":"room:later","gen":2,"cursor":0,"part":0,"last":true,"data":device})
                ]
            );
            assert_eq!(
                std::mem::take(&mut *calls.lock().unwrap()),
                vec![
                    (Some("initial".into()), info(device)),
                    (Some("later".into()), info(device))
                ]
            );
            assert_eq!(
                mux.connections(),
                vec![snapshot(
                    conn,
                    info(device),
                    &["room:initial", "room:later"],
                    true
                )]
            );
            let mut changes = mux.subscribe_connections();
            drop(body);
            tokio::time::timeout(STEP, async {
                while mux.connections()[0].attached {
                    changes.changed().await.unwrap();
                }
            })
            .await
            .unwrap();
            assert_eq!(
                mux.connections(),
                vec![snapshot(
                    conn,
                    info(device),
                    &["room:initial", "room:later"],
                    false
                )]
            );
            let response = client
                .delete(format!("{base}/{conn}"))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 204);
            assert_eq!(mux.connections(), vec![]);
        }
        stop.send(()).unwrap();
        tokio::time::timeout(STEP, server).await.unwrap().unwrap();
    }
}
