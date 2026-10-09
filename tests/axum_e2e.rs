//! End-to-end tests of the `axum` adapter over real HTTP: a real axum
//! server on `127.0.0.1:0`, driven by a real HTTP client.
//!
//! Each test checks one clause of the wire contract:
//! * the stream delivers every event after the snapshot, `id:` contiguous;
//! * a reconnect resumes right after the client's `Last-Event-ID`, even
//!   when the server already sent later events into the dead connection;
//! * the same id given as the `last_event_id` query parameter (a browser
//!   `EventSource` cannot set headers) resumes the same way, and the
//!   header wins when both are sent;
//! * a `Last-Event-ID` older than the snapshot gets `409 needs_snapshot`;
//! * a `Last-Event-ID` no longer retained gets `409 needs_snapshot`;
//! * a malformed `Last-Event-ID` gets `400`;
//! * an unknown `sub_id` gets `409 needs_snapshot`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures::StreamExt;
use kal_sse::axum::{last_event_id, sse_response_from, LAST_EVENT_ID_PARAM, SNAPSHOT_INDEX_HEADER};
use kal_sse::EventLog;
use serde::Deserialize;

const TTL: Duration = Duration::from_secs(60);
const STEP: Duration = Duration::from_secs(5);

type Log = Arc<Mutex<EventLog<String>>>;

/// Server state: the log, plus the payloads the adapter has serialized,
/// i.e. handed to the HTTP stack (the cursor has advanced past them).
#[derive(Clone)]
struct App {
    log: Log,
    sent: Arc<Mutex<Vec<String>>>,
}

#[derive(Deserialize)]
struct SnapshotQuery {
    sub: Option<String>,
}

#[derive(Deserialize)]
struct StreamQuery {
    sub: String,
    last_event_id: Option<String>,
}

async fn snapshot(State(app): State<App>, Query(q): Query<SnapshotQuery>) -> impl IntoResponse {
    let (sub_id, cursor) = app.log.lock().unwrap().register(q.sub);
    let mut headers = HeaderMap::new();
    headers.insert(SNAPSHOT_INDEX_HEADER, cursor.to_string().parse().unwrap());
    (headers, sub_id)
}

async fn stream(
    State(app): State<App>,
    Query(q): Query<StreamQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let last = match last_event_id(&headers, q.last_event_id.as_deref()) {
        Ok(last) => last,
        Err(rejection) => return Err(rejection),
    };
    let sent = app.sent.clone();
    sse_response_from(
        app.log,
        q.sub,
        last,
        move |e: &String| {
            sent.lock().unwrap().push(e.clone());
            e.clone()
        },
        "resync via GET /things",
    )
}

async fn serve_with(log: EventLog<String>) -> (App, String) {
    let app = App {
        log: Arc::new(Mutex::new(log)),
        sent: Arc::default(),
    };
    let router = Router::new()
        .route("/things", get(snapshot))
        .route("/things/stream", get(stream))
        .with_state(app.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (app, base)
}

async fn serve() -> (App, String) {
    serve_with(EventLog::new(TTL, 1024, 64)).await
}

/// Take the snapshot (optionally proposing `sub`); returns
/// `(sub_id, snapshot index)`.
async fn take_snapshot(base: &str, sub: Option<&str>) -> (String, u64) {
    let url = match sub {
        Some(sub) => format!("{base}/things?sub={sub}"),
        None => format!("{base}/things"),
    };
    let res = reqwest::get(url).await.unwrap();
    assert_eq!(res.status(), 200);
    let index = res.headers()[SNAPSHOT_INDEX_HEADER]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    (res.text().await.unwrap(), index)
}

/// `GET` the stream, sending `Last-Event-ID` when given.
async fn get_stream(base: &str, sub: &str, last_event_id: Option<&str>) -> reqwest::Response {
    get_stream_with(base, sub, last_event_id, None).await
}

/// `GET` the stream with an optional `Last-Event-ID` header and an
/// optional `last_event_id` query parameter.
async fn get_stream_with(
    base: &str,
    sub: &str,
    header: Option<&str>,
    query: Option<&str>,
) -> reqwest::Response {
    let mut url = format!("{base}/things/stream?sub={sub}");
    if let Some(id) = query {
        url.push_str(&format!("&{LAST_EVENT_ID_PARAM}={id}"));
    }
    let mut req = reqwest::Client::new().get(url);
    if let Some(id) = header {
        req = req.header("Last-Event-ID", id);
    }
    req.send().await.unwrap()
}

/// Reads `(id, data)` SSE events from one open stream, skipping comments.
struct SseReader {
    body: std::pin::Pin<Box<dyn futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>,
    buf: String,
}

impl SseReader {
    async fn open(base: &str, sub: &str, last_event_id: Option<&str>) -> Self {
        Self::from_response(get_stream(base, sub, last_event_id).await)
    }

    fn from_response(res: reqwest::Response) -> Self {
        assert_eq!(res.status(), 200);
        SseReader {
            body: Box::pin(res.bytes_stream()),
            buf: String::new(),
        }
    }

    /// Next event, or panic after `STEP`.
    async fn next_event(&mut self) -> (u64, String) {
        tokio::time::timeout(STEP, async {
            loop {
                while let Some(end) = self.buf.find("\n\n") {
                    let block: String = self.buf.drain(..end + 2).collect();
                    let (mut id, mut data) = (None, None);
                    for line in block.lines() {
                        if let Some(v) = line.strip_prefix("id: ") {
                            id = Some(v.parse().unwrap());
                        } else if let Some(v) = line.strip_prefix("data: ") {
                            data = Some(v.to_string());
                        }
                    }
                    if let (Some(id), Some(data)) = (id, data) {
                        return (id, data);
                    }
                }
                let chunk = self.body.next().await.expect("stream ended").unwrap();
                self.buf.push_str(std::str::from_utf8(&chunk).unwrap());
            }
        })
        .await
        .expect("no SSE event within the timeout")
    }

    async fn next_events(&mut self, n: usize) -> Vec<(u64, String)> {
        let mut out = Vec::new();
        for _ in 0..n {
            out.push(self.next_event().await);
        }
        out
    }
}

fn append(log: &Log, events: &[&str]) {
    let mut g = log.lock().unwrap();
    for e in events {
        g.append(e.to_string());
    }
}

/// Poll `cond` until true, or panic after `STEP`.
async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(start.elapsed() < STEP, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Client reads events "a", "b" (ids 1, 2) on `sub`, then closes its
/// connection; the server then sends "c", "d" (ids 3, 4) into the closed
/// connection. Returns once the server has handed "d" to the HTTP stack.
async fn lose_c_and_d_in_a_closed_connection(app: &App, base: &str, sub: &str) {
    let mut reader = SseReader::open(base, sub, None).await;
    append(&app.log, &["a", "b"]);
    assert_eq!(
        reader.next_events(2).await,
        vec![(1, "a".into()), (2, "b".into())]
    );
    drop(reader);
    append(&app.log, &["c", "d"]);
    wait_until(
        "the server has sent \"d\" into the closed connection",
        || app.sent.lock().unwrap().iter().any(|e| e == "d"),
    )
    .await;
}

#[tokio::test]
async fn stream_delivers_every_event_after_the_snapshot_in_order() {
    let (app, base) = serve().await;
    append(&app.log, &["before"]);
    let (sub, index) = take_snapshot(&base, None).await;
    assert_eq!(index, 1);

    let mut reader = SseReader::open(&base, &sub, None).await;
    append(&app.log, &["a", "b", "c"]);

    assert_eq!(
        reader.next_events(3).await,
        vec![(2, "a".into()), (3, "b".into()), (4, "c".into())],
    );
}

#[tokio::test]
async fn reconnect_resumes_right_after_the_clients_last_event_id() {
    let (app, base) = serve().await;
    let (sub, _) = take_snapshot(&base, None).await;
    lose_c_and_d_in_a_closed_connection(&app, &base, &sub).await;

    let mut reader = SseReader::open(&base, &sub, Some("2")).await;
    append(&app.log, &["e"]);

    assert_eq!(
        reader.next_events(3).await,
        vec![(3, "c".into()), (4, "d".into()), (5, "e".into())],
    );
}

#[tokio::test]
async fn last_event_id_query_parameter_resumes_like_the_header() {
    let (app, base) = serve().await;
    let (sub, _) = take_snapshot(&base, None).await;
    lose_c_and_d_in_a_closed_connection(&app, &base, &sub).await;

    let mut reader = SseReader::from_response(get_stream_with(&base, &sub, None, Some("2")).await);

    assert_eq!(
        reader.next_events(2).await,
        vec![(3, "c".into()), (4, "d".into())],
    );
}

#[tokio::test]
async fn last_event_id_header_wins_over_the_query_parameter() {
    let (app, base) = serve().await;
    let (sub, _) = take_snapshot(&base, None).await;
    lose_c_and_d_in_a_closed_connection(&app, &base, &sub).await;

    let mut reader =
        SseReader::from_response(get_stream_with(&base, &sub, Some("3"), Some("1")).await);

    assert_eq!(reader.next_event().await, (4, "d".into()));
}

#[tokio::test]
async fn last_event_id_older_than_the_snapshot_gets_409_needs_snapshot() {
    let (app, base) = serve().await;
    let (sub, _) = take_snapshot(&base, None).await;
    let mut reader = SseReader::open(&base, &sub, None).await;
    append(&app.log, &["a", "b"]);
    reader.next_events(2).await;
    drop(reader);

    // Re-snapshot (same sub_id) at index 3. A client still at 2 never saw
    // that snapshot: it must re-take it rather than silently skip "c".
    append(&app.log, &["c"]);
    let (_, index) = take_snapshot(&base, Some(&sub)).await;
    assert_eq!(index, 3);

    let res = get_stream(&base, &sub, Some("2")).await;

    assert_eq!(res.status(), 409);
    let mut reader = SseReader::open(&base, &sub, Some("3")).await;
    append(&app.log, &["d"]);
    assert_eq!(reader.next_event().await, (4, "d".into()));
}

#[tokio::test]
async fn last_event_id_no_longer_retained_gets_409_needs_snapshot() {
    // max_events = 2: the events after the client's last id are evicted.
    let (app, base) = serve_with(EventLog::new(TTL, 2, 64)).await;
    let (sub, _) = take_snapshot(&base, None).await;
    lose_c_and_d_in_a_closed_connection(&app, &base, &sub).await;
    append(&app.log, &["e", "f"]);

    let res = get_stream(&base, &sub, Some("2")).await;

    assert_eq!(res.status(), 409);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["error"], "needs_snapshot");
}

#[tokio::test]
async fn malformed_last_event_id_gets_400() {
    let (_app, base) = serve().await;
    let (sub, _) = take_snapshot(&base, None).await;

    let header = get_stream_with(&base, &sub, Some("not-a-number"), None).await;
    let query = get_stream_with(&base, &sub, None, Some("not-a-number")).await;

    assert_eq!(
        (header.status().as_u16(), query.status().as_u16()),
        (400, 400)
    );
}

#[tokio::test]
async fn unknown_sub_id_gets_409_needs_snapshot() {
    let (_app, base) = serve().await;

    let res = get_stream(&base, "never-registered", None).await;

    assert_eq!(res.status(), 409);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(
        body,
        serde_json::json!({
            "error": "needs_snapshot",
            "sub": "never-registered",
            "hint": "resync via GET /things",
        })
    );
}
