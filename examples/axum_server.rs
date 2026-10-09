//! The `README` server example: a snapshot endpoint and its SSE stream.
//!
//! Run with `cargo run --example axum_server --features axum`, then
//! `curl localhost:3000/api/things` and
//! `curl -N 'localhost:3000/api/things/stream?sub=<sub>'`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use kal_sse::axum::{last_event_id, sse_response_from, SNAPSHOT_INDEX_HEADER};
use kal_sse::core::EventLog;
use serde::Deserialize;
use tokio::time;

#[derive(Clone)]
struct AppState {
    log: Arc<Mutex<EventLog<String>>>,
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

async fn snapshot(
    State(state): State<AppState>,
    Query(q): Query<SnapshotQuery>,
) -> impl IntoResponse {
    let (sub_id, cursor) = state.log.lock().unwrap().register(q.sub);
    let mut headers = HeaderMap::new();
    headers.insert(SNAPSHOT_INDEX_HEADER, cursor.to_string().parse().unwrap());
    // Return headers + snapshot body (abbreviated here).
    (headers, format!("{{\"sub\":\"{sub_id}\"}}"))
}

async fn stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<StreamQuery>,
) -> Result<impl IntoResponse, (axum::http::StatusCode, Json<serde_json::Value>)> {
    let last_event_id = last_event_id(&headers, q.last_event_id.as_deref())?;
    sse_response_from(
        state.log.clone(),
        q.sub,
        last_event_id,
        |e: &String| e.clone(),
        "resync via GET /api/things",
    )
}

#[tokio::main]
async fn main() {
    let log = Arc::new(Mutex::new(EventLog::<String>::new(
        Duration::from_secs(900), // TTL of a handle with no attached stream
        10_000,                   // max retained events
        256,                      // broadcast channel capacity
    )));

    // Periodic compaction: reclaim expired handles, trim the buffer.
    let compact_log = log.clone();
    tokio::spawn(async move {
        let mut tick = time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            compact_log
                .lock()
                .unwrap()
                .compact(std::time::Instant::now());
        }
    });

    // Appender (example: append from some external source).
    let append_log = log.clone();
    tokio::spawn(async move {
        let mut tick = time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            append_log.lock().unwrap().append("hello".to_string());
        }
    });

    let state = AppState { log };
    let app = Router::new()
        .route("/api/things", get(snapshot))
        .route("/api/things/stream", get(stream))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
