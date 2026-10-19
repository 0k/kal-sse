//! JSON/HTTP adapter for [`crate::mux`]. Mount with
//! `Router::new().nest("/mux", mux_axum::router(mux))`.
//!
//! Routes relative to that mount are `POST /`, `GET /stream?conn=...`,
//! `POST /{conn}/channels`, and `DELETE /{conn}`. With axum 0.8 nesting, the
//! reservation URL is the mount path itself (`/mux`, no trailing slash).
//! Protect the router with the embedding application's authentication: a
//! connection token grants subscription access.
//! There is deliberately no SSE `id:` field; resume hints are per channel.

use std::collections::HashMap;
use std::convert::Infallible;

use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::mux::{Mux, MuxError, MuxFrame, ResumeHints};

/// Four endpoints with caller-selected mount point; see module documentation.
pub fn router(mux: Mux) -> Router {
    Router::new()
        .route("/", post(reserve))
        .route("/stream", get(stream))
        .route("/{conn}/channels", post(channels))
        .route("/{conn}", delete(release))
        .with_state(mux)
}

fn bad_request(reason: impl ToString) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "invalid_request", "reason": reason.to_string() })),
    )
        .into_response()
}

fn refusal(error: MuxError) -> Response {
    let (status, body) = match error {
        MuxError::InvalidChannel { channel, reason } => (
            StatusCode::BAD_REQUEST,
            json!({"error": "invalid_channel", "channel": channel, "reason": reason}),
        ),
        MuxError::NeedsReservation { conn, reason } => (
            StatusCode::CONFLICT,
            json!({"error": "needs_reservation", "conn": conn, "reason": reason}),
        ),
        MuxError::InvalidChange { reason } => (
            StatusCode::BAD_REQUEST,
            json!({"error": "invalid_change", "reason": reason}),
        ),
        MuxError::Busy { reason } => (
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error": "control_queue_full", "reason": reason}),
        ),
    };
    (status, Json(body)).into_response()
}

fn strings(body: &Value, field: &str, optional: bool) -> Result<Vec<String>, String> {
    if !body.is_object() {
        return Err("expected a JSON object".into());
    }
    if optional && body.get(field).is_none() {
        return Ok(Vec::new());
    }
    body.get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{field} must be an array of channel keys"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("{field} must contain only strings"))
        })
        .collect()
}

async fn reserve(State(mux): State<Mux>, body: Result<Json<Value>, JsonRejection>) -> Response {
    let body = match body {
        Ok(Json(body)) => body,
        Err(error) => return bad_request(error),
    };
    let keys = match strings(&body, "channels", false) {
        Ok(keys) => keys,
        Err(error) => return bad_request(error),
    };
    match mux.reserve(keys) {
        Ok(reservation) => (StatusCode::CREATED, Json(json!({"conn": reservation.conn, "gens": reservation.gens, "heartbeat_ms": reservation.heartbeat_ms}))).into_response(),
        Err(error) => refusal(error),
    }
}

async fn channels(
    State(mux): State<Mux>,
    Path(conn): Path<String>,
    body: Result<Json<Value>, JsonRejection>,
) -> Response {
    let body = match body {
        Ok(Json(body)) => body,
        Err(error) => return bad_request(error),
    };
    let add = match strings(&body, "add", true) {
        Ok(keys) => keys,
        Err(error) => return bad_request(error),
    };
    let remove = match strings(&body, "remove", true) {
        Ok(keys) => keys,
        Err(error) => return bad_request(error),
    };
    match mux.change(&conn, add, remove) {
        Ok(gens) => Json(json!({"gens": gens})).into_response(),
        Err(error) => refusal(error),
    }
}

async fn release(State(mux): State<Mux>, Path(conn): Path<String>) -> StatusCode {
    mux.release(&conn);
    StatusCode::NO_CONTENT
}

async fn stream(
    State(mux): State<Mux>,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> Response {
    let query = match query {
        Ok(Query(query)) => query,
        Err(error) => return bad_request(error),
    };
    let Some(conn) = query.get("conn") else {
        return bad_request("conn query parameter is required");
    };
    let hints = match query.get("resume") {
        Some(hints) => match serde_json::from_str::<ResumeHints>(hints) {
            Ok(hints) => hints,
            Err(error) => return bad_request(format!("invalid resume hints: {error}")),
        },
        None => ResumeHints::new(),
    };
    let mut source = match mux.stream(conn, hints) {
        Ok(stream) => stream,
        Err(error) => return refusal(error),
    };
    // Capture the stream outside the generator: an unpolled response still
    // owns its disconnect guard. No await or queue between next() and yield.
    let frames = async_stream::stream! {
        while let Some(frame) = source.next().await {
            yield Ok::<Event, Infallible>(render(frame));
        }
    };
    Sse::new(frames)
        .keep_alive(
            KeepAlive::new()
                .interval(mux.config().heartbeat)
                .text("ping"),
        )
        .into_response()
}

fn quoted(value: &str) -> String {
    serde_json::to_string(value).expect("string serialization cannot fail")
}

fn render(frame: MuxFrame) -> Event {
    let (name, data) = match frame {
        MuxFrame::Hello { conn, heartbeat_ms } => ("hello", json!({"conn": conn, "heartbeat_ms": heartbeat_ms}).to_string()),
        MuxFrame::Snapshot { ch, r#gen: generation, cursor, part, last, data } => ("snapshot",
            format!("{{\"ch\":{},\"gen\":{generation},\"cursor\":{cursor},\"part\":{part},\"last\":{last},\"data\":{data}}}", quoted(&ch))),
        MuxFrame::Delta { ch, r#gen: generation, cursor, data } => ("delta",
            format!("{{\"ch\":{},\"gen\":{generation},\"cursor\":{cursor},\"data\":{data}}}", quoted(&ch))),
        MuxFrame::Removed { ch, r#gen: generation } => ("removed", json!({"ch": ch, "gen": generation}).to_string()),
        MuxFrame::Error { ch, r#gen: generation, reason } => ("channel_error", json!({"ch": ch, "gen": generation, "reason": reason}).to_string()),
    };
    Event::default().event(name).data(data)
}
