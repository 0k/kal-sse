//! Run: `cargo run --example mux --features axum` (listens on localhost:3000).
//!
//! Reserve (copy the returned conn into CONN):
//! ```sh
//! curl -s localhost:3000/mux -H 'content-type: application/json' \
//!   -d '{"channels":["tick","room:lobby"]}'
//! CONN='<returned conn>'
//! curl -N "localhost:3000/mux/stream?conn=$CONN"
//! # In another terminal, using the same CONN:
//! curl localhost:3000/mux/$CONN/channels -H 'content-type: application/json' \
//!   -d '{"add":["room:kitchen"],"remove":["room:lobby"]}'
//! # Reconnect with the last applied generation and cursor for each channel:
//! curl -NG localhost:3000/mux/stream --data-urlencode "conn=$CONN" \
//!   --data-urlencode 'resume={"tick":[1,3]}'
//! curl -X DELETE "localhost:3000/mux/$CONN"
//! ```

#[cfg(feature = "axum")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use kal_sse::mux::{Channel, Mux, MuxConfig, MuxSource};
    use kal_sse::EventLog;

    fn counter() -> Arc<Channel<u64>> {
        Arc::new(Channel::new(
            0,
            EventLog::new(Duration::from_secs(900), 1024, 16),
            |value| Ok(vec![serde_json::json!({"value": value}).to_string()]),
        ))
    }

    let tick = counter();
    let tick_source = tick.clone();
    let rooms = Arc::new(Mutex::new(HashMap::<String, Arc<Channel<u64>>>::new()));
    let room_sources = rooms.clone();
    let mux = Mux::new(MuxConfig::default())
        .family("tick", move |param: Option<&str>| {
            if param.is_some() {
                return Err("tick does not accept a parameter".into());
            }
            Ok(tick_source.clone() as Arc<dyn MuxSource>)
        })?
        .family("room", move |param: Option<&str>| {
            let name = param.ok_or("room requires a name")?;
            let mut rooms = room_sources.lock().expect("rooms mutex poisoned");
            Ok(rooms.entry(name.into()).or_insert_with(counter).clone() as Arc<dyn MuxSource>)
        })?;
    let maintenance = mux.clone();
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let appender = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = stopped.changed() => break,
                _ = interval.tick() => {
                    let now = Instant::now();
                    let update = |value: &mut u64, log: &mut EventLog<String>| {
                        *value += 1;
                        log.append(serde_json::json!({"value": value}).to_string());
                        log.compact(now);
                    };
                    tick.update(update);
                    for room in rooms.lock().expect("rooms mutex poisoned").values() { room.update(update); }
                    maintenance.compact(now);
                }
            }
        }
    });
    let router = axum::Router::new().nest("/mux", kal_sse::mux_axum::router(mux));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    let result = axum::serve(listener, router).await;
    let _ = stop.send(true);
    appender.await?;
    result?;
    Ok(())
}

#[cfg(not(feature = "axum"))]
fn main() {
    println!("Run with --features axum to enable the multiplexed HTTP example.");
}
