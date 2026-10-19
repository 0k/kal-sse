use std::sync::Arc;
use std::time::Duration;

use kal_sse::mux::{Channel, Mux, MuxConfig, MuxSource};
use kal_sse::EventLog;

pub const TTL: Duration = Duration::from_secs(60);
pub const STEP: Duration = Duration::from_secs(2);

pub fn channel(cap: usize, broadcast: usize, parts: usize) -> Arc<Channel<u64>> {
    Arc::new(Channel::new(
        0,
        EventLog::new(TTL, cap, broadcast),
        move |state| Ok(vec![state.to_string(); parts]),
    ))
}

pub fn append(channel: &Channel<u64>, count: u64) {
    channel.update(|state, log| {
        for _ in 0..count {
            *state += 1;
            log.append(state.to_string());
        }
    });
}

pub fn mux(channels: &[(&str, Arc<Channel<u64>>)]) -> Mux {
    let mut mux = Mux::new(MuxConfig {
        ttl: TTL,
        heartbeat: Duration::from_millis(20),
        ..MuxConfig::default()
    });
    for (key, source) in channels {
        let source = source.clone();
        mux = mux
            .family(key, move |param: Option<&str>| {
                if param == Some("denied") {
                    Err("unknown room".into())
                } else {
                    Ok(source.clone() as Arc<dyn MuxSource>)
                }
            })
            .unwrap();
    }
    mux
}
