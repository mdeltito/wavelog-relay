//! Periodic poll → push loop.
//!
//! Each tick reads a [`RigState`] via [`RigHandle::poll`] and forwards
//! it to both the WS bandmap (every tick, no dedupe) and Wavelog's
//! `/api/radio` endpoint (dedupe + heartbeat + retry policy applied).
//! The asymmetry is deliberate: WS frames are cheap and the bandmap UI
//! wants live updates while the VFO turns; the HTTP dedupe exists to
//! spare Wavelog DB writes.
//!
//! Dedupe state lives **here**, not on [`WavelogClient`]. That's the
//! point: dedupe is a poller strategy (quantize, compare, skip,
//! heartbeat every 30 s) — it's not a property of the HTTP client.
//! Keeping it in this module lets the client stay stateless and
//! shareable between the poller, the WSJT-X listener, and the
//! `stations` subcommand.
//!
//! Per-tick errors are logged at WARN and the loop continues — only a
//! shutdown signal exits.
//!
//! [`RigState`]: crate::rigctld::RigState

use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior, interval};

use crate::rigctld::{RigHandle, RigState};
use crate::wavelog::{WavelogClient, WavelogError};
use crate::ws::WsBandmapHandle;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Run the poll → push loop until `shutdown` resolves to `true` or the
/// watch sender is dropped. Per-tick errors are logged and skipped.
///
/// The `radio` name and `power_max_watts` are baked in here because
/// they're constants for the bridge's lifetime; pulling them out of
/// the client lets a single `WavelogClient` serve both this poller
/// and the WSJT-X listener.
pub async fn run(
    rig: RigHandle,
    client: WavelogClient,
    radio: Box<str>,
    power_max_watts: f32,
    ws_bandmap: WsBandmapHandle,
    tick_interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    // Mark the current value as seen so `changed()` only fires on
    // subsequent updates. If the sender already flipped to `true`
    // before we got here, exit straight away.
    if *shutdown.borrow_and_update() {
        return;
    }

    let mut deduper = Deduper::default();
    let mut ticker = interval(tick_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tracing::info!(?tick_interval, "poller started");
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                tick(&rig, &client, &radio, power_max_watts, &mut deduper, &ws_bandmap).await;
            },
            result = shutdown.changed() => {
                let should_stop = result.is_err() || *shutdown.borrow();
                if should_stop {
                    tracing::info!("poller shutting down");
                    return;
                }
            }
        }
    }
}

async fn tick(
    rig: &RigHandle,
    client: &WavelogClient,
    radio: &str,
    power_max_watts: f32,
    deduper: &mut Deduper,
    ws_bandmap: &WsBandmapHandle,
) {
    let state = match rig.poll().await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "rig poll failed");
            return;
        },
    };
    // Broadcast before the wavelog POST — the POST awaits a network
    // round-trip, but WS subscribers should see fresh state on every
    // tick regardless of whether wavelog is reachable.
    ws_bandmap.broadcast(&state);

    let now = Instant::now();
    if deduper.should_skip(&state, now) {
        tracing::debug!(freq = state.freq, mode = %state.mode, "wavelog push deduped");
        return;
    }
    match client.push_radio(radio, &state, power_max_watts).await {
        Ok(()) => deduper.record(&state, now),
        Err(e) => log_push_failure(&e),
    }
}

fn log_push_failure(err: &WavelogError) {
    tracing::warn!(error = %err, "wavelog push failed");
}

/// Per-poller dedupe state. Skips a push when frequency, mode, and a
/// quantized RFPOWER reading haven't changed and the last successful
/// POST was within `HEARTBEAT_INTERVAL` ago. State updates only after
/// a successful push — a transient Wavelog outage during a real QSY
/// must not silently swallow the QSY once Wavelog comes back.
#[derive(Default)]
struct Deduper {
    last: Option<DedupeKey>,
    last_at: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DedupeKey {
    freq: u64,
    mode: Box<str>,
    rfpower_q: i32,
}

impl Deduper {
    fn should_skip(&self, state: &RigState, now: Instant) -> bool {
        let (Some(last), Some(last_at)) = (&self.last, self.last_at) else {
            return false;
        };
        last.freq == state.freq
            && *last.mode == *state.mode
            && last.rfpower_q == quantize_rfpower(state.power)
            && now.duration_since(last_at) < HEARTBEAT_INTERVAL
    }

    fn record(&mut self, state: &RigState, now: Instant) {
        self.last = Some(DedupeKey {
            freq: state.freq,
            mode: state.mode.clone(),
            rfpower_q: quantize_rfpower(state.power),
        });
        self.last_at = Some(now);
    }
}

/// Quantize an RFPOWER reading (`0.0..=1.0`) into half-percent bins of
/// full scale. The dedupe key compares on this quantized value so a
/// continuously-drifting RFPOWER doesn't generate one POST per tick;
/// keeping the bin relative (rather than fixed at 0.5 W) means a
/// low-`power_max` rig still benefits from dedupe.
fn quantize_rfpower(rfpower: f32) -> i32 {
    (rfpower * 200.0).round() as i32
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
    use tokio::net::TcpListener;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::rigctld;

    /// Spin up a long-lived TCP server that mimics rigctld's reply
    /// shape for any number of connections and commands. Used as a
    /// stand-in for the real daemon during poller tests.
    async fn spawn_persistent_rigctld() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut stream = BufStream::new(stream);
                    let mut line = String::new();
                    loop {
                        line.clear();
                        let Ok(n) = stream.read_line(&mut line).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        let reply: &[u8] = match line.trim_end_matches(['\r', '\n']) {
                            "f" => b"14074000\n",
                            "m" => b"USB\n2400\n",
                            "\\get_level RFPOWER" => b"0.1\n",
                            cmd if cmd.starts_with("F ") || cmd.starts_with("M ") => b"RPRT 0\n",
                            _ => b"RPRT -11\n",
                        };
                        if stream.write_all(reply).await.is_err() {
                            return;
                        }
                        if stream.flush().await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    fn dummy_rig_handle() -> RigHandle {
        // Port 1 is reserved (RFC 1700 "tcpmux") and won't have a real
        // listener locally — the actor enters backoff but the handle
        // stays valid. Good enough for tests that don't actually tick.
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let (handle, _join) = rigctld::spawn(addr, Duration::from_secs(3));
        handle
    }

    fn dummy_wavelog_client() -> WavelogClient {
        WavelogClient::new("http://127.0.0.1:1", "k").unwrap()
    }

    fn dummy_ws_handle() -> WsBandmapHandle {
        WsBandmapHandle::new("R".into(), 100.0)
    }

    fn rig_state(freq: u64, mode: &str, power: f32) -> RigState {
        RigState {
            freq,
            mode: mode.into(),
            power,
        }
    }

    // -- Deduper unit tests --

    #[test]
    fn quantize_rfpower_rounds_to_half_percent_bins() {
        assert_eq!(quantize_rfpower(0.0), 0);
        assert_eq!(quantize_rfpower(0.10), 20);
        assert_eq!(quantize_rfpower(0.102), 20);
        assert_eq!(quantize_rfpower(0.103), 21);
        assert_eq!(quantize_rfpower(0.998), 200);
        assert_eq!(quantize_rfpower(1.0), 200);
    }

    #[test]
    fn deduper_initial_state_never_skips() {
        let deduper = Deduper::default();
        let now = Instant::now();
        assert!(!deduper.should_skip(&rig_state(14_074_000, "USB", 0.1), now));
    }

    #[test]
    fn deduper_skips_identical_state_within_heartbeat() {
        let mut deduper = Deduper::default();
        let now = Instant::now();
        let state = rig_state(14_074_000, "USB", 0.1);
        deduper.record(&state, now);
        assert!(deduper.should_skip(&state, now + Duration::from_secs(10)));
    }

    #[test]
    fn deduper_re_pushes_after_heartbeat_interval() {
        let mut deduper = Deduper::default();
        let now = Instant::now();
        let state = rig_state(14_074_000, "USB", 0.1);
        deduper.record(&state, now);
        assert!(!deduper.should_skip(&state, now + HEARTBEAT_INTERVAL + Duration::from_millis(1)));
    }

    #[test]
    fn deduper_breaks_on_freq_change() {
        let mut deduper = Deduper::default();
        let now = Instant::now();
        deduper.record(&rig_state(14_074_000, "USB", 0.1), now);
        assert!(!deduper.should_skip(&rig_state(14_100_000, "USB", 0.1), now));
    }

    #[test]
    fn deduper_breaks_on_mode_change() {
        let mut deduper = Deduper::default();
        let now = Instant::now();
        deduper.record(&rig_state(14_074_000, "USB", 0.1), now);
        assert!(!deduper.should_skip(&rig_state(14_074_000, "CW", 0.1), now));
    }

    #[test]
    fn deduper_breaks_on_quantized_power_change() {
        let mut deduper = Deduper::default();
        let now = Instant::now();
        // 0.10 -> bin 20, 0.15 -> bin 30: crosses several bins.
        deduper.record(&rig_state(14_074_000, "USB", 0.10), now);
        assert!(!deduper.should_skip(&rig_state(14_074_000, "USB", 0.15), now));
    }

    #[test]
    fn deduper_collapses_subquantum_power_drift() {
        let mut deduper = Deduper::default();
        let now = Instant::now();
        // 0.100 -> bin 20, 0.101 -> 20.2 -> bin 20: same bin, dedupes.
        deduper.record(&rig_state(14_074_000, "USB", 0.100), now);
        assert!(deduper.should_skip(&rig_state(14_074_000, "USB", 0.101), now));
    }

    #[test]
    fn deduper_quantum_independent_of_power_max() {
        // The bin width is in *fraction-of-full-scale* terms, so a
        // QRP rig with `--power-max 5` gets the same dedupe behaviour
        // as a 100 W rig — the deduper doesn't even see power_max.
        // 0.5 -> bin 100 regardless of rig.
        let now = Instant::now();
        let state = rig_state(14_074_000, "USB", 0.5);
        let mut deduper = Deduper::default();
        deduper.record(&state, now);
        assert!(deduper.should_skip(&state, now));
    }

    // -- Poller loop tests --

    #[tokio::test]
    async fn shutdown_signal_set_to_true_stops_loop_promptly() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let poller = tokio::spawn(run(
            dummy_rig_handle(),
            dummy_wavelog_client(),
            "R".into(),
            100.0,
            dummy_ws_handle(),
            Duration::from_secs(60),
            shutdown_rx,
        ));

        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(Duration::from_millis(500), poller)
            .await
            .expect("poller did not exit within 500ms")
            .expect("poller task panicked");
    }

    #[tokio::test]
    async fn dropping_shutdown_sender_stops_loop() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let poller = tokio::spawn(run(
            dummy_rig_handle(),
            dummy_wavelog_client(),
            "R".into(),
            100.0,
            dummy_ws_handle(),
            Duration::from_secs(60),
            shutdown_rx,
        ));

        drop(shutdown_tx);

        tokio::time::timeout(Duration::from_millis(500), poller)
            .await
            .expect("poller did not exit within 500ms")
            .expect("poller task panicked");
    }

    #[tokio::test]
    async fn tick_drives_rig_poll_and_wavelog_push() {
        let rig_addr = spawn_persistent_rigctld().await;
        let (rig, _rig_join) = rigctld::spawn(rig_addr, Duration::from_secs(3));

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/radio"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let poller = tokio::spawn(run(
            rig,
            client,
            "FT-710".into(),
            100.0,
            dummy_ws_handle(),
            Duration::from_millis(50),
            shutdown_rx,
        ));

        // Real-time wait: ~4 ticks, but dedupe collapses them to 1 POST.
        tokio::time::sleep(Duration::from_millis(250)).await;

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), poller)
            .await
            .expect("poller did not exit within 1s")
            .expect("poller task panicked");

        let requests = server.received_requests().await.unwrap();
        assert!(!requests.is_empty(), "expected at least one POST");
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["frequency"], 14_074_000);
        assert_eq!(body["mode"], "USB");
        assert!((body["power"].as_f64().unwrap() - 10.0).abs() < 1e-3);
    }

    #[tokio::test]
    async fn poller_dedupes_repeated_state() {
        // Verifies the integration: poller calls Deduper *and*
        // client.push_radio, and repeat states are collapsed to one
        // POST on the wire — the property that gives the dedupe its
        // raison d'être.
        let rig_addr = spawn_persistent_rigctld().await;
        let (rig, _rig_join) = rigctld::spawn(rig_addr, Duration::from_secs(3));

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/radio"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let poller = tokio::spawn(run(
            rig,
            client,
            "FT-710".into(),
            100.0,
            dummy_ws_handle(),
            Duration::from_millis(10),
            shutdown_rx,
        ));

        // Tick rapidly for half a second. Rig state is constant from
        // the mock; expect dedupe to collapse to 1 POST.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(1), poller).await;

        let count = server.received_requests().await.unwrap().len();
        assert_eq!(
            count, 1,
            "expected dedupe to collapse repeated state to 1 POST, got {count}",
        );
    }

    #[tokio::test]
    async fn poller_re_pushes_after_failed_push() {
        // Regression: dedupe state must update only on success. With
        // the first push failing (3 attempts, all 500), the next
        // identical-state tick must POST again, not be silently
        // deduped.
        let rig_addr = spawn_persistent_rigctld().await;
        let (rig, _rig_join) = rigctld::spawn(rig_addr, Duration::from_secs(3));

        let server = MockServer::start().await;
        // First three POSTs (one full retry exhaustion) all 500.
        Mock::given(method("POST"))
            .and(path("/api/radio"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(3)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/radio"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let poller = tokio::spawn(run(
            rig,
            client,
            "FT-710".into(),
            100.0,
            dummy_ws_handle(),
            Duration::from_millis(100),
            shutdown_rx,
        ));

        // Run long enough for the failing push (which sleeps 0, 1, 4 s
        // between retries) plus a follow-up tick on the 200-mock.
        tokio::time::sleep(Duration::from_secs(7)).await;

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), poller).await;

        let n = server.received_requests().await.unwrap().len();
        assert!(
            n >= 4,
            "expected ≥4 POSTs (3 failing + ≥1 retry-after-success), got {n}",
        );
    }

    #[tokio::test]
    async fn rig_errors_do_not_kill_the_loop() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let poller = tokio::spawn(run(
            dummy_rig_handle(),
            dummy_wavelog_client(),
            "R".into(),
            100.0,
            dummy_ws_handle(),
            Duration::from_millis(20),
            shutdown_rx,
        ));

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!poller.is_finished(), "poller exited on rig errors");

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), poller)
            .await
            .unwrap()
            .unwrap();
    }
}
