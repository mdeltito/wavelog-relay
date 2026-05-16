//! Periodic poll → push loop.
//!
//! Two cooperating tasks coordinated by a `watch::channel`:
//!
//! - **Tick loop** (`run`): reads a [`RigState`] via [`RigHandle::poll`]
//!   each tick, hands it to [`WsBandmapHandle::broadcast`] synchronously
//!   (cheap, fan-out is local), and publishes it on the watch channel.
//!   The tick loop never awaits a network POST.
//! - **Radio worker** ([`radio_worker`]): observes `changed()` on the
//!   watch channel, runs the deduper, and POSTs to `/api/radio` with the
//!   shared retry policy. The watch's latest-only semantics drop
//!   intermediate samples while a slow POST is in flight — exactly
//!   right for the "WS bandmap stays live; HTTP can lag" asymmetry.
//!
//! Splitting the two means a multi-second Wavelog stall (5s timeout × 3
//! retries on outage) doesn't starve the WS bandmap or block shutdown
//! observation.
//!
//! Dedupe state lives in the worker, not on [`WavelogClient`] — dedupe
//! is a poller strategy (quantize, compare, skip, heartbeat every 30 s),
//! not a property of the HTTP client. Keeping it here lets one client
//! serve both the poller and the WSJT-X listener.
//!
//! Per-tick errors are logged at WARN and the loop continues — only a
//! shutdown signal exits.
//!
//! [`RigState`]: crate::rigctld::RigState

use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior, interval};

use crate::rigctld::{RigHandle, RigState};
use crate::wavelog::WavelogClient;
use crate::ws::WsBandmapHandle;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Run the poll → push loop until `shutdown` resolves to `true` or the
/// watch sender is dropped. Per-tick errors are logged and skipped.
///
/// Spawns one background worker task for the wavelog POST path; this
/// function owns the tick loop. Returns once shutdown is observed and
/// the worker has joined.
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
    if *shutdown.borrow_and_update() {
        return;
    }

    // None means "no snapshot yet"; the worker treats this as a no-op.
    // Watch's "latest only" semantics intentionally drop intermediate
    // states while a slow POST is in flight.
    let (state_tx, state_rx) = watch::channel::<Option<RigState>>(None);
    let worker = tokio::spawn(radio_worker(
        state_rx,
        client,
        radio,
        power_max_watts,
        shutdown.clone(),
    ));

    let mut ticker = interval(tick_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tracing::info!(?tick_interval, "poller started");
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                match rig.poll().await {
                    Ok(state) => {
                        ws_bandmap.broadcast(&state);
                        // send only errors when every receiver has been
                        // dropped, which only happens after this loop
                        // exits and the worker join completes.
                        let _ = state_tx.send(Some(state));
                    }
                    Err(e) => tracing::warn!(error = %e, "rig poll failed"),
                }
            }
            result = shutdown.changed() => {
                let should_stop = result.is_err() || *shutdown.borrow();
                if should_stop {
                    tracing::info!("poller shutting down");
                    break;
                }
            }
        }
    }

    // Drop the state sender so the worker's `changed()` returns Err
    // and it exits cleanly even if shutdown didn't fire (e.g. tests
    // that drop the watch sender to terminate).
    drop(state_tx);
    if let Err(e) = worker.await {
        tracing::warn!(error = %e, "radio worker task did not exit cleanly");
    }
}

/// Drain the `watch` of latest [`RigState`] snapshots, run the deduper,
/// and POST to Wavelog. Cancellation-aware: if `shutdown` flips during
/// an in-flight POST, the task returns without waiting for the retry
/// schedule (`[0,1,4]s` × 5s timeout) to exhaust.
async fn radio_worker(
    mut state_rx: watch::Receiver<Option<RigState>>,
    client: WavelogClient,
    radio: Box<str>,
    power_max_watts: f32,
    mut shutdown: watch::Receiver<bool>,
) {
    if *shutdown.borrow_and_update() {
        return;
    }
    let mut deduper = Deduper::default();
    loop {
        tokio::select! {
            res = state_rx.changed() => {
                if res.is_err() {
                    tracing::info!("radio worker: state channel closed");
                    return;
                }
                let snapshot = state_rx.borrow_and_update().clone();
                let Some(state) = snapshot else { continue };
                let now = Instant::now();
                if deduper.should_skip(&state, now) {
                    tracing::debug!(
                        freq = state.freq,
                        mode = %state.mode,
                        "wavelog push deduped",
                    );
                    continue;
                }
                tokio::select! {
                    push_res = client.push_radio(&radio, &state, power_max_watts) => {
                        match push_res {
                            Ok(()) => deduper.record(&state, now),
                            Err(e) => tracing::warn!(error = %e, "wavelog push failed"),
                        }
                    }
                    res = shutdown.changed() => {
                        let should_stop = res.is_err() || *shutdown.borrow();
                        if should_stop {
                            tracing::info!("radio worker shutting down (mid-push)");
                            return;
                        }
                    }
                }
            }
            result = shutdown.changed() => {
                let should_stop = result.is_err() || *shutdown.borrow();
                if should_stop {
                    tracing::info!("radio worker shutting down");
                    return;
                }
            }
        }
    }
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
    /// `None` when the rig backend doesn't expose RFPOWER. Compared as
    /// part of the key so a rig that suddenly starts/stops reporting
    /// power crosses the dedupe boundary and re-pushes.
    rfpower_q: Option<i32>,
}

impl Deduper {
    fn should_skip(&self, state: &RigState, now: Instant) -> bool {
        let (Some(last), Some(last_at)) = (&self.last, self.last_at) else {
            return false;
        };
        last.freq == state.freq
            && *last.mode == *state.mode
            && last.rfpower_q == state.power.map(quantize_rfpower)
            && now.duration_since(last_at) < HEARTBEAT_INTERVAL
    }

    fn record(&mut self, state: &RigState, now: Instant) {
        self.last = Some(DedupeKey {
            freq: state.freq,
            mode: state.mode.clone(),
            rfpower_q: state.power.map(quantize_rfpower),
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
            power: Some(power),
        }
    }

    fn rig_state_no_power(freq: u64, mode: &str) -> RigState {
        RigState {
            freq,
            mode: mode.into(),
            power: None,
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
    fn deduper_treats_none_power_as_distinct_from_some() {
        // A rig that flips between reporting power and not (e.g. a
        // hamlib backend with intermittent RFPOWER support) should
        // not silently dedupe the transition.
        let now = Instant::now();
        let mut deduper = Deduper::default();
        deduper.record(&rig_state(14_074_000, "USB", 0.10), now);
        assert!(!deduper.should_skip(&rig_state_no_power(14_074_000, "USB"), now));
    }

    #[test]
    fn deduper_skips_repeated_none_power() {
        // Steady-state on a RFPOWER-less rig: still want dedupe.
        let now = Instant::now();
        let mut deduper = Deduper::default();
        let state = rig_state_no_power(14_074_000, "USB");
        deduper.record(&state, now);
        assert!(deduper.should_skip(&state, now + Duration::from_secs(10)));
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

    #[tokio::test]
    async fn ws_broadcast_keeps_running_during_slow_wavelog_post() {
        // The whole point of the off-tick worker: a multi-second
        // /api/radio stall must not freeze the WS bandmap. We assert
        // that the rig is polled (and therefore the WS handle's
        // broadcast is invoked) repeatedly while the wavelog mock
        // holds every POST for several seconds.
        let rig_addr = spawn_persistent_rigctld().await;
        let (rig, _rig_join) = rigctld::spawn(rig_addr, Duration::from_secs(3));

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/radio"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(3)))
            .mount(&server)
            .await;
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();

        let counter_handle = CountingWsHandle::new();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let poller = tokio::spawn(run(
            rig,
            client,
            "FT-710".into(),
            100.0,
            counter_handle.bandmap_handle(),
            Duration::from_millis(50),
            shutdown_rx,
        ));

        // Run for ~750ms — well under the 3s POST stall but long
        // enough to expect ~15 ticks. A pre-refactor poller would
        // have produced exactly one broadcast (then blocked on the
        // POST); the worker split lets the tick loop keep polling.
        tokio::time::sleep(Duration::from_millis(750)).await;
        let observed = counter_handle.count();
        assert!(
            observed >= 5,
            "expected ws broadcasts to keep firing during slow POST; got {observed}",
        );

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), poller).await;
    }

    #[tokio::test]
    async fn shutdown_during_inflight_post_returns_promptly() {
        // Wavelog mock sleeps forever on POST. With cancellation-aware
        // shutdown the whole poller (tick loop + worker) must exit
        // well before the 5s reqwest timeout × 3 retries.
        let rig_addr = spawn_persistent_rigctld().await;
        let (rig, _rig_join) = rigctld::spawn(rig_addr, Duration::from_secs(3));

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/radio"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(60)))
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

        // Let one POST get into flight.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let _ = shutdown_tx.send(true);
        tokio::time::timeout(Duration::from_secs(1), poller)
            .await
            .expect("poller did not exit within 1s of shutdown")
            .expect("poller task panicked");
    }

    /// WS handle wrapper that counts how many `broadcast` calls land —
    /// used to assert the tick loop keeps polling while the worker
    /// holds an in-flight POST.
    struct CountingWsHandle {
        count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        handle: WsBandmapHandle,
    }

    impl CountingWsHandle {
        fn new() -> Self {
            // Real handle wraps a broadcast channel; subscribing here
            // means the channel actually serializes a frame per send,
            // and the count is observed when we drain.
            let handle = WsBandmapHandle::new("FT-710".into(), 100.0);
            let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut rx = handle.subscribe();
            let counter = std::sync::Arc::clone(&count);
            tokio::spawn(async move {
                while rx.recv().await.is_ok() {
                    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            });
            Self { count, handle }
        }

        fn bandmap_handle(&self) -> WsBandmapHandle {
            self.handle.clone()
        }

        fn count(&self) -> usize {
            self.count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }
}
