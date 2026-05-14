//! Periodic poll → push loop.
//!
//! Each tick reads a [`RigState`] via [`RigHandle::poll`] and forwards
//! it to [`WavelogClient::push`], which encapsulates the
//! dedupe + heartbeat + retry policy. Individual rig polling and
//! wavelog push errors are logged at WARN and the loop continues —
//! only a shutdown signal exits.
//!
//! [`RigState`]: crate::rigctld::RigState

use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{MissedTickBehavior, interval};

use crate::rigctld::RigHandle;
use crate::wavelog::WavelogClient;

/// Run the poll → push loop until `shutdown` resolves to `true` or the
/// watch sender is dropped. Per-tick errors are logged and skipped.
pub async fn run(
    rig: RigHandle,
    mut client: WavelogClient,
    tick_interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    // Mark the current value as seen so `changed()` only fires on
    // subsequent updates. If the sender already flipped to `true`
    // before we got here, exit straight away.
    if *shutdown.borrow_and_update() {
        return;
    }

    let mut ticker = interval(tick_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tracing::info!(?tick_interval, "poller started");
    loop {
        tokio::select! {
            _ = ticker.tick() => tick(&rig, &mut client).await,
            result = shutdown.changed() => {
                // Sender dropped, or value changed: either way, only
                // exit when the value is now `true`.
                let should_stop = result.is_err() || *shutdown.borrow();
                if should_stop {
                    tracing::info!("poller shutting down");
                    return;
                }
            }
        }
    }
}

async fn tick(rig: &RigHandle, client: &mut WavelogClient) {
    let state = match rig.poll().await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "rig poll failed");
            return;
        },
    };
    if let Err(e) = client.push(&state).await {
        tracing::warn!(error = %e, "wavelog push failed");
    }
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
        WavelogClient::new("http://127.0.0.1:1", "k", "r", 100.0).unwrap()
    }

    #[tokio::test]
    async fn shutdown_signal_set_to_true_stops_loop_promptly() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let poller = tokio::spawn(run(
            dummy_rig_handle(),
            dummy_wavelog_client(),
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
        let client = WavelogClient::new(&server.uri(), "test-key", "FT-710", 100.0).unwrap();

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let poller = tokio::spawn(run(rig, client, Duration::from_millis(50), shutdown_rx));

        // Real-time wait: ~4 ticks, but dedupe collapses them to 1 POST.
        tokio::time::sleep(Duration::from_millis(250)).await;

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), poller)
            .await
            .expect("poller did not exit within 1s")
            .expect("poller task panicked");

        let requests = server.received_requests().await.unwrap();
        assert!(
            !requests.is_empty(),
            "expected at least one POST to wavelog"
        );
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["frequency"], 14_074_000);
        assert_eq!(body["mode"], "USB");
        assert!((body["power"].as_f64().unwrap() - 10.0).abs() < 1e-3);
    }

    #[tokio::test]
    async fn rig_errors_do_not_kill_the_loop() {
        // Rigctld pointed at port 1: the actor stays in backoff, every
        // poll returns Disconnected. Poller should keep ticking instead
        // of returning.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let poller = tokio::spawn(run(
            dummy_rig_handle(),
            dummy_wavelog_client(),
            Duration::from_millis(20),
            shutdown_rx,
        ));

        // Give the loop time to attempt several ticks; if a rig error
        // were fatal the task would already be done.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!poller.is_finished(), "poller exited on rig errors");

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), poller)
            .await
            .unwrap()
            .unwrap();
    }
}
