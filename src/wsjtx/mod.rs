//! WSJT-X UDP listener.
//!
//! WSJT-X (and its forks JTDX/MSHV) emit a binary "network message"
//! protocol over UDP, documented in upstream's `Network/NetworkMessage.hpp`
//! and parsed by [`protocol`]. The default listener is `127.0.0.1:2237`.
//!
//! We care about exactly one message: **Logged ADIF (type 12)**. It
//! carries the complete ADIF record from a completed QSO and is what
//! we forward to Wavelog's `/api/qso` endpoint. WSJT-X also emits
//! `QSO Logged` (type 5) with structured fields immediately before
//! type 12 for each logged QSO — we discard it to avoid double-logging.
//! All other types (Heartbeat, Status, …) are accepted and discarded
//! at debug level.
//!
//! Architecture: a UDP listener task receives + parses, and pipes the
//! ADIF string through a bounded mpsc (32) to a POST worker that calls
//! [`WavelogClient::push_qso`]. Two tasks (rather than one inline) so
//! a slow Wavelog POST can't stall UDP receive — overflowed bursts
//! drop with a warn log instead of relying on the kernel's UDP buffer
//! quietly catching them.

mod bind;
mod protocol;

use std::sync::Arc;

pub use bind::bind;
pub use protocol::WsjtxError;
use protocol::{parse_logged_adif, summarize_adif};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::qso_queue::QsoQueue;
use crate::util::shutdown_observed;
use crate::wavelog::{StationSource, WavelogClient, WavelogError};

const QUEUE_CAPACITY: usize = 32;
const RECV_BUF_SIZE: usize = 65_535;

/// Spawn the WSJT-X listener and POST worker.
///
/// The listener reads from the pre-bound `socket`, parses
/// Logged-ADIF messages, and pipes them through a bounded queue to
/// the worker, which submits each QSO via
/// [`WavelogClient::push_qso`] using the station ID returned by
/// [`StationSource::resolve`] at POST time.
///
/// `queue` enables on-disk persistence: the listener appends every
/// accepted ADIF to disk before queueing, and the worker removes the
/// entry only after a successful POST or a permanent
/// [`WavelogError::Rejected`] response. Pass `None` to run in pure
/// in-memory mode (the v1 behaviour); transient outages drop QSOs
/// after the standard `[0, 1, 4] s` retries exhaust.
///
/// `replay` is the set of entries already on disk at startup; they
/// get pushed onto the worker queue ahead of any new arrivals. Pass
/// an empty vec when persistence is disabled.
///
/// Returns both task join handles. Both observe the shutdown watch
/// channel and exit cleanly when it flips to `true` or its sender is
/// dropped.
#[must_use]
pub fn spawn(
    socket: UdpSocket,
    client: WavelogClient,
    station: StationSource,
    queue: Option<Arc<QsoQueue>>,
    replay: Vec<(u64, Box<str>)>,
    shutdown: watch::Receiver<bool>,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<QueueItem>(QUEUE_CAPACITY);
    let listener = tokio::spawn(listen(
        socket,
        tx,
        queue.clone(),
        replay,
        station.clone(),
        shutdown.clone(),
    ));
    let worker = tokio::spawn(post_loop(rx, client, station, queue, shutdown));
    (listener, worker)
}

#[derive(Debug)]
struct QueueItem {
    /// Sequence number from the on-disk queue, so the worker can call
    /// `queue.remove(seq)` after a successful POST. `None` when
    /// persistence is disabled or the entry came from a path that
    /// didn't go through the queue.
    seq: Option<u64>,
    adif: Box<str>,
}

async fn listen(
    socket: UdpSocket,
    tx: mpsc::Sender<QueueItem>,
    queue: Option<Arc<QsoQueue>>,
    replay: Vec<(u64, Box<str>)>,
    station: StationSource,
    mut shutdown: watch::Receiver<bool>,
) {
    if let Ok(addr) = socket.local_addr() {
        tracing::info!(
            addr = %addr,
            station = ?station,
            replay_count = replay.len(),
            "wsjtx listener serving",
        );
    }
    if *shutdown.borrow_and_update() {
        return;
    }

    // Prime the worker with replay entries before the recv loop starts.
    // send().await back-pressures so a replay deeper than QUEUE_CAPACITY drains fully.
    for (seq, adif) in replay {
        tokio::select! {
            res = tx.send(QueueItem { seq: Some(seq), adif }) => {
                if res.is_err() {
                    tracing::warn!("wsjtx POST worker channel closed during replay; exiting listener");
                    return;
                }
            }
            result = shutdown.changed() => {
                if shutdown_observed(result, &shutdown) {
                    tracing::info!("wsjtx listener shutting down (during replay)");
                    return;
                }
            }
        }
    }

    let mut buf = vec![0u8; RECV_BUF_SIZE];
    loop {
        tokio::select! {
            recv = socket.recv_from(&mut buf) => {
                let (n, from) = match recv {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(error = %e, "wsjtx recv_from failed");
                        continue;
                    }
                };
                match parse_logged_adif(&buf[..n]) {
                    Ok(Some(adif)) => {
                        let summary = summarize_adif(&adif);
                        tracing::info!(
                            from = %from,
                            callsign = summary.callsign(),
                            mode = summary.mode(),
                            band = summary.band(),
                            adif_len = adif.len(),
                            "wsjtx QSO received",
                        );
                        let item = match &queue {
                            Some(q) => {
                                // Persist before handoff so a crash between accept and POST
                                // doesn't lose the QSO.
                                match q.append(adif.clone()).await {
                                    Ok(seq) => QueueItem { seq: Some(seq), adif },
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            from = %from,
                                            "wsjtx queue append failed; dropping ADIF",
                                        );
                                        continue;
                                    }
                                }
                            }
                            None => QueueItem { seq: None, adif },
                        };
                        match tx.try_send(item) {
                            Ok(()) => {},
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                tracing::warn!(
                                    "wsjtx POST queue full; dropping ADIF (still on disk if persistence is on)",
                                );
                            },
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                tracing::warn!("wsjtx POST worker channel closed; exiting listener");
                                return;
                            },
                        }
                    }
                    Ok(None) => {
                        tracing::debug!(from = %from, "wsjtx non-LoggedADIF message ignored");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, from = %from, "wsjtx parse failed");
                    }
                }
            },
            result = shutdown.changed() => {
                if shutdown_observed(result, &shutdown) {
                    tracing::info!("wsjtx listener shutting down");
                    return;
                }
            }
        }
    }
}

async fn post_loop(
    mut rx: mpsc::Receiver<QueueItem>,
    client: WavelogClient,
    station: StationSource,
    queue: Option<Arc<QsoQueue>>,
    mut shutdown: watch::Receiver<bool>,
) {
    if *shutdown.borrow_and_update() {
        return;
    }
    loop {
        tokio::select! {
            item = rx.recv() => match item {
                Some(item) => {
                    // Inner select cancels the in-flight POST (and the
                    // preceding active-station lookup, if any) on shutdown.
                    tokio::select! {
                        push_res = push_one(&client, &station, &item.adif) => {
                            handle_post_outcome(push_res, &item, queue.as_deref()).await;
                        },
                        result = shutdown.changed() => {
                            if shutdown_observed(result, &shutdown) {
                                tracing::info!("wsjtx POST worker shutting down (mid-push)");
                                return;
                            }
                        }
                    }
                }
                None => return,
            },
            result = shutdown.changed() => {
                if shutdown_observed(result, &shutdown) {
                    tracing::info!("wsjtx POST worker shutting down");
                    return;
                }
            }
        }
    }
}

/// Resolve the station ID and submit the ADIF. Pulled out so the
/// resolve + POST sequence shares a single cancellation boundary in
/// [`post_loop`].
async fn push_one(
    client: &WavelogClient,
    station: &StationSource,
    adif: &str,
) -> Result<(), WavelogError> {
    let station_id = station.resolve().await?;
    client.push_qso(&station_id, adif).await
}

async fn handle_post_outcome(
    res: Result<(), WavelogError>,
    item: &QueueItem,
    queue: Option<&QsoQueue>,
) {
    let summary = summarize_adif(&item.adif);
    let callsign = summary.callsign();
    let mode = summary.mode();
    let band = summary.band();
    match res {
        Ok(()) => {
            tracing::info!(
                callsign,
                mode,
                band,
                seq = item.seq,
                "wsjtx QSO logged to wavelog",
            );
            remove_persisted(item.seq, queue, "completed").await;
        },
        Err(WavelogError::Rejected { ref reason }) => {
            // Wavelog answered cleanly with a permanent "no" (duplicate,
            // validation error, etc). Retrying will produce the same
            // answer; drop the entry from disk too.
            tracing::warn!(
                reason = %reason,
                callsign,
                mode,
                band,
                "wsjtx QSO rejected by wavelog",
            );
            remove_persisted(item.seq, queue, "rejected").await;
        },
        Err(e) => {
            // Transport / 5xx / 4xx / BadResponse — keep on disk so
            // the next startup (or a future outage-recovery pass)
            // gets another shot.
            tracing::warn!(
                error = %e,
                callsign,
                mode,
                band,
                seq = item.seq,
                "wsjtx QSO POST failed; entry retained on disk for retry",
            );
        },
    }
}

async fn remove_persisted(seq: Option<u64>, queue: Option<&QsoQueue>, why: &'static str) {
    let (Some(seq), Some(queue)) = (seq, queue) else {
        return;
    };
    if let Err(e) = queue.remove(seq).await {
        tracing::warn!(error = %e, seq, why, "wsjtx queue remove failed");
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::net::UdpSocket;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::protocol::test_packets::{encode_logged_adif, encode_qso_logged};
    use super::*;

    #[tokio::test]
    async fn listener_to_worker_round_trip_posts_to_wavelog() {
        // Real UDP socket on an ephemeral port + wiremock standing in
        // for Wavelog's `/api/qso`.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "created",
            })))
            .mount(&server)
            .await;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = socket.local_addr().unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) = spawn(
            socket,
            client,
            StationSource::Fixed("7".into()),
            None,
            Vec::new(),
            shutdown_rx,
        );

        // Send the WSJT-X packet from a separate socket.
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let pkt = encode_logged_adif("WSJT-X", "<CALL:5>VK3AB <MODE:3>FT8 <EOR>");
        sender.send_to(&pkt, listen_addr).await.unwrap();

        // Spin for up to a few seconds for the POST to land.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let requests = server.received_requests().await.unwrap_or_default();
            if !requests.is_empty() {
                let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
                assert_eq!(body["key"], "test-key");
                assert_eq!(body["station_profile_id"], "7");
                assert_eq!(body["type"], "adif");
                assert_eq!(body["string"], "<CALL:5>VK3AB <MODE:3>FT8 <EOR>");
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("wavelog never received the QSO POST");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    }

    #[tokio::test]
    async fn listener_ignores_non_logged_adif_messages() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "created",
            })))
            .mount(&server)
            .await;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = socket.local_addr().unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) = spawn(
            socket,
            client,
            StationSource::Fixed("1".into()),
            None,
            Vec::new(),
            shutdown_rx,
        );

        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .send_to(&encode_qso_logged("WSJT-X"), listen_addr)
            .await
            .unwrap();

        // Give the listener time to receive + discard.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let requests = server.received_requests().await.unwrap_or_default();
        assert!(
            requests.is_empty(),
            "type 5 (QSO Logged) should not have produced a POST: got {requests:?}",
        );

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    }

    #[tokio::test]
    async fn shutdown_signal_stops_listener_and_worker() {
        let server = MockServer::start().await;
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client = WavelogClient::new(&server.uri(), "k").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) = spawn(
            socket,
            client,
            StationSource::Fixed("1".into()),
            None,
            Vec::new(),
            shutdown_rx,
        );

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_millis(500), listener_task)
            .await
            .expect("listener didn't exit within 500ms")
            .expect("listener panicked");
        tokio::time::timeout(Duration::from_millis(500), worker_task)
            .await
            .expect("worker didn't exit within 500ms")
            .expect("worker panicked");
    }

    #[tokio::test]
    async fn shutdown_during_inflight_qso_post_returns_promptly() {
        // Wavelog mock holds every POST for 60s. Without cancellation-
        // aware shutdown the worker would block for the full 5s
        // timeout × 3 retries. Tight the assert to 1s to lock that in.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(60)))
            .mount(&server)
            .await;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = socket.local_addr().unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) = spawn(
            socket,
            client,
            StationSource::Fixed("1".into()),
            None,
            Vec::new(),
            shutdown_rx,
        );

        // Push one ADIF in to start a POST.
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .send_to(
                &encode_logged_adif("WSJT-X", "<CALL:5>VK3AB <EOR>"),
                listen_addr,
            )
            .await
            .unwrap();

        // Give the worker time to pick up the ADIF and start the POST.
        tokio::time::sleep(Duration::from_millis(150)).await;

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), listener_task)
            .await
            .expect("listener did not exit within 1s of shutdown")
            .expect("listener panicked");
        tokio::time::timeout(Duration::from_secs(1), worker_task)
            .await
            .expect("worker did not exit within 1s of shutdown (in-flight POST not cancelled)")
            .expect("worker panicked");
    }

    #[tokio::test]
    async fn persisted_adif_is_removed_from_disk_after_successful_post() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "created",
            })))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let queue_path = dir.path().join("queue.jsonl");
        let (queue, _replay) = QsoQueue::open(queue_path.clone()).await.unwrap();
        let queue = Arc::new(queue);

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = socket.local_addr().unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) = spawn(
            socket,
            client,
            StationSource::Fixed("7".into()),
            Some(queue.clone()),
            Vec::new(),
            shutdown_rx,
        );

        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .send_to(
                &encode_logged_adif("WSJT-X", "<CALL:5>VK3AB <EOR>"),
                listen_addr,
            )
            .await
            .unwrap();

        // Spin until the queue drains (POST landed -> remove called).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            if queue.len().await == 0 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "queue still holds {} entries after success",
                    queue.len().await,
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;

        // Re-open the file: zero entries means the worker actually
        // wrote the removal back to disk, not just memory.
        drop(queue);
        let (_reopened, replay) = QsoQueue::open(queue_path).await.unwrap();
        assert!(
            replay.is_empty(),
            "queue file should be empty after success"
        );
    }

    #[tokio::test]
    async fn persisted_adif_is_kept_when_wavelog_returns_5xx() {
        // 5xx is retryable. After exhausting [0,1,4]s retries the
        // worker must leave the entry on disk for next-startup replay.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let queue_path = dir.path().join("queue.jsonl");
        let (queue, _replay) = QsoQueue::open(queue_path.clone()).await.unwrap();
        let queue = Arc::new(queue);

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = socket.local_addr().unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) = spawn(
            socket,
            client,
            StationSource::Fixed("7".into()),
            Some(queue.clone()),
            Vec::new(),
            shutdown_rx,
        );

        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .send_to(
                &encode_logged_adif("WSJT-X", "<CALL:5>VK3AB <EOR>"),
                listen_addr,
            )
            .await
            .unwrap();

        // Wait long enough for [0,1,4]s retry exhaustion (~5s).
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert_eq!(
            queue.len().await,
            1,
            "5xx after retry exhaustion must keep entry on disk for next-startup replay",
        );

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    }

    #[tokio::test]
    async fn replay_entries_are_pushed_through_worker_at_startup() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "created",
            })))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let queue_path = dir.path().join("queue.jsonl");

        // Pre-seed the queue with 2 entries (simulating leftover from
        // a prior daemon run).
        let (queue, _) = QsoQueue::open(queue_path.clone()).await.unwrap();
        queue.append("<CALL:5>K1AAA <EOR>".into()).await.unwrap();
        queue.append("<CALL:5>K1BBB <EOR>".into()).await.unwrap();
        drop(queue);

        // Re-open and feed the replay back into a fresh spawn.
        let (queue, replay) = QsoQueue::open(queue_path.clone()).await.unwrap();
        let queue = Arc::new(queue);
        assert_eq!(replay.len(), 2);

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) = spawn(
            socket,
            client,
            StationSource::Fixed("7".into()),
            Some(queue.clone()),
            replay.into_vec(),
            shutdown_rx,
        );

        // Spin for both replays to land.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let posts = server.received_requests().await.unwrap_or_default().len();
            if posts >= 2 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("expected 2 replayed POSTs, got {posts}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // And the queue should now be empty in memory + on disk.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if queue.len().await == 0 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("queue did not drain after replay+success");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    }

    #[tokio::test]
    async fn active_station_source_routes_qso_to_active_profile() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/station_info/test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                { "station_id": "3", "station_profile_name": "Home", "station_callsign": "K1", "station_active": null },
                { "station_id": "11", "station_profile_name": "Portable", "station_callsign": "K1/P", "station_active": "1" },
            ])))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "created",
            })))
            .mount(&server)
            .await;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = socket.local_addr().unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) = spawn(
            socket,
            client.clone(),
            StationSource::active(client),
            None,
            Vec::new(),
            shutdown_rx,
        );

        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .send_to(
                &encode_logged_adif("WSJT-X", "<CALL:5>VK3AB <MODE:3>FT8 <EOR>"),
                listen_addr,
            )
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let qso_requests: Vec<_> = server
                .received_requests()
                .await
                .unwrap_or_default()
                .into_iter()
                .filter(|r| r.url.path() == "/api/qso")
                .collect();
            if let Some(req) = qso_requests.first() {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                assert_eq!(
                    body["station_profile_id"], "11",
                    "QSO must have been routed to the active station",
                );
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("wavelog never received the QSO POST");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    }

    #[tokio::test]
    async fn no_active_station_keeps_qso_on_spool_and_skips_post() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/station_info/test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                { "station_id": "3", "station_profile_name": "Home", "station_callsign": "K1", "station_active": null },
            ])))
            .mount(&server)
            .await;
        // No mock for /api/qso — assertion is that it's never hit.

        let dir = tempfile::tempdir().unwrap();
        let queue_path = dir.path().join("queue.jsonl");
        let (queue, _replay) = QsoQueue::open(queue_path.clone()).await.unwrap();
        let queue = Arc::new(queue);

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = socket.local_addr().unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) = spawn(
            socket,
            client.clone(),
            StationSource::active(client),
            Some(queue.clone()),
            Vec::new(),
            shutdown_rx,
        );

        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .send_to(
                &encode_logged_adif("WSJT-X", "<CALL:5>VK3AB <EOR>"),
                listen_addr,
            )
            .await
            .unwrap();

        // Give the worker time to consume + fail + retry cycle.
        // Resolver returns NoActiveStation (non-retryable) so the
        // retry chain collapses immediately; the entry stays on disk.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            // Wait until the queue at least observed the QSO arrive on disk.
            if queue.len().await >= 1 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("ADIF never landed on disk");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Give the worker a moment to complete (or skip) the POST.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let qso_posts = server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|r| r.url.path() == "/api/qso")
            .count();
        assert_eq!(
            qso_posts, 0,
            "no /api/qso POST should fire when no active station is configured",
        );
        assert_eq!(
            queue.len().await,
            1,
            "QSO must stay on spool when station resolution fails",
        );

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    }
}
