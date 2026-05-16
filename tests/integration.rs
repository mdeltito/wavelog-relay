//! End-to-end test that wires real rigctld actor + axum listener +
//! wavelog client + wiremock together and exercises both directions
//! of the bridge in a single scenario.
//!
//! Outbound: the 1 Hz poller reads rig state via the actor and POSTs
//! to wiremock-backed `/api/radio`.
//! Inbound: a real `reqwest::get` to the listener routes through the
//! actor as `F`/`M` commands, captured by the recording mock rigctld.
//!
//! Real time only — paused tokio time conflicts with reqwest's
//! built-in timeout via auto-advance, and the no-timeout escape hatch
//! used by inner unit tests is not part of the public API.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::HeaderValue;
use futures_util::StreamExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue as TungHeaderValue;
use tokio_tungstenite::tungstenite::protocol::Message as TungMessage;
use wavelog_bridge::modes::ModeOverrides;
use wavelog_bridge::qso_queue::QsoQueue;
use wavelog_bridge::wavelog::WavelogClient;
use wavelog_bridge::ws::WsBandmapHandle;
use wavelog_bridge::{listener, poller, rigctld, ws, wsjtx};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct RecordingRigctld {
    addr: SocketAddr,
    recorded: Arc<Mutex<Vec<String>>>,
}

impl RecordingRigctld {
    fn commands(&self) -> Vec<String> {
        self.recorded.lock().unwrap().clone()
    }
}

async fn spawn_recording_rigctld() -> RecordingRigctld {
    let recorded: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let recorded_for_task = Arc::clone(&recorded);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let recorded = Arc::clone(&recorded_for_task);
            tokio::spawn(handle_mock_session(stream, recorded));
        }
    });
    RecordingRigctld { addr, recorded }
}

async fn handle_mock_session(stream: TcpStream, recorded: Arc<Mutex<Vec<String>>>) {
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
        let cmd = line.trim_end_matches(['\r', '\n']).to_owned();
        let reply: &[u8] = match cmd.as_str() {
            "f" => b"14000000\n",
            "m" => b"USB\n2400\n",
            "\\get_level RFPOWER" => b"0.1\n",
            c if c.starts_with("F ") || c.starts_with("M ") => b"RPRT 0\n",
            _ => b"RPRT -11\n",
        };
        recorded.lock().unwrap().push(cmd);
        if stream.write_all(reply).await.is_err() {
            return;
        }
        if stream.flush().await.is_err() {
            return;
        }
    }
}

#[tokio::test]
async fn full_round_trip_outbound_and_inbound() {
    let mock_rig = spawn_recording_rigctld().await;
    let (rig_handle, rig_join) = rigctld::spawn(mock_rig.addr, Duration::from_secs(3));

    let wavelog_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/radio"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&wavelog_server)
        .await;
    let client = WavelogClient::new(&wavelog_server.uri(), "test-key").unwrap();

    let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_addr = tcp_listener.local_addr().unwrap();

    let ws_tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = ws_tcp_listener.local_addr().unwrap();
    let ws_handle = WsBandmapHandle::new("FT-710".into(), 100.0);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let poller_task = tokio::spawn(poller::run(
        rig_handle.clone(),
        client,
        "FT-710".into(),
        100.0,
        ws_handle.clone(),
        Duration::from_millis(50),
        shutdown_rx.clone(),
    ));

    let listener_task = tokio::spawn(listener::serve(
        tcp_listener,
        rig_handle.clone(),
        HeaderValue::from_static("https://wavelog.test"),
        ModeOverrides::default(),
        shutdown_rx.clone(),
    ));

    let ws_task = tokio::spawn(ws::serve(
        ws_tcp_listener,
        ws_handle.clone(),
        HeaderValue::from_static("https://wavelog.test"),
        shutdown_rx.clone(),
    ));

    drop(rig_handle);
    drop(ws_handle);
    drop(shutdown_rx);

    // Give the poller a couple of tick windows to push at least once.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Click a DX-cluster spot
    let url = format!("http://{listener_addr}/14074000/usb");
    let response = tokio::time::timeout(Duration::from_secs(5), reqwest::get(&url))
        .await
        .expect("listener did not respond within 5s")
        .expect("reqwest GET failed");
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let requests = wavelog_server.received_requests().await.unwrap();
    assert!(
        !requests.is_empty(),
        "expected at least one wavelog POST, got 0",
    );
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["key"], "test-key");
    assert_eq!(body["radio"], "FT-710");
    assert_eq!(body["frequency"], 14_000_000);
    assert_eq!(body["mode"], "USB");
    assert!((body["power"].as_f64().unwrap() - 10.0).abs() < 1e-3);

    let recorded = mock_rig.commands();
    assert!(
        recorded.contains(&"F 14074000".to_owned()),
        "missing `F 14074000` in {recorded:?}",
    );
    assert!(
        recorded.contains(&"M USB -1".to_owned()),
        "missing `M USB -1` in {recorded:?}",
    );

    let ws_url = format!("ws://{ws_addr}/");
    let mut req = ws_url.into_client_request().unwrap();
    req.headers_mut().insert(
        "Origin",
        TungHeaderValue::from_static("https://wavelog.test"),
    );
    let (mut socket, _resp) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(req),
    )
    .await
    .expect("ws connect timed out")
    .expect("ws connect failed");

    // First frame is the welcome; subsequent frames are radio_status.
    // Loop until we see a radio_status or hit the timeout.
    let radio_status_body = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let msg = socket
                .next()
                .await
                .expect("ws stream closed")
                .expect("ws error");
            match msg {
                TungMessage::Text(s) => {
                    let parsed: serde_json::Value = serde_json::from_str(s.as_ref()).unwrap();
                    if parsed["type"] == "radio_status" {
                        break parsed;
                    }
                },
                _ => continue,
            }
        }
    })
    .await
    .expect("never received a radio_status frame");

    assert_eq!(radio_status_body["radio"], "FT-710");
    assert_eq!(radio_status_body["frequency"], 14_000_000);
    assert_eq!(radio_status_body["mode"], "USB");
    assert!((radio_status_body["power"].as_f64().unwrap() - 10.0).abs() < 1e-3);
    let ts = radio_status_body["timestamp"]
        .as_u64()
        .expect("timestamp must be epoch ms (u64)");
    assert!(ts >= 1_577_836_800_000, "timestamp {ts} too small");

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), poller_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), ws_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), rig_join).await;
}

const WSJTX_MAGIC: u32 = 0xadbc_cbda;
const WSJTX_MSG_LOGGED_ADIF: u32 = 12;

/// Construct a WSJT-X `Logged ADIF` (type 12) UDP datagram by hand so
/// the integration test doesn't need a running WSJT-X instance.
fn encode_wsjtx_logged_adif(id: &str, adif: &str) -> Vec<u8> {
    fn push_qstring(out: &mut Vec<u8>, s: &str) {
        out.extend_from_slice(&(s.len() as i32).to_be_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    let mut out = Vec::new();
    out.extend_from_slice(&WSJTX_MAGIC.to_be_bytes());
    out.extend_from_slice(&3u32.to_be_bytes()); // schema version
    out.extend_from_slice(&WSJTX_MSG_LOGGED_ADIF.to_be_bytes());
    push_qstring(&mut out, id);
    push_qstring(&mut out, adif);
    out
}

#[tokio::test]
async fn wsjtx_udp_message_forwards_to_wavelog_qso_endpoint() {
    let wavelog_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/qso"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "created",
        })))
        .mount(&wavelog_server)
        .await;

    let udp_listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let listen_addr = udp_listener.local_addr().unwrap();

    let qso_client = WavelogClient::new(&wavelog_server.uri(), "test-key").unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (listener_task, worker_task) = wsjtx::spawn(
        udp_listener,
        qso_client,
        "5".into(),
        None,
        Vec::new(),
        shutdown_rx,
    );

    // Send a WSJT-X type-12 packet
    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let adif = "<CALL:5>VK3AB <MODE:3>FT8 <FREQ:8>14.07400 <EOR>";
    let pkt = encode_wsjtx_logged_adif("WSJT-X", adif);
    sender.send_to(&pkt, listen_addr).await.unwrap();

    // Spin for the POST to land
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let body = loop {
        let requests = wavelog_server.received_requests().await.unwrap_or_default();
        if !requests.is_empty() {
            break serde_json::from_slice::<serde_json::Value>(&requests[0].body).unwrap();
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("wavelog never received the QSO POST");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(body["key"], "test-key");
    assert_eq!(body["station_profile_id"], "5");
    assert_eq!(body["type"], "adif");
    assert_eq!(body["string"], adif);

    // --- shutdown ---
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
}

/// End-to-end persistence happy path: a WSJT-X datagram lands on the
/// UDP socket, gets persisted to disk *before* the POST, and is
/// removed from disk after Wavelog confirms `status: "created"`.
/// Mirrors what `main.rs` does at startup.
#[tokio::test]
async fn wsjtx_qso_persists_to_disk_then_drains_on_success() {
    let wavelog_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/qso"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "created",
        })))
        .mount(&wavelog_server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let queue_path = dir.path().join("qso_queue.jsonl");

    // Open the queue exactly the way main.rs does, including the
    // `Arc::new` wrap that the spawn signature requires.
    let (queue, replay) = QsoQueue::open(queue_path.clone()).await.unwrap();
    assert!(replay.is_empty(), "fresh queue should have no entries");
    let queue = Arc::new(queue);

    let udp_listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let listen_addr = udp_listener.local_addr().unwrap();
    let qso_client = WavelogClient::new(&wavelog_server.uri(), "test-key").unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (listener_task, worker_task) = wsjtx::spawn(
        udp_listener,
        qso_client,
        "5".into(),
        Some(Arc::clone(&queue)),
        replay.into_vec(),
        shutdown_rx,
    );

    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let adif = "<CALL:5>VK3AB <MODE:3>FT8 <FREQ:8>14.07400 <EOR>";
    sender
        .send_to(&encode_wsjtx_logged_adif("WSJT-X", adif), listen_addr)
        .await
        .unwrap();

    // Wait for the full happy path: wavelog must observe the POST AND
    // the queue must drain. Checking `is_empty` alone is a race —
    // `is_empty` is true both before the listener picks up the UDP
    // datagram (append hasn't happened) and after the worker removes
    // the completed entry.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let posts = wavelog_server
            .received_requests()
            .await
            .unwrap_or_default()
            .len();
        if posts >= 1 && queue.is_empty().await {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "did not complete within 5s: {posts} POSTs, {} queue entries",
                queue.len().await,
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;

    // Re-open the file from disk: zero entries means the worker
    // actually wrote the removal back, not just in-memory state.
    drop(queue);
    let (_, replay) = QsoQueue::open(queue_path).await.unwrap();
    assert!(
        replay.is_empty(),
        "queue file should be empty after successful POST",
    );

    let requests = wavelog_server.received_requests().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["string"], adif);
}

/// End-to-end restart-replay path: a daemon crash leaves entries on
/// disk; a fresh start opens the same file, replays its entries
/// through the worker, and drains.
#[tokio::test]
async fn wsjtx_queue_replays_pending_entries_on_startup() {
    let wavelog_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/qso"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "created",
        })))
        .mount(&wavelog_server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let queue_path = dir.path().join("qso_queue.jsonl");

    // First daemon: seed the file with two pending QSOs without
    // running the worker, then drop the queue.
    let (seed_queue, _) = QsoQueue::open(queue_path.clone()).await.unwrap();
    seed_queue
        .append("<CALL:5>K1AAA <MODE:3>FT8 <EOR>".into())
        .await
        .unwrap();
    seed_queue
        .append("<CALL:5>K1BBB <MODE:3>FT8 <EOR>".into())
        .await
        .unwrap();
    drop(seed_queue);

    // Second daemon: re-open and let the spawned worker drain the
    // replay before the listener task pulls any new datagrams.
    let (queue, replay) = QsoQueue::open(queue_path.clone()).await.unwrap();
    assert_eq!(replay.len(), 2, "replay should surface both seeded entries");
    let queue = Arc::new(queue);

    let udp_listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let qso_client = WavelogClient::new(&wavelog_server.uri(), "test-key").unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (listener_task, worker_task) = wsjtx::spawn(
        udp_listener,
        qso_client,
        "5".into(),
        Some(Arc::clone(&queue)),
        replay.into_vec(),
        shutdown_rx,
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let posts = wavelog_server
            .received_requests()
            .await
            .unwrap_or_default()
            .len();
        if posts >= 2 && queue.is_empty().await {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "replay did not complete: {posts} POSTs, {} entries left",
                queue.len().await,
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;

    let bodies: Vec<_> = wavelog_server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r| serde_json::from_slice::<serde_json::Value>(&r.body).unwrap())
        .map(|v| v["string"].as_str().unwrap().to_owned())
        .collect();
    assert!(
        bodies.iter().any(|s| s.contains("K1AAA")),
        "K1AAA missing from {bodies:?}",
    );
    assert!(
        bodies.iter().any(|s| s.contains("K1BBB")),
        "K1BBB missing from {bodies:?}",
    );
}
