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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use wavelog_bridge::modes::ModeOverrides;
use wavelog_bridge::wavelog::WavelogClient;
use wavelog_bridge::{listener, poller, rigctld};
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
    // --- mock rigctld ---
    let mock_rig = spawn_recording_rigctld().await;
    let (rig_handle, rig_join) = rigctld::spawn(mock_rig.addr, Duration::from_secs(3));

    // --- wiremock wavelog ---
    let wavelog_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/radio"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&wavelog_server)
        .await;
    let client = WavelogClient::new(&wavelog_server.uri(), "test-key", "FT-710", 100.0).unwrap();

    // --- listener (real bind) ---
    let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_addr = tcp_listener.local_addr().unwrap();

    // --- shutdown channel ---
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let poller_task = tokio::spawn(poller::run(
        rig_handle.clone(),
        client,
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

    drop(rig_handle);
    drop(shutdown_rx);

    // Give the poller a couple of tick windows to push at least once.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- inbound: click a DX-cluster spot ---
    let url = format!("http://{listener_addr}/14074000/usb");
    let response = tokio::time::timeout(Duration::from_secs(5), reqwest::get(&url))
        .await
        .expect("listener did not respond within 5s")
        .expect("reqwest GET failed");
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // --- outbound: wavelog received a JSON POST with the expected shape ---
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

    // --- inbound assertion: rigctld received F and M from the listener ---
    let recorded = mock_rig.commands();
    assert!(
        recorded.contains(&"F 14074000".to_owned()),
        "missing `F 14074000` in {recorded:?}",
    );
    assert!(
        recorded.contains(&"M USB 0".to_owned()),
        "missing `M USB 0` in {recorded:?}",
    );

    // --- shutdown ---
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), poller_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), rig_join).await;
}
