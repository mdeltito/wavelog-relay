//! WebSocket server for Wavelog's frontend.
//!
//! Wavelog's `assets/js/cat.js` opens a WebSocket to
//! `ws://127.0.0.1:54322` (after trying `wss://...:54323` first) and
//! consumes `radio_status` frames to drive multiple live widgets —
//! primarily the rig card on the dashboard, with the bandmap page as a
//! secondary consumer. Without a server on either port the frontend
//! falls back to a 3 s AJAX poll of `/api/radio` — usable but sluggish,
//! especially while spinning the VFO.
//!
//! Scope (intentionally narrower than WaveLogGate):
//!
//! - **WS only, no WSS.** The frontend tries WSS first, fails fast, and
//!   reconnects to WS. A real WSS cert implies the user already has TLS
//!   infrastructure that could just as easily reverse-proxy to us; a
//!   self-signed cert is useless in the browser. Same Safari caveat as
//!   the HTTP listener applies.
//! - **One outbound message type: `radio_status`.** Sent on every poll
//!   tick (1 Hz), not deduped — the POST dedupe exists to spare Wavelog
//!   DB writes; WS frames are cheap and the frontend wants live updates
//!   while the VFO turns.
//! - **Inbound messages are accepted and discarded.** Wavelog's bandmap
//!   page sends `qso_logged` / `satellite_position` / `lookup_result` —
//!   forwarding those (UDP to N1MM, az/el to rotctld, etc.) is a
//!   separate feature with its own config surface, deferred past v1.
//! - **Origin check is strict.** The WS handshake has no CORS; the
//!   server alone decides whether to accept it. We require the `Origin`
//!   header to equal the configured Wavelog URL's origin.

use std::io;
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::Serialize;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch};

use crate::rigctld::RigState;
use crate::util::{epoch_millis, shutdown_observed, wait_for_shutdown};

const CHANNEL_CAPACITY: usize = 16;
const WELCOME_FRAME: &str = r#"{"type":"welcome"}"#;
const INBOUND_LOG_TRUNC: usize = 120;

#[derive(Debug, Error)]
pub enum WsError {
    #[error("axum serve loop failed: {0}")]
    Serve(#[source] io::Error),
}

/// Cloneable broadcast handle. The poller produces one
/// [`broadcast`](WsHandle::broadcast) call per tick; every
/// connected client receives the serialized `radio_status` frame.
#[derive(Clone)]
pub struct WsHandle {
    inner: Arc<HandleInner>,
}

struct HandleInner {
    tx: broadcast::Sender<Arc<str>>,
    radio: Box<str>,
    power_max_watts: f32,
}

impl WsHandle {
    pub fn new(radio: Box<str>, power_max_watts: f32) -> Self {
        let (tx, _rx) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            inner: Arc::new(HandleInner {
                tx,
                radio,
                power_max_watts,
            }),
        }
    }

    /// Serialize a `radio_status` frame from this snapshot and fan it
    /// out to all subscribers. Non-blocking; if there are no
    /// subscribers (the common idle case) or the frame fails to
    /// serialize, the call is silently a no-op aside from a debug log.
    pub fn broadcast(&self, state: &RigState) {
        let frame = RadioStatus {
            kind: "radio_status",
            radio: &self.inner.radio,
            frequency: state.freq,
            mode: &state.mode,
            power: state.power.map(|p| p * self.inner.power_max_watts),
            timestamp: epoch_millis(),
        };
        let json = match serde_json::to_string(&frame) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "ws: serialize radio_status");
                return;
            },
        };
        // Err only when there are no subscribers (the idle case).
        let _ = self.inner.tx.send(Arc::from(json));
    }

    /// Subscribe to the broadcast stream of `radio_status` frames.
    /// `pub(crate)` so the poller's integration tests can count
    /// frames; not part of the public crate API.
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<Arc<str>> {
        self.inner.tx.subscribe()
    }
}

#[derive(Serialize)]
struct RadioStatus<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    radio: &'a str,
    frequency: u64,
    mode: &'a str,
    /// Watts (post `--power-max` scaling). Omitted when the rig
    /// backend doesn't expose RFPOWER readback so the rig card shows
    /// freq/mode without a fake wattage.
    #[serde(skip_serializing_if = "Option::is_none")]
    power: Option<f32>,
    /// Unix epoch milliseconds. Wavelog's `cat.js` consumes this with
    /// `Date.now() - data.timestamp` to drive the rig card's staleness
    /// indicator — anything other than a numeric epoch-ms produces NaN
    /// and the staleness state never fires.
    timestamp: u64,
}

/// Run the WebSocket server on a pre-bound TCP listener until
/// `shutdown` resolves to `true` (or its sender drops). Pre-binding
/// (same pattern as the HTTP listener) ensures `EADDRINUSE` on 54322
/// surfaces synchronously at startup.
pub async fn serve(
    tcp_listener: TcpListener,
    handle: WsHandle,
    allow_origin: HeaderValue,
    shutdown: watch::Receiver<bool>,
) -> Result<(), WsError> {
    if let Ok(addr) = tcp_listener.local_addr() {
        tracing::info!(addr = %addr, "ws serving");
    }
    let app = build_router(handle, allow_origin, shutdown.clone());
    axum::serve(tcp_listener, app)
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await
        .map_err(WsError::Serve)?;
    tracing::info!("ws stopped");
    Ok(())
}

fn build_router(
    handle: WsHandle,
    allow_origin: HeaderValue,
    shutdown: watch::Receiver<bool>,
) -> Router {
    let state = AppState {
        handle,
        allow_origin,
        shutdown,
    };
    Router::new().route("/", get(ws_upgrade)).with_state(state)
}

#[derive(Clone)]
struct AppState {
    handle: WsHandle,
    allow_origin: HeaderValue,
    shutdown: watch::Receiver<bool>,
}

/// Logs a `disconnected` info on drop. Used inside `handle_connection`
/// so every exit path — clean close, lag drop, shutdown, socket error —
/// produces a matching pair to the connect log.
struct ConnectionGuard;

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        tracing::info!("ws client disconnected");
    }
}

async fn ws_upgrade(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    // CORS doesn't apply to WS upgrades; server must vet Origin or any
    // HTTPS origin could read live rig state.
    let origin = headers.get(header::ORIGIN);
    if origin != Some(&state.allow_origin) {
        tracing::warn!(?origin, "ws: rejecting handshake with mismatched origin",);
        return StatusCode::FORBIDDEN.into_response();
    }
    ws.on_upgrade(move |socket| handle_connection(socket, state.handle, state.shutdown))
}

async fn handle_connection(
    mut socket: WebSocket,
    handle: WsHandle,
    mut shutdown: watch::Receiver<bool>,
) {
    if *shutdown.borrow_and_update() {
        let _ = socket.send(Message::Close(None)).await;
        return;
    }

    let mut rx = handle.subscribe();

    if socket
        .send(Message::Text(WELCOME_FRAME.into()))
        .await
        .is_err()
    {
        return;
    }

    tracing::info!("ws client connected");
    let _guard = ConnectionGuard;

    loop {
        tokio::select! {
            biased;

            result = shutdown.changed() => {
                if shutdown_observed(result, &shutdown) {
                    let _ = socket.send(Message::Close(None)).await;
                    return;
                }
            }

            recv = rx.recv() => match recv {
                Ok(frame) => {
                    if socket.send(Message::Text(frame.as_ref().into())).await.is_err() {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    // Drop on lag; frontend reconnects and stale state is worse than a gap.
                    tracing::warn!(skipped = n, "ws: subscriber lagged, dropping connection");
                    let _ = socket.send(Message::Close(None)).await;
                    return;
                }
                Err(broadcast::error::RecvError::Closed) => return,
            },

            msg = socket.recv() => match msg {
                Some(Ok(Message::Text(s))) => {
                    let s_ref: &str = s.as_ref();
                    let trunc: String = s_ref.chars().take(INBOUND_LOG_TRUNC).collect();
                    tracing::debug!(text = %trunc, "ws: ignoring inbound text");
                }
                Some(Ok(Message::Binary(_))) => {
                    tracing::debug!("ws: ignoring inbound binary frame");
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) | None => return,
                Some(Err(e)) => {
                    tracing::debug!(error = %e, "ws: socket recv error, closing");
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use axum::http::StatusCode;
    use futures_util::StreamExt;
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::HeaderValue as TungHeaderValue;
    use tokio_tungstenite::tungstenite::protocol::Message as TungMessage;

    use super::*;

    fn allow_origin() -> HeaderValue {
        HeaderValue::from_static("https://wavelog.test")
    }

    fn dummy_state() -> RigState {
        RigState {
            freq: 14_074_000,
            mode: "USB".into(),
            power: Some(0.1),
        }
    }

    fn dummy_state_no_power() -> RigState {
        RigState {
            freq: 14_074_000,
            mode: "USB".into(),
            power: None,
        }
    }

    /// Bind a serve loop on an ephemeral port and return its address.
    /// Caller is responsible for keeping the returned `JoinHandle` alive
    /// so the task doesn't abort.
    async fn spawn_server(
        handle: WsHandle,
        shutdown: watch::Receiver<bool>,
    ) -> (SocketAddr, tokio::task::JoinHandle<Result<(), WsError>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(serve(listener, handle, allow_origin(), shutdown));
        (addr, join)
    }

    /// Build a WS client request with a custom Origin header.
    fn ws_request(
        addr: SocketAddr,
        origin: &str,
    ) -> tokio_tungstenite::tungstenite::handshake::client::Request {
        let url = format!("ws://{addr}/");
        let mut req = url.into_client_request().unwrap();
        req.headers_mut()
            .insert("Origin", TungHeaderValue::from_str(origin).unwrap());
        req
    }

    #[tokio::test]
    async fn rejects_handshake_with_mismatched_origin() {
        let handle = WsHandle::new("FT-710".into(), 100.0);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (addr, _join) = spawn_server(handle, shutdown_rx).await;

        let err = tokio_tungstenite::connect_async(ws_request(addr, "https://evil.example"))
            .await
            .expect_err("handshake must fail with bad origin");
        match err {
            tokio_tungstenite::tungstenite::Error::Http(resp) => {
                assert_eq!(resp.status(), StatusCode::FORBIDDEN);
            },
            other => panic!("expected Http(FORBIDDEN), got {other:?}"),
        }

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn rejects_handshake_without_origin_header() {
        let handle = WsHandle::new("FT-710".into(), 100.0);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (addr, _join) = spawn_server(handle, shutdown_rx).await;

        // tokio-tungstenite doesn't send Origin by default unless we
        // build the request ourselves, so this is the no-origin case.
        let url = format!("ws://{addr}/");
        let req = url.into_client_request().unwrap();
        let err = tokio_tungstenite::connect_async(req)
            .await
            .expect_err("handshake must fail without Origin");
        match err {
            tokio_tungstenite::tungstenite::Error::Http(resp) => {
                assert_eq!(resp.status(), StatusCode::FORBIDDEN);
            },
            other => panic!("expected Http(FORBIDDEN), got {other:?}"),
        }

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn welcome_then_broadcast_is_received() {
        let handle = WsHandle::new("FT-710".into(), 100.0);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (addr, _join) = spawn_server(handle.clone(), shutdown_rx).await;

        let req = ws_request(addr, "https://wavelog.test");
        let (mut socket, _resp) = tokio::time::timeout(
            Duration::from_secs(2),
            tokio_tungstenite::connect_async(req),
        )
        .await
        .expect("connect timed out")
        .expect("connect failed");

        // First message: welcome
        let msg = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("welcome timed out")
            .expect("stream closed")
            .expect("ws error");
        match msg {
            TungMessage::Text(s) => assert_eq!(s.as_str(), WELCOME_FRAME),
            other => panic!("unexpected first message: {other:?}"),
        }

        // Now broadcast something and assert the client sees it
        handle.broadcast(&dummy_state());
        let msg = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("radio_status timed out")
            .expect("stream closed")
            .expect("ws error");
        let body = match msg {
            TungMessage::Text(s) => s,
            other => panic!("unexpected message: {other:?}"),
        };
        let parsed: serde_json::Value = serde_json::from_str(body.as_str()).unwrap();
        assert_eq!(parsed["type"], "radio_status");
        assert_eq!(parsed["radio"], "FT-710");
        assert_eq!(parsed["frequency"], 14_074_000);
        assert_eq!(parsed["mode"], "USB");
        assert!((parsed["power"].as_f64().unwrap() - 10.0).abs() < 1e-3);
        let ts = parsed["timestamp"]
            .as_u64()
            .unwrap_or_else(|| panic!("timestamp not a u64 in {parsed}"));
        // Sanity-check: epoch ms in 2020 onwards (>= 2020-01-01) and
        // not a Unix-second value mistakenly serialized as ms.
        assert!(ts >= 1_577_836_800_000, "timestamp {ts} too small");

        let _ = shutdown_tx.send(true);
        let _ = socket.close(None).await;
    }

    #[tokio::test]
    async fn broadcast_fans_out_to_multiple_subscribers() {
        let handle = WsHandle::new("FT-710".into(), 100.0);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (addr, _join) = spawn_server(handle.clone(), shutdown_rx).await;

        let (mut a, _) = tokio_tungstenite::connect_async(ws_request(addr, "https://wavelog.test"))
            .await
            .expect("client A connect");
        let (mut b, _) = tokio_tungstenite::connect_async(ws_request(addr, "https://wavelog.test"))
            .await
            .expect("client B connect");

        // Drain welcomes
        let _ = a.next().await.unwrap().unwrap();
        let _ = b.next().await.unwrap().unwrap();

        // broadcast::send only reaches subscribers that have already
        // subscribed at the moment of send. The first message after
        // `subscribe` is observable; spin briefly to give both ends a
        // chance to settle into recv state before firing.
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.broadcast(&dummy_state());

        for socket in [&mut a, &mut b] {
            let msg = tokio::time::timeout(Duration::from_secs(2), socket.next())
                .await
                .expect("fan-out timed out")
                .expect("stream closed")
                .expect("ws error");
            let body = match msg {
                TungMessage::Text(s) => s,
                other => panic!("unexpected: {other:?}"),
            };
            let parsed: serde_json::Value = serde_json::from_str(body.as_str()).unwrap();
            assert_eq!(parsed["type"], "radio_status");
        }

        let _ = shutdown_tx.send(true);
        let _ = a.close(None).await;
        let _ = b.close(None).await;
    }

    #[tokio::test]
    async fn shutdown_signal_closes_connected_clients() {
        let handle = WsHandle::new("FT-710".into(), 100.0);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (addr, join) = spawn_server(handle.clone(), shutdown_rx).await;

        let (mut socket, _) =
            tokio_tungstenite::connect_async(ws_request(addr, "https://wavelog.test"))
                .await
                .expect("connect");
        let _welcome = socket.next().await.unwrap().unwrap();

        // Signal shutdown — server sends Close and the per-connection
        // task exits, then axum::serve resolves.
        let _ = shutdown_tx.send(true);

        // Expect either a Close frame or stream-end within a generous
        // timeout. Tungstenite delivers Close as its own Message.
        let outcome = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match socket.next().await {
                    Some(Ok(TungMessage::Close(_))) | None => return Ok::<_, ()>(()),
                    Some(Ok(_)) => continue,
                    Some(Err(_)) => return Ok(()),
                }
            }
        })
        .await;
        assert!(outcome.is_ok(), "client did not observe close within 2s");

        // The serve loop should also resolve cleanly.
        let serve_result = tokio::time::timeout(Duration::from_secs(2), join).await;
        assert!(serve_result.is_ok(), "serve task did not exit within 2s");
    }

    #[test]
    fn broadcast_without_subscribers_is_a_noop() {
        let handle = WsHandle::new("FT-710".into(), 100.0);
        // No subscribers — must not panic, not error visibly.
        handle.broadcast(&dummy_state());
    }

    #[tokio::test]
    async fn broadcast_omits_power_when_state_power_is_none() {
        let handle = WsHandle::new("FT-710".into(), 100.0);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (addr, _join) = spawn_server(handle.clone(), shutdown_rx).await;

        let req = ws_request(addr, "https://wavelog.test");
        let (mut socket, _resp) = tokio_tungstenite::connect_async(req).await.unwrap();
        let _welcome = socket.next().await.unwrap().unwrap();

        handle.broadcast(&dummy_state_no_power());
        let msg = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let body = match msg {
            TungMessage::Text(s) => s,
            other => panic!("unexpected: {other:?}"),
        };
        let parsed: serde_json::Value = serde_json::from_str(body.as_str()).unwrap();
        assert_eq!(parsed["type"], "radio_status");
        assert!(
            parsed.get("power").is_none(),
            "power must be omitted when None: {parsed}",
        );

        let _ = shutdown_tx.send(true);
        let _ = socket.close(None).await;
    }
}
