//! WebSocket bandmap server for Wavelog's frontend.
//!
//! Wavelog's `assets/js/cat.js` opens a WebSocket to
//! `ws://127.0.0.1:54322` (after trying `wss://...:54323` first) and
//! consumes `radio_status` frames to drive the live rig card. Without a
//! server on either port the frontend falls back to a 3 s AJAX poll of
//! `/api/radio` — usable but sluggish, especially while spinning the
//! VFO.
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
//! - **Inbound messages are accepted and discarded.** Wavelog sends
//!   `qso_logged` / `satellite_position` / `lookup_result` — forwarding
//!   those (UDP to N1MM, az/el to rotctld, etc.) is a separate product
//!   with its own config surface, not part of v1 bandmap.
//! - **Origin check is strict.** The WS handshake has no CORS; the
//!   server alone decides whether to accept it. We require the `Origin`
//!   header to equal the configured Wavelog URL's origin.

use std::io;
use std::sync::Arc;
use std::time::SystemTime;

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

const CHANNEL_CAPACITY: usize = 16;
const WELCOME_FRAME: &str = r#"{"type":"welcome"}"#;
const INBOUND_LOG_TRUNC: usize = 120;

#[derive(Debug, Error)]
pub enum WsBandmapError {
    #[error("axum serve loop failed: {0}")]
    Serve(#[source] io::Error),
}

/// Cloneable broadcast handle. The poller produces one
/// [`broadcast`](WsBandmapHandle::broadcast) call per tick; every
/// connected client receives the serialized `radio_status` frame.
#[derive(Clone)]
pub struct WsBandmapHandle {
    inner: Arc<HandleInner>,
}

struct HandleInner {
    tx: broadcast::Sender<Arc<str>>,
    radio: Box<str>,
    power_max_watts: f32,
}

impl WsBandmapHandle {
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
            power: state.power * self.inner.power_max_watts,
            timestamp: Iso8601(SystemTime::now()),
        };
        let json = match serde_json::to_string(&frame) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "ws bandmap: serialize radio_status");
                return;
            },
        };
        // `send` returns Err only when there are zero subscribers — the
        // normal idle state. Don't log on every tick.
        let _ = self.inner.tx.send(Arc::from(json));
    }

    fn subscribe(&self) -> broadcast::Receiver<Arc<str>> {
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
    power: f32,
    timestamp: Iso8601,
}

// Wrap SystemTime so we can serialize via humantime's Rfc3339 formatter
// without pulling in chrono. The frontend's "updated N minutes ago"
// display reads the `timestamp` field — exact precision doesn't matter,
// whole-seconds is plenty.
struct Iso8601(SystemTime);

impl Serialize for Iso8601 {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&humantime::format_rfc3339_seconds(self.0))
    }
}

/// Run the WebSocket bandmap server on a pre-bound TCP listener until
/// `shutdown` resolves to `true` (or its sender drops). Pre-binding
/// (same pattern as the HTTP listener) ensures `EADDRINUSE` on 54322
/// surfaces synchronously at startup.
pub async fn serve(
    tcp_listener: TcpListener,
    handle: WsBandmapHandle,
    allow_origin: HeaderValue,
    shutdown: watch::Receiver<bool>,
) -> Result<(), WsBandmapError> {
    if let Ok(addr) = tcp_listener.local_addr() {
        tracing::info!(addr = %addr, "ws bandmap serving");
    }
    let app = build_router(handle, allow_origin, shutdown.clone());
    axum::serve(tcp_listener, app)
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await
        .map_err(WsBandmapError::Serve)?;
    tracing::info!("ws bandmap stopped");
    Ok(())
}

fn build_router(
    handle: WsBandmapHandle,
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
    handle: WsBandmapHandle,
    allow_origin: HeaderValue,
    shutdown: watch::Receiver<bool>,
}

async fn ws_upgrade(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    // The WebSocket handshake doesn't go through tower-http's CORS
    // layer — there's no preflight, and the browser sends the upgrade
    // request as if it were same-origin. The server is solely
    // responsible for vetting Origin; otherwise any HTTPS origin could
    // open a connection to `ws://127.0.0.1:54322` and read live rig
    // state.
    let origin = headers.get(header::ORIGIN);
    if origin != Some(&state.allow_origin) {
        tracing::debug!(
            ?origin,
            "ws bandmap: rejecting handshake with mismatched origin"
        );
        return StatusCode::FORBIDDEN.into_response();
    }
    ws.on_upgrade(move |socket| handle_connection(socket, state.handle, state.shutdown))
}

async fn handle_connection(
    mut socket: WebSocket,
    handle: WsBandmapHandle,
    mut shutdown: watch::Receiver<bool>,
) {
    // Already shutting down? Close cleanly before we wire anything up.
    if *shutdown.borrow_and_update() {
        let _ = socket.send(Message::Close(None)).await;
        return;
    }

    let mut rx = handle.subscribe();

    // Welcome is optional per the frontend (it `return`s on it), but
    // sending it doubles as a smoke signal on `websocat`.
    if socket
        .send(Message::Text(WELCOME_FRAME.into()))
        .await
        .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            biased;

            result = shutdown.changed() => {
                let should_stop = result.is_err() || *shutdown.borrow();
                if should_stop {
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
                    // The frontend reconnects with progressive backoff,
                    // so dropping is preferable to forwarding stale
                    // state from before the lag window.
                    tracing::warn!(skipped = n, "ws bandmap: subscriber lagged, dropping connection");
                    let _ = socket.send(Message::Close(None)).await;
                    return;
                }
                Err(broadcast::error::RecvError::Closed) => return,
            },

            msg = socket.recv() => match msg {
                Some(Ok(Message::Text(s))) => {
                    let s_ref: &str = s.as_ref();
                    let trunc: String = s_ref.chars().take(INBOUND_LOG_TRUNC).collect();
                    tracing::debug!(text = %trunc, "ws bandmap: ignoring inbound text");
                }
                Some(Ok(Message::Binary(_))) => {
                    tracing::debug!("ws bandmap: ignoring inbound binary frame");
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {
                    // axum responds to Ping automatically.
                }
                Some(Ok(Message::Close(_))) | None => return,
                Some(Err(e)) => {
                    tracing::debug!(error = %e, "ws bandmap: socket recv error, closing");
                    return;
                }
            }
        }
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow_and_update() {
        return;
    }
    loop {
        if shutdown.changed().await.is_err() {
            return;
        }
        if *shutdown.borrow() {
            return;
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
            power: 0.1,
        }
    }

    /// Bind a serve loop on an ephemeral port and return its address.
    /// Caller is responsible for keeping the returned `JoinHandle` alive
    /// so the task doesn't abort.
    async fn spawn_server(
        handle: WsBandmapHandle,
        shutdown: watch::Receiver<bool>,
    ) -> (
        SocketAddr,
        tokio::task::JoinHandle<Result<(), WsBandmapError>>,
    ) {
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
        let handle = WsBandmapHandle::new("FT-710".into(), 100.0);
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
        let handle = WsBandmapHandle::new("FT-710".into(), 100.0);
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
        let handle = WsBandmapHandle::new("FT-710".into(), 100.0);
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
        assert!(
            parsed["timestamp"].as_str().is_some(),
            "missing timestamp in {parsed}"
        );

        let _ = shutdown_tx.send(true);
        let _ = socket.close(None).await;
    }

    #[tokio::test]
    async fn broadcast_fans_out_to_multiple_subscribers() {
        let handle = WsBandmapHandle::new("FT-710".into(), 100.0);
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
        let handle = WsBandmapHandle::new("FT-710".into(), 100.0);
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
        let handle = WsBandmapHandle::new("FT-710".into(), 100.0);
        // No subscribers — must not panic, not error visibly.
        handle.broadcast(&dummy_state());
    }
}
