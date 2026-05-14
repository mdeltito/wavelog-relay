//! Click-to-tune HTTP listener.
//!
//! Wavelog's frontend issues `fetch('<cat_url>/<freq_hz>/<mode>',
//! { method: 'GET' })` whenever a DX-cluster or bandmap spot is
//! clicked. This module wires that route to the rigctld actor: parse
//! the path, resolve the lowercase Wavelog mode to a hamlib name via
//! [`ModeOverrides`], and dispatch `F`/`M` commands through the
//! cloned [`RigHandle`].
//!
//! CORS is required (the browser issues this as a `cors`-mode `fetch`
//! and reads the response body); we allow exactly one origin — the
//! configured Wavelog base URL's origin — and rely on tower-http's
//! default `Vary: Origin`.

use std::io;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::modes::{Mode, ModeOverrides};
use crate::rigctld::RigHandle;

#[derive(Debug, Error)]
pub enum ListenerError {
    #[error("axum serve loop failed: {0}")]
    Serve(#[source] io::Error),
}

/// Serve the click-to-tune route on a pre-bound TCP listener until
/// `shutdown` resolves to `true` (or its sender drops). The caller is
/// responsible for the bind so port-in-use errors surface synchronously
/// at startup.
pub async fn serve(
    tcp_listener: TcpListener,
    rig: RigHandle,
    allow_origin: HeaderValue,
    overrides: ModeOverrides,
    shutdown: watch::Receiver<bool>,
) -> Result<(), ListenerError> {
    let app = build_router(rig, allow_origin, overrides);
    if let Ok(addr) = tcp_listener.local_addr() {
        tracing::info!(addr = %addr, "listener serving");
    }
    axum::serve(tcp_listener, app)
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await
        .map_err(ListenerError::Serve)?;
    tracing::info!("listener stopped");
    Ok(())
}

fn build_router(rig: RigHandle, allow_origin: HeaderValue, overrides: ModeOverrides) -> Router {
    let state = AppState { rig, overrides };
    // Predicate (not `AllowOrigin::exact`) so the listener only
    // advertises Access-Control-Allow-Origin when the request actually
    // came from the configured origin — `exact` would echo the
    // configured origin back even for unrelated requests.
    // Wavelog's current frontend issues a simple CORS GET (no
    // preflight). Advertising OPTIONS is a one-line defence against
    // future frontend changes that might add a header which forces a
    // preflight — without it the layer would refuse OPTIONS at the
    // CORS check rather than producing a usable preflight reply.
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(move |origin, _parts| {
            *origin == allow_origin
        }))
        .allow_methods([Method::GET, Method::OPTIONS]);
    Router::new()
        .route("/:freq/:mode", get(tune))
        .layer(cors)
        .with_state(state)
}

#[derive(Clone)]
struct AppState {
    rig: RigHandle,
    overrides: ModeOverrides,
}

async fn tune(
    State(state): State<AppState>,
    Path((freq_segment, mode_segment)): Path<(String, String)>,
) -> Response {
    let Ok(freq) = freq_segment.parse::<u64>() else {
        tracing::debug!(freq = %freq_segment, "click-to-tune: bad freq");
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Ok(mode) = mode_segment.parse::<Mode>() else {
        tracing::debug!(mode = %mode_segment, "click-to-tune: bad mode");
        return StatusCode::BAD_REQUEST.into_response();
    };
    let hamlib = mode.resolve(&state.overrides);

    if let Err(e) = state.rig.set_freq(freq).await {
        tracing::warn!(error = %e, freq, "click-to-tune: set_freq failed");
        return StatusCode::BAD_GATEWAY.into_response();
    }
    if let Err(e) = state.rig.set_mode(hamlib).await {
        tracing::warn!(error = %e, mode = hamlib.as_str(), "click-to-tune: set_mode failed");
        return StatusCode::BAD_GATEWAY.into_response();
    }

    tracing::info!(freq, mode = hamlib.as_str(), "click-to-tune");
    (StatusCode::OK, Body::empty()).into_response()
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
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::{Request, header};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    use super::*;
    use crate::modes::HamlibMode;
    use crate::rigctld;

    /// Mock rigctld that records every command line it receives. The
    /// test uses the captured `Vec<String>` to assert what the listener
    /// dispatched.
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
                });
            }
        });
        RecordingRigctld { addr, recorded }
    }

    fn wavelog_origin() -> HeaderValue {
        HeaderValue::from_static("https://wavelog.mdel.io")
    }

    fn get_request(uri: &str, origin: Option<&'static str>) -> Request<Body> {
        let mut b = Request::builder().method("GET").uri(uri);
        if let Some(o) = origin {
            b = b.header("Origin", o);
        }
        b.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn tune_dispatches_set_freq_then_set_mode() {
        let mock = spawn_recording_rigctld().await;
        let (rig, _join) = rigctld::spawn(mock.addr, Duration::from_secs(3));
        let app = build_router(rig, wavelog_origin(), ModeOverrides::default());

        let response = app
            .oneshot(get_request("/14074000/usb", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        assert_eq!(mock.commands(), vec!["F 14074000", "M USB -1"]);
    }

    #[tokio::test]
    async fn non_numeric_freq_returns_400_without_touching_rig() {
        let mock = spawn_recording_rigctld().await;
        let (rig, _join) = rigctld::spawn(mock.addr, Duration::from_secs(3));
        let app = build_router(rig, wavelog_origin(), ModeOverrides::default());

        let response = app.oneshot(get_request("/abc/usb", None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(mock.commands().is_empty());
    }

    #[tokio::test]
    async fn unknown_mode_returns_400_without_touching_rig() {
        let mock = spawn_recording_rigctld().await;
        let (rig, _join) = rigctld::spawn(mock.addr, Duration::from_secs(3));
        let app = build_router(rig, wavelog_origin(), ModeOverrides::default());

        let response = app
            .oneshot(get_request("/14074000/xyz", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(mock.commands().is_empty());
    }

    #[tokio::test]
    async fn matching_origin_gets_cors_allow_origin_header() {
        let mock = spawn_recording_rigctld().await;
        let (rig, _join) = rigctld::spawn(mock.addr, Duration::from_secs(3));
        let app = build_router(rig, wavelog_origin(), ModeOverrides::default());

        let response = app
            .oneshot(get_request(
                "/14074000/usb",
                Some("https://wavelog.mdel.io"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let header = response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .expect("missing CORS allow-origin header");
        assert_eq!(header, "https://wavelog.mdel.io");
    }

    #[tokio::test]
    async fn non_matching_origin_gets_no_cors_header() {
        let mock = spawn_recording_rigctld().await;
        let (rig, _join) = rigctld::spawn(mock.addr, Duration::from_secs(3));
        let app = build_router(rig, wavelog_origin(), ModeOverrides::default());

        let response = app
            .oneshot(get_request(
                "/14074000/usb",
                Some("https://evil.example.com"),
            ))
            .await
            .unwrap();
        // The request still succeeds at the HTTP layer — CORS is
        // enforced browser-side via the absence of the allow-origin
        // header. We assert that absence.
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none(),
            "expected no allow-origin header for non-matching origin",
        );
    }

    #[tokio::test]
    async fn request_without_origin_still_succeeds() {
        let mock = spawn_recording_rigctld().await;
        let (rig, _join) = rigctld::spawn(mock.addr, Duration::from_secs(3));
        let app = build_router(rig, wavelog_origin(), ModeOverrides::default());

        // No Origin header — simulates a `curl` invocation from the
        // host operator. Must still tune the rig.
        let response = app
            .oneshot(get_request("/14074000/usb", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(mock.commands(), vec!["F 14074000", "M USB -1"]);
    }

    #[tokio::test]
    async fn pkt_mode_resolves_through_overrides() {
        let mock = spawn_recording_rigctld().await;
        let (rig, _join) = rigctld::spawn(mock.addr, Duration::from_secs(3));
        let overrides = ModeOverrides {
            pkt: HamlibMode::PktLsb,
            dig: HamlibMode::PktUsb,
        };
        let app = build_router(rig, wavelog_origin(), overrides);

        let response = app
            .oneshot(get_request("/3573000/pkt", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(mock.commands(), vec!["F 3573000", "M PKTLSB -1"]);
    }

    #[tokio::test]
    async fn rig_disconnected_returns_502() {
        // No real rigctld on port 1 — the actor sits in backoff and
        // replies `Disconnected` to every command immediately.
        let (rig, _join) = rigctld::spawn(
            "127.0.0.1:1".parse::<SocketAddr>().unwrap(),
            Duration::from_secs(3),
        );
        let app = build_router(rig, wavelog_origin(), ModeOverrides::default());

        let response = app
            .oneshot(get_request("/14074000/usb", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }
}
