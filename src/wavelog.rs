//! Wavelog `/api/radio` push client.
//!
//! [`WavelogClient::push`] handles the full cycle for a single
//! [`RigState`] sample: quantize the RFPOWER reading into 0.5 %-of-full-
//! scale bins (so the heartbeat path isn't drowned out on a QRP rig
//! with a small `--power-max`), check the `(freq, mode, rfpower_q)`
//! dedupe key against the last successful push (with a 30 s heartbeat
//! that forces a re-POST even when nothing changed), POST the JSON
//! payload with bounded retries (`[0 s, 1 s, 4 s]` before attempts
//! 1, 2, 3 on network/5xx errors; 4xx fails immediately), and update
//! the dedupe state only on a 2xx response so a transient Wavelog
//! outage doesn't silently swallow a real QSY.

use std::fmt;
use std::time::Duration;

use reqwest::Url;
use serde::Serialize;
use thiserror::Error;
use tokio::time::Instant;

use crate::rigctld::RigState;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const RETRY_SLEEPS: [Duration; 3] = [
    Duration::from_secs(0),
    Duration::from_secs(1),
    Duration::from_secs(4),
];
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const USER_AGENT: &str = concat!("wavelog-bridge/", env!("CARGO_PKG_VERSION"));

pub struct WavelogClient {
    http: reqwest::Client,
    endpoint: Url,
    key: Box<str>,
    radio: Box<str>,
    power_max_watts: f32,
    last_pushed: Option<DedupeKey>,
    last_push_time: Option<Instant>,
}

impl fmt::Debug for WavelogClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WavelogClient")
            .field("endpoint", &self.endpoint)
            .field("radio", &self.radio)
            .field("key", &Redacted(&self.key))
            .field("power_max_watts", &self.power_max_watts)
            .field("last_pushed", &self.last_pushed)
            .field("last_push_time", &self.last_push_time)
            .finish()
    }
}

struct Redacted<'a>(&'a str);

impl fmt::Debug for Redacted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.len() <= 4 {
            return f.write_str("\"****\"");
        }
        let tail = &self.0[self.0.len() - 4..];
        write!(f, "\"****{tail}\"")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DedupeKey {
    freq: u64,
    mode: Box<str>,
    rfpower_q: i32,
}

#[derive(Debug, Error)]
pub enum WavelogError {
    #[error("invalid wavelog URL `{0}`")]
    InvalidUrl(Box<str>),

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("wavelog returned HTTP {status}: {body}")]
    Status { status: u16, body: Box<str> },
}

impl WavelogClient {
    /// Construct a client targeting `<base_url>/api/radio` with a
    /// 5-second per-request timeout. The base URL's trailing slash is
    /// trimmed if present.
    pub fn new(
        base_url: &str,
        key: &str,
        radio: &str,
        power_max_watts: f32,
    ) -> Result<Self, WavelogError> {
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .build()?;
        Self::with_http(base_url, key, radio, power_max_watts, http)
    }

    fn with_http(
        base_url: &str,
        key: &str,
        radio: &str,
        power_max_watts: f32,
        http: reqwest::Client,
    ) -> Result<Self, WavelogError> {
        let endpoint = format!("{}/api/radio", base_url.trim_end_matches('/'))
            .parse::<Url>()
            .map_err(|_| WavelogError::InvalidUrl(base_url.into()))?;
        Ok(Self {
            http,
            endpoint,
            key: key.into(),
            radio: radio.into(),
            power_max_watts,
            last_pushed: None,
            last_push_time: None,
        })
    }

    /// Push a single rig-state sample. Returns `Ok(())` on a successful
    /// POST or a dedupe-skip; returns `Err` only when all retries are
    /// exhausted or the response is a non-retryable error (4xx).
    pub async fn push(&mut self, state: &RigState) -> Result<(), WavelogError> {
        let watts = state.power * self.power_max_watts;
        let rfpower_q = quantize_rfpower(state.power);
        let now = Instant::now();

        if self.should_skip(state, rfpower_q, now) {
            tracing::debug!(
                freq = state.freq,
                mode = %state.mode,
                "wavelog push deduped"
            );
            return Ok(());
        }

        let payload = PushPayload {
            key: &self.key,
            radio: &self.radio,
            frequency: state.freq,
            mode: &state.mode,
            power: watts,
        };
        self.send_with_retries(&payload).await?;

        self.last_pushed = Some(DedupeKey {
            freq: state.freq,
            mode: state.mode.clone(),
            rfpower_q,
        });
        self.last_push_time = Some(now);
        Ok(())
    }

    fn should_skip(&self, state: &RigState, rfpower_q: i32, now: Instant) -> bool {
        let (Some(last), Some(last_time)) = (&self.last_pushed, self.last_push_time) else {
            return false;
        };
        last.freq == state.freq
            && *last.mode == *state.mode
            && last.rfpower_q == rfpower_q
            && now.duration_since(last_time) < HEARTBEAT_INTERVAL
    }

    async fn send_with_retries(&self, payload: &PushPayload<'_>) -> Result<(), WavelogError> {
        let mut last_err: Option<WavelogError> = None;
        for (idx, sleep) in RETRY_SLEEPS.iter().enumerate() {
            if !sleep.is_zero() {
                tokio::time::sleep(*sleep).await;
            }
            match self.do_post(payload).await {
                Ok(()) => {
                    tracing::debug!(attempt = idx + 1, "wavelog push sent");
                    return Ok(());
                },
                Err(e) if !is_retryable(&e) => {
                    tracing::warn!(error = %e, "wavelog push failed (non-retryable)");
                    return Err(e);
                },
                Err(e) => {
                    tracing::warn!(
                        attempt = idx + 1,
                        error = %e,
                        "wavelog push failed; will retry",
                    );
                    last_err = Some(e);
                },
            }
        }
        let err = last_err.expect("retry loop must have produced an error");
        tracing::warn!(error = %err, "wavelog push exhausted retries");
        Err(err)
    }

    async fn do_post(&self, payload: &PushPayload<'_>) -> Result<(), WavelogError> {
        let response = self
            .http
            .post(self.endpoint.clone())
            .json(payload)
            .send()
            .await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response.text().await.unwrap_or_default();
        Err(WavelogError::Status {
            status: status.as_u16(),
            body: body.into(),
        })
    }
}

#[derive(Serialize)]
struct PushPayload<'a> {
    key: &'a str,
    radio: &'a str,
    frequency: u64,
    mode: &'a str,
    power: f32,
}

/// Quantize an RFPOWER reading (`0.0..=1.0`) into half-percent bins of
/// full scale. The dedupe key compares on this quantized value so a
/// continuously-drifting RFPOWER doesn't generate one POST per tick;
/// keeping the bin relative (rather than fixed at 0.5 W) means a
/// low-`power_max` rig still benefits from dedupe.
fn quantize_rfpower(rfpower: f32) -> i32 {
    (rfpower * 200.0).round() as i32
}

fn is_retryable(err: &WavelogError) -> bool {
    match err {
        WavelogError::Http(_) => true,
        WavelogError::Status { status, .. } => *status >= 500,
        WavelogError::InvalidUrl(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::Value;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn rig_state(freq: u64, mode: &str, power: f32) -> RigState {
        RigState {
            freq,
            mode: mode.into(),
            power,
        }
    }

    async fn server_with_response(template: ResponseTemplate) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/radio"))
            .respond_with(template)
            .mount(&server)
            .await;
        server
    }

    /// Build a client without the production reqwest timeout. Tests
    /// that use `tokio::time::pause()` need this because tokio's
    /// auto-advance would otherwise fire the timer during the real
    /// wiremock round-trip and surface a spurious `TimedOut` error.
    async fn client_for(server: &MockServer) -> WavelogClient {
        WavelogClient::with_http(
            &server.uri(),
            "test-key",
            "FT-710",
            100.0,
            reqwest::Client::new(),
        )
        .unwrap()
    }

    #[test]
    fn quantize_rfpower_rounds_to_half_percent_bins() {
        assert_eq!(quantize_rfpower(0.0), 0);
        assert_eq!(quantize_rfpower(0.10), 20); // 0.10 * 200 = 20
        assert_eq!(quantize_rfpower(0.102), 20); // 20.4 -> 20
        assert_eq!(quantize_rfpower(0.103), 21); // 20.6 -> 21
        assert_eq!(quantize_rfpower(0.105), 21); // 21.0 -> 21
        assert_eq!(quantize_rfpower(0.997), 199); // 199.4 -> 199
        assert_eq!(quantize_rfpower(0.998), 200); // 199.6 -> 200
        assert_eq!(quantize_rfpower(1.0), 200);
    }

    #[test]
    fn quantize_is_independent_of_power_max() {
        // A 5W QRP rig (power_max=5) and a 100W rig (power_max=100)
        // both quantize the same RFPOWER fraction identically — that's
        // the point of operating on the raw fraction rather than watts.
        assert_eq!(quantize_rfpower(0.5), 100);
    }

    #[tokio::test(start_paused = true)]
    async fn qrp_rig_is_not_dedupe_starved_by_small_power_max() {
        // Regression test: with the old per-watt quantization, a 5W rig
        // saw 0.5W bins = 10% of full scale, so any sub-10% RFPOWER
        // swing collapsed to one bin and dedupe ate the heartbeat.
        // With per-RFPOWER bins, 0.10 vs 0.20 RFPOWER (10% delta)
        // still crosses ~20 bins regardless of power_max.
        let server = server_with_response(ResponseTemplate::new(200)).await;
        let mut client =
            WavelogClient::with_http(&server.uri(), "k", "QRP", 5.0, reqwest::Client::new())
                .unwrap();
        client
            .push(&rig_state(14_074_000, "USB", 0.10))
            .await
            .unwrap();
        client
            .push(&rig_state(14_074_000, "USB", 0.20))
            .await
            .unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[test]
    fn redacted_debug_hides_key_body() {
        let client = WavelogClient::with_http(
            "http://localhost",
            "supersecret",
            "r",
            100.0,
            reqwest::Client::new(),
        )
        .unwrap();
        let dbg = format!("{client:?}");
        assert!(
            !dbg.contains("supersecret"),
            "raw key leaked into Debug: {dbg}"
        );
        assert!(dbg.contains("****cret"), "missing redacted tail in: {dbg}");
    }

    #[test]
    fn redacted_debug_short_key_fully_masked() {
        let client = WavelogClient::with_http(
            "http://localhost",
            "abc",
            "r",
            100.0,
            reqwest::Client::new(),
        )
        .unwrap();
        let dbg = format!("{client:?}");
        assert!(!dbg.contains("abc"), "short key leaked: {dbg}");
        assert!(dbg.contains("****"));
    }

    #[test]
    fn is_retryable_classifies_status_codes() {
        assert!(is_retryable(&WavelogError::Status {
            status: 500,
            body: "".into()
        }));
        assert!(is_retryable(&WavelogError::Status {
            status: 503,
            body: "".into()
        }));
        assert!(!is_retryable(&WavelogError::Status {
            status: 400,
            body: "".into()
        }));
        assert!(!is_retryable(&WavelogError::Status {
            status: 401,
            body: "".into()
        }));
        assert!(!is_retryable(&WavelogError::InvalidUrl("".into())));
    }

    #[test]
    fn new_rejects_unparseable_url() {
        let err = WavelogClient::new("not a url", "k", "r", 100.0).unwrap_err();
        assert!(matches!(err, WavelogError::InvalidUrl(_)));
    }

    #[tokio::test]
    async fn json_body_has_required_fields_and_omits_split_fields() {
        let server = server_with_response(ResponseTemplate::new(200)).await;
        let mut client = client_for(&server).await;
        client
            .push(&rig_state(14_074_000, "USB", 0.1))
            .await
            .unwrap();

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["key"], "test-key");
        assert_eq!(body["radio"], "FT-710");
        assert_eq!(body["frequency"], 14_074_000);
        assert_eq!(body["mode"], "USB");
        assert!((body["power"].as_f64().unwrap() - 10.0).abs() < 1e-3);
        assert!(body.get("frequency_rx").is_none(), "no split fields in v1");
        assert!(body.get("mode_rx").is_none(), "no split fields in v1");
        assert!(body.get("timestamp").is_none(), "no timestamp in v1");
    }

    #[tokio::test]
    async fn power_conversion_scales_by_power_max_watts() {
        let server = server_with_response(ResponseTemplate::new(200)).await;
        // power_max = 50 W: RFPOWER 0.2 -> 10.0 W
        let mut client = WavelogClient::new(&server.uri(), "k", "FT-710", 50.0).unwrap();
        client
            .push(&rig_state(14_074_000, "USB", 0.2))
            .await
            .unwrap();

        let body: Value =
            serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
        assert!((body["power"].as_f64().unwrap() - 10.0).abs() < 1e-3);
    }

    #[tokio::test(start_paused = true)]
    async fn small_rfpower_fluctuation_quantizes_into_dedupe() {
        let server = server_with_response(ResponseTemplate::new(200)).await;
        let mut client = client_for(&server).await;
        // 0.100 -> 10.0 W -> 20 half-W. 0.101 -> 10.1 W -> 20.2 -> 20 half-W.
        client
            .push(&rig_state(14_074_000, "USB", 0.100))
            .await
            .unwrap();
        client
            .push(&rig_state(14_074_000, "USB", 0.101))
            .await
            .unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn power_change_beyond_quantum_sends_new_post() {
        let server = server_with_response(ResponseTemplate::new(200)).await;
        let mut client = client_for(&server).await;
        client
            .push(&rig_state(14_074_000, "USB", 0.10))
            .await
            .unwrap();
        client
            .push(&rig_state(14_074_000, "USB", 0.15))
            .await
            .unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn freq_change_breaks_dedupe() {
        let server = server_with_response(ResponseTemplate::new(200)).await;
        let mut client = client_for(&server).await;
        client
            .push(&rig_state(14_074_000, "USB", 0.1))
            .await
            .unwrap();
        client
            .push(&rig_state(14_100_000, "USB", 0.1))
            .await
            .unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn mode_change_breaks_dedupe() {
        let server = server_with_response(ResponseTemplate::new(200)).await;
        let mut client = client_for(&server).await;
        client
            .push(&rig_state(14_074_000, "USB", 0.1))
            .await
            .unwrap();
        client
            .push(&rig_state(14_074_000, "CW", 0.1))
            .await
            .unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_after_30s_resends_unchanged_state() {
        let server = server_with_response(ResponseTemplate::new(200)).await;
        let mut client = client_for(&server).await;
        let state = rig_state(14_074_000, "USB", 0.1);

        client.push(&state).await.unwrap();
        tokio::time::advance(Duration::from_secs(31)).await;
        client.push(&state).await.unwrap();

        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn unchanged_state_within_30s_is_skipped() {
        let server = server_with_response(ResponseTemplate::new(200)).await;
        let mut client = client_for(&server).await;
        let state = rig_state(14_074_000, "USB", 0.1);

        client.push(&state).await.unwrap();
        tokio::time::advance(Duration::from_secs(10)).await;
        client.push(&state).await.unwrap();

        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_on_5xx_then_succeeds() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/radio"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/radio"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let mut client = client_for(&server).await;
        client
            .push(&rig_state(14_074_000, "USB", 0.1))
            .await
            .unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn all_5xx_returns_error_after_three_attempts() {
        let server = server_with_response(ResponseTemplate::new(503)).await;
        let mut client = client_for(&server).await;
        let err = client
            .push(&rig_state(14_074_000, "USB", 0.1))
            .await
            .unwrap_err();
        assert!(
            matches!(err, WavelogError::Status { status: 503, .. }),
            "got {err:?}"
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn does_not_retry_on_4xx() {
        let server = server_with_response(ResponseTemplate::new(400)).await;
        let mut client = client_for(&server).await;
        let err = client
            .push(&rig_state(14_074_000, "USB", 0.1))
            .await
            .unwrap_err();
        assert!(
            matches!(err, WavelogError::Status { status: 400, .. }),
            "got {err:?}"
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn dedupe_state_persists_only_after_successful_push() {
        let server = MockServer::start().await;
        // First push: all three attempts get 500.
        Mock::given(method("POST"))
            .and(path("/api/radio"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(3)
            .mount(&server)
            .await;
        // Subsequent requests succeed.
        Mock::given(method("POST"))
            .and(path("/api/radio"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let mut client = client_for(&server).await;
        let state = rig_state(14_074_000, "USB", 0.1);

        // First push: 3 attempts all fail.
        client.push(&state).await.unwrap_err();
        // Same state again — must NOT be deduped because the previous
        // push never succeeded. Should hit the 200 mock on the next try.
        client.push(&state).await.unwrap();

        assert_eq!(server.received_requests().await.unwrap().len(), 4);
    }
}
