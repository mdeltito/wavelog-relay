//! Wavelog HTTP client.
//!
//! [`WavelogClient`] is stateless and [`Clone`] — safe to share
//! between the poller, the WSJT-X worker, and the one-shot subcommand.
//! Three endpoints:
//!
//! - [`push_radio`](WavelogClient::push_radio) — `POST /api/radio`,
//!   used by the poller for live rig-state updates. Caller owns any
//!   dedupe / heartbeat policy.
//! - [`push_qso`](WavelogClient::push_qso) — `POST /api/qso`. Wavelog
//!   signals success via JSON `status: "created"`; 2xx alone is not
//!   enough (duplicates and validation errors return 200 too).
//! - [`list_stations`](WavelogClient::list_stations) —
//!   `GET /api/station_info/<key>`, used by the `stations` subcommand.

use std::fmt;
use std::future::Future;
use std::time::Duration;

use reqwest::Url;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::rigctld::RigState;

const RETRY_SLEEPS: [Duration; 3] = [
    Duration::from_secs(0),
    Duration::from_secs(1),
    Duration::from_secs(4),
];
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const USER_AGENT: &str = concat!("wavelog-relay/", env!("CARGO_PKG_VERSION"));

#[derive(Clone)]
pub struct WavelogClient {
    http: reqwest::Client,
    /// Trimmed base URL, e.g. `https://wavelog.example.com/index.php`.
    /// No trailing slash; endpoints are derived per call.
    base_url: Box<str>,
    key: Box<str>,
}

impl fmt::Debug for WavelogClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WavelogClient")
            .field("base_url", &self.base_url)
            .field("key", &Redacted(&self.key))
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

/// A Wavelog station-profile entry as returned by `/api/station_info`.
/// `id` is the value to pass as `station_profile_id` when submitting
/// QSOs to `/api/qso`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Station {
    pub id: Box<str>,
    pub name: Box<str>,
    pub callsign: Box<str>,
}

#[derive(Debug, Error)]
pub enum WavelogError {
    #[error("invalid wavelog URL `{0}`")]
    InvalidUrl(Box<str>),

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("wavelog returned HTTP {status}: {body}")]
    Status { status: u16, body: Box<str> },

    /// 2xx with JSON `status` other than `"created"`. Not retryable:
    /// transport succeeded; only the logical operation failed.
    #[error("wavelog rejected the submission: {reason}")]
    Rejected { reason: Box<str> },

    #[error("wavelog response could not be parsed: {0}")]
    BadResponse(Box<str>),
}

impl WavelogClient {
    /// Construct a client for the given Wavelog base URL with the
    /// standard 5-second per-request timeout. The base URL's trailing
    /// slash is trimmed if present.
    pub fn new(base_url: &str, key: &str) -> Result<Self, WavelogError> {
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .build()?;
        Self::with_http(base_url, key, http)
    }

    fn with_http(base_url: &str, key: &str, http: reqwest::Client) -> Result<Self, WavelogError> {
        let trimmed = base_url.trim_end_matches('/');
        // Validate URL shape now so the daemon fails fast at startup
        // rather than per-request.
        let _ = build_url(trimmed, "radio")?;
        Ok(Self {
            http,
            base_url: trimmed.into(),
            key: key.into(),
        })
    }

    /// POST a radio-state snapshot to `/api/radio`. `power` is omitted
    /// when [`RigState::power`] is `None` so rigs without RFPOWER
    /// readback don't log fake wattage.
    pub async fn push_radio(
        &self,
        radio: &str,
        state: &RigState,
        power_max_watts: f32,
    ) -> Result<(), WavelogError> {
        let url = build_url(&self.base_url, "radio")?;
        let payload = RadioPayload {
            key: &self.key,
            radio,
            frequency: state.freq,
            mode: &state.mode,
            power: state.power.map(|p| p * power_max_watts),
        };
        with_retries("push_radio", || async {
            self.do_radio_post(&url, &payload).await
        })
        .await
    }

    async fn do_radio_post(
        &self,
        url: &Url,
        payload: &RadioPayload<'_>,
    ) -> Result<(), WavelogError> {
        let response = self.http.post(url.clone()).json(payload).send().await?;
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

    /// POST a logged QSO to `/api/qso` as `{ type: "adif", string:
    /// <adif> }`. Success requires both HTTP 2xx **and** a response
    /// JSON `status: "created"`; non-`created` 2xx responses surface
    /// as [`WavelogError::Rejected`].
    pub async fn push_qso(&self, station_id: &str, adif: &str) -> Result<(), WavelogError> {
        let url = build_url(&self.base_url, "qso")?;
        let payload = QsoPayload {
            key: &self.key,
            station_profile_id: station_id,
            kind: "adif",
            string: adif,
        };
        with_retries("push_qso", || async {
            self.do_qso_post(&url, &payload).await
        })
        .await
    }

    async fn do_qso_post(&self, url: &Url, payload: &QsoPayload<'_>) -> Result<(), WavelogError> {
        let response = self.http.post(url.clone()).json(payload).send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(WavelogError::Status {
                status: status.as_u16(),
                body: body.into(),
            });
        }
        let body = response.text().await.unwrap_or_default();
        let parsed: QsoResponse = serde_json::from_str(&body)
            .map_err(|e| WavelogError::BadResponse(format!("{e}: {body}").into()))?;
        if parsed.status.as_deref() == Some("created") {
            return Ok(());
        }
        let reason = parsed
            .reason
            .or(parsed.status)
            .unwrap_or_else(|| "unknown".to_owned());
        Err(WavelogError::Rejected {
            reason: reason.into(),
        })
    }

    /// Fetch the list of station profiles configured in Wavelog for
    /// the API key this client was constructed with. One-shot — no
    /// retries — used by the `stations` subcommand.
    pub async fn list_stations(&self) -> Result<Vec<Station>, WavelogError> {
        let url = build_station_info_url(&self.base_url, &self.key)?;
        let response = self.http.get(url).send().await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(WavelogError::Status {
                status: status.as_u16(),
                body: body.into(),
            });
        }
        let raw: Vec<StationInfoRow> = serde_json::from_str(&body)
            .map_err(|e| WavelogError::BadResponse(format!("{e}: {body}").into()))?;
        Ok(raw.into_iter().map(Station::from).collect())
    }
}

/// Single retry helper shared by every POST. The closure must return
/// a future that resolves to `Result<T, WavelogError>`; classification
/// is delegated to [`is_retryable`].
async fn with_retries<T, F, Fut>(label: &'static str, mut attempt: F) -> Result<T, WavelogError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, WavelogError>>,
{
    let mut last_err: Option<WavelogError> = None;
    for (idx, sleep) in RETRY_SLEEPS.iter().enumerate() {
        if !sleep.is_zero() {
            tokio::time::sleep(*sleep).await;
        }
        match attempt().await {
            Ok(v) => {
                tracing::debug!(label, attempt = idx + 1, "wavelog request sent");
                return Ok(v);
            },
            Err(e) if !is_retryable(&e) => {
                tracing::warn!(label, error = %e, "wavelog request failed (non-retryable)");
                return Err(e);
            },
            Err(e) => {
                tracing::warn!(
                    label,
                    attempt = idx + 1,
                    error = %e,
                    "wavelog request failed; will retry",
                );
                last_err = Some(e);
            },
        }
    }
    let err = last_err.expect("retry loop must have produced an error");
    tracing::warn!(label, error = %err, "wavelog request exhausted retries");
    Err(err)
}

fn build_url(base_url: &str, suffix: &str) -> Result<Url, WavelogError> {
    format!("{}/api/{suffix}", base_url.trim_end_matches('/'))
        .parse::<Url>()
        .map_err(|_| WavelogError::InvalidUrl(base_url.into()))
}

fn build_station_info_url(base_url: &str, key: &str) -> Result<Url, WavelogError> {
    let mut url = build_url(base_url, "station_info")?;
    url.path_segments_mut()
        .map_err(|_| WavelogError::InvalidUrl(base_url.into()))?
        .push(key);
    Ok(url)
}

#[derive(Serialize)]
struct RadioPayload<'a> {
    key: &'a str,
    radio: &'a str,
    frequency: u64,
    mode: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    power: Option<f32>,
}

#[derive(Serialize)]
struct QsoPayload<'a> {
    key: &'a str,
    station_profile_id: &'a str,
    #[serde(rename = "type")]
    kind: &'a str,
    string: &'a str,
}

#[derive(Deserialize)]
struct QsoResponse {
    status: Option<String>,
    reason: Option<String>,
}

#[derive(Deserialize)]
struct StationInfoRow {
    station_id: String,
    station_profile_name: String,
    station_callsign: String,
}

impl From<StationInfoRow> for Station {
    fn from(row: StationInfoRow) -> Self {
        Self {
            id: row.station_id.into(),
            name: row.station_profile_name.into(),
            callsign: row.station_callsign.into(),
        }
    }
}

fn is_retryable(err: &WavelogError) -> bool {
    match err {
        WavelogError::Http(_) => true,
        WavelogError::Status { status, .. } => *status >= 500,
        WavelogError::InvalidUrl(_)
        | WavelogError::Rejected { .. }
        | WavelogError::BadResponse(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

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

    /// Build a client without the production reqwest timeout. Tests
    /// that use `tokio::time::pause()` need this because tokio's
    /// auto-advance would otherwise fire the timer during the real
    /// wiremock round-trip and surface a spurious `TimedOut` error.
    fn client_for(server: &MockServer) -> WavelogClient {
        WavelogClient::with_http(&server.uri(), "test-key", reqwest::Client::new()).unwrap()
    }

    async fn radio_server_with_response(template: ResponseTemplate) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/radio"))
            .respond_with(template)
            .mount(&server)
            .await;
        server
    }

    fn qso_created_response() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "created",
            "reason": "",
        }))
    }

    #[test]
    fn redacted_debug_hides_key_body() {
        let client =
            WavelogClient::with_http("http://localhost", "supersecret", reqwest::Client::new())
                .unwrap();
        let dbg = format!("{client:?}");
        assert!(!dbg.contains("supersecret"), "key leaked: {dbg}");
        assert!(dbg.contains("****cret"));
    }

    #[test]
    fn redacted_debug_short_key_fully_masked() {
        let client =
            WavelogClient::with_http("http://localhost", "abc", reqwest::Client::new()).unwrap();
        let dbg = format!("{client:?}");
        assert!(!dbg.contains("abc"), "key leaked: {dbg}");
        assert!(dbg.contains("****"));
    }

    #[test]
    fn is_retryable_classifies_variants() {
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
        assert!(!is_retryable(&WavelogError::Rejected {
            reason: "dup".into()
        }));
        assert!(!is_retryable(&WavelogError::BadResponse("garbage".into())));
    }

    #[test]
    fn new_rejects_unparseable_url() {
        let err = WavelogClient::new("not a url", "k").unwrap_err();
        assert!(matches!(err, WavelogError::InvalidUrl(_)));
    }

    #[test]
    fn build_station_info_url_percent_encodes_key() {
        let url = build_station_info_url("https://wavelog.test", "key with spaces").unwrap();
        assert!(
            url.path()
                .ends_with("/api/station_info/key%20with%20spaces"),
            "got {}",
            url.path()
        );
    }

    #[tokio::test]
    async fn push_radio_posts_expected_json_body() {
        let server = radio_server_with_response(ResponseTemplate::new(200)).await;
        let client = client_for(&server);
        client
            .push_radio("FT-710", &rig_state(14_074_000, "USB", 0.1), 100.0)
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
        assert!(body.get("frequency_rx").is_none());
        assert!(body.get("mode_rx").is_none());
        assert!(body.get("timestamp").is_none());
    }

    #[tokio::test]
    async fn push_radio_omits_power_field_when_state_power_is_none() {
        let server = radio_server_with_response(ResponseTemplate::new(200)).await;
        let client = client_for(&server);
        client
            .push_radio("FT-710", &rig_state_no_power(14_074_000, "USB"), 100.0)
            .await
            .unwrap();

        let body: Value =
            serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
        assert!(
            body.get("power").is_none(),
            "power must be omitted when None: {body}",
        );
        // Other required fields still present.
        assert_eq!(body["frequency"], 14_074_000);
        assert_eq!(body["mode"], "USB");
    }

    #[tokio::test]
    async fn push_radio_scales_power_by_power_max_watts() {
        let server = radio_server_with_response(ResponseTemplate::new(200)).await;
        let client = client_for(&server);
        // power_max=50, fraction=0.2 -> 10W
        client
            .push_radio("FT-710", &rig_state(14_074_000, "USB", 0.2), 50.0)
            .await
            .unwrap();

        let body: Value =
            serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
        assert!((body["power"].as_f64().unwrap() - 10.0).abs() < 1e-3);
    }

    #[tokio::test(start_paused = true)]
    async fn push_radio_retries_on_5xx_then_succeeds() {
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
        let client = client_for(&server);
        client
            .push_radio("FT-710", &rig_state(14_074_000, "USB", 0.1), 100.0)
            .await
            .unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn push_radio_all_5xx_returns_error_after_three_attempts() {
        let server = radio_server_with_response(ResponseTemplate::new(503)).await;
        let client = client_for(&server);
        let err = client
            .push_radio("FT-710", &rig_state(14_074_000, "USB", 0.1), 100.0)
            .await
            .unwrap_err();
        assert!(
            matches!(err, WavelogError::Status { status: 503, .. }),
            "got {err:?}"
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn push_radio_does_not_retry_on_4xx() {
        let server = radio_server_with_response(ResponseTemplate::new(400)).await;
        let client = client_for(&server);
        let err = client
            .push_radio("FT-710", &rig_state(14_074_000, "USB", 0.1), 100.0)
            .await
            .unwrap_err();
        assert!(
            matches!(err, WavelogError::Status { status: 400, .. }),
            "got {err:?}"
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn push_qso_posts_adif_payload() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(qso_created_response())
            .mount(&server)
            .await;
        let client = client_for(&server);

        client
            .push_qso("3", "<CALL:3>K1B <MODE:3>FT8 <EOR>")
            .await
            .unwrap();

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["key"], "test-key");
        assert_eq!(body["station_profile_id"], "3");
        assert_eq!(body["type"], "adif");
        assert_eq!(body["string"], "<CALL:3>K1B <MODE:3>FT8 <EOR>");
    }

    #[tokio::test(start_paused = true)]
    async fn push_qso_treats_200_with_non_created_status_as_rejection() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "rejected",
                "reason": "duplicate qso",
            })))
            .mount(&server)
            .await;
        let client = client_for(&server);
        let err = client.push_qso("3", "<EOR>").await.unwrap_err();
        match err {
            WavelogError::Rejected { reason } => assert_eq!(&*reason, "duplicate qso"),
            other => panic!("expected Rejected, got {other:?}"),
        }
        // No retry on Rejected.
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn push_qso_retries_5xx_then_succeeds() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(qso_created_response())
            .mount(&server)
            .await;
        let client = client_for(&server);
        client.push_qso("3", "<EOR>").await.unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn push_qso_does_not_retry_on_4xx() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let client = client_for(&server);
        let err = client.push_qso("3", "<EOR>").await.unwrap_err();
        assert!(
            matches!(err, WavelogError::Status { status: 401, .. }),
            "got {err:?}"
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn push_qso_unparseable_response_is_bad_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;
        let client = client_for(&server);
        let err = client.push_qso("3", "<EOR>").await.unwrap_err();
        assert!(matches!(err, WavelogError::BadResponse(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn list_stations_parses_station_info_rows() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/station_info/test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {
                    "station_id": "1",
                    "station_profile_name": "Home",
                    "station_callsign": "K1AB",
                },
                {
                    "station_id": "2",
                    "station_profile_name": "Portable",
                    "station_callsign": "K1AB/P",
                }
            ])))
            .mount(&server)
            .await;
        let client = client_for(&server);
        let stations = client.list_stations().await.unwrap();
        assert_eq!(stations.len(), 2);
        assert_eq!(&*stations[0].id, "1");
        assert_eq!(&*stations[0].name, "Home");
        assert_eq!(&*stations[0].callsign, "K1AB");
        assert_eq!(&*stations[1].id, "2");
    }

    #[tokio::test]
    async fn list_stations_surfaces_http_error_as_status() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/station_info/test-key"))
            .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
            .mount(&server)
            .await;
        let client = client_for(&server);
        let err = client.list_stations().await.unwrap_err();
        assert!(
            matches!(err, WavelogError::Status { status: 403, .. }),
            "got {err:?}"
        );
    }
}
