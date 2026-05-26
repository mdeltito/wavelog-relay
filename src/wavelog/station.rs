//! Station-profile resolution policy layered over [`WavelogClient`].
//!
//! [`StationSource`] is what the WSJT-X worker calls to obtain the
//! `station_profile_id` for an outbound QSO POST. Two variants:
//! [`StationSource::Fixed`] for an operator-pinned ID (no network
//! traffic) and [`StationSource::Active`] for a TTL-cached lookup of
//! whichever profile Wavelog currently flags `station_active=1`.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use super::client::{WavelogClient, WavelogError};

/// A Wavelog station-profile entry as returned by `/api/station_info`.
/// `id` is the value to pass as `station_profile_id` when submitting
/// QSOs to `/api/qso`. `active` reflects Wavelog's per-user
/// `station_active` flag. Exactly one profile per API-key owner is
/// active at a time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Station {
    pub id: Box<str>,
    pub name: Box<str>,
    pub callsign: Box<str>,
    pub active: bool,
}

/// Default TTL for [`ActiveStationCache`]. Chosen so that an operator
/// flipping the active station in the Wavelog UI sees the daemon route
/// QSOs to the new station within one cache window without a restart;
/// also small enough that the daemon doesn't pin onto a stale value if
/// they forget they changed it.
const DEFAULT_ACTIVE_TTL: Duration = Duration::from_mins(5);

/// Source of the `station_profile_id` for outbound QSO POSTs.
///
/// - [`StationSource::Fixed`] returns a pre-configured ID with no
///   network traffic — used when the operator passed `--station-id`.
/// - [`StationSource::Active`] looks the active station up via
///   `/api/station_info` on first use and caches the result for
///   [`DEFAULT_ACTIVE_TTL`]. Used when `--station-id` is unset.
///
/// `Clone` semantics differ by variant: cloning `Active` shares the
/// underlying cache via `Arc<Mutex>`, cloning `Fixed` allocates a
/// fresh owned ID.
#[derive(Clone)]
pub enum StationSource {
    Fixed(Box<str>),
    Active(ActiveStationCache),
}

impl StationSource {
    /// Construct an active-lookup source using the standard 60-second
    /// cache TTL.
    pub fn active(client: WavelogClient) -> Self {
        Self::Active(ActiveStationCache::new(client, DEFAULT_ACTIVE_TTL))
    }

    /// Resolve to a station ID. Returns the configured ID for
    /// [`StationSource::Fixed`]; for [`StationSource::Active`] consults
    /// the cache and refreshes via Wavelog on miss.
    pub async fn resolve(&self) -> Result<Box<str>, WavelogError> {
        match self {
            Self::Fixed(id) => Ok(id.clone()),
            Self::Active(cache) => cache.resolve().await,
        }
    }
}

impl fmt::Debug for StationSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fixed(id) => write!(f, "Fixed({id:?})"),
            Self::Active(cache) => match cache.state.try_lock() {
                Ok(guard) => match guard.as_ref() {
                    Some(cached) => write!(f, "Active(cached={:?})", &*cached.id),
                    None => f.write_str("Active(unresolved)"),
                },
                Err(_) => f.write_str("Active(refreshing)"),
            },
        }
    }
}

#[derive(Clone)]
pub struct ActiveStationCache {
    client: WavelogClient,
    state: Arc<Mutex<Option<CachedActive>>>,
    ttl: Duration,
}

#[derive(Clone)]
struct CachedActive {
    id: Box<str>,
    fetched_at: Instant,
}

impl ActiveStationCache {
    fn new(client: WavelogClient, ttl: Duration) -> Self {
        Self {
            client,
            state: Arc::new(Mutex::new(None)),
            ttl,
        }
    }

    async fn resolve(&self) -> Result<Box<str>, WavelogError> {
        // Hold the mutex across the fetch so only one in-flight lookup
        // runs at a time. WSJT-X QSOs are serial through the worker,
        // so contention here is a non-issue in practice.
        let mut state = self.state.lock().await;
        if let Some(cached) = state.as_ref()
            && cached.fetched_at.elapsed() < self.ttl
        {
            return Ok(cached.id.clone());
        }
        let id = self.client.find_active_station().await?;
        tracing::info!(station_id = %id, "resolved active wavelog station");
        *state = Some(CachedActive {
            id: id.clone(),
            fetched_at: Instant::now(),
        });
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::super::client::client_for;
    use super::*;

    fn station_info_mock_response(active_id: &str) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "station_id": "1",
                "station_profile_name": "Home",
                "station_callsign": "K1",
                "station_active": null,
            },
            {
                "station_id": active_id,
                "station_profile_name": "Portable",
                "station_callsign": "K1/P",
                "station_active": "1",
            }
        ]))
    }

    #[tokio::test]
    async fn fixed_station_source_returns_configured_id() {
        let src = StationSource::Fixed("42".into());
        let resolved = src.resolve().await.unwrap();
        assert_eq!(&*resolved, "42");
    }

    #[tokio::test]
    async fn active_station_cache_serves_cached_value_within_ttl() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/station_info/test-key"))
            .respond_with(station_info_mock_response("7"))
            .mount(&server)
            .await;
        let client = client_for(&server);
        let cache = ActiveStationCache::new(client, Duration::from_secs(300));

        let a = cache.resolve().await.unwrap();
        let b = cache.resolve().await.unwrap();
        assert_eq!(&*a, "7");
        assert_eq!(&*b, "7");
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            1,
            "second resolve within TTL must hit the cache, not the network",
        );
    }

    #[tokio::test]
    async fn active_station_cache_refreshes_after_ttl() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/station_info/test-key"))
            .respond_with(station_info_mock_response("7"))
            .mount(&server)
            .await;
        let client = client_for(&server);
        // Very short TTL — real time, so the second resolve falls outside the window.
        let cache = ActiveStationCache::new(client, Duration::from_millis(10));

        cache.resolve().await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        cache.resolve().await.unwrap();
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            2,
            "resolve after TTL expiry must re-fetch",
        );
    }

    #[tokio::test]
    async fn active_station_cache_does_not_poison_on_error() {
        let server = MockServer::start().await;
        // First call fails with NoActiveStation (no row is active),
        // second call returns a populated active row.
        Mock::given(method("GET"))
            .and(path("/api/station_info/test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                { "station_id": "1", "station_profile_name": "Home", "station_callsign": "K1", "station_active": null },
            ])))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/station_info/test-key"))
            .respond_with(station_info_mock_response("7"))
            .mount(&server)
            .await;
        let client = client_for(&server);
        let cache = ActiveStationCache::new(client, Duration::from_secs(300));

        let first = cache.resolve().await;
        assert!(
            matches!(first, Err(WavelogError::NoActiveStation)),
            "expected NoActiveStation, got {first:?}",
        );
        let second = cache.resolve().await.unwrap();
        assert_eq!(
            &*second, "7",
            "second resolve must re-fetch (error did not poison cache)",
        );
    }

    #[tokio::test]
    async fn active_station_cache_is_shared_across_clones() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/station_info/test-key"))
            .respond_with(station_info_mock_response("7"))
            .mount(&server)
            .await;
        let client = client_for(&server);
        let cache = ActiveStationCache::new(client, Duration::from_secs(300));
        let clone = cache.clone();

        cache.resolve().await.unwrap();
        clone.resolve().await.unwrap();
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            1,
            "cloned cache must share state with the original",
        );
    }
}
