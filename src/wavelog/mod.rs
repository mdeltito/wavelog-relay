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
//!
//! [`StationSource`] layers a station-profile resolution policy on top
//! of the client: either an operator-pinned ID or a TTL-cached lookup
//! of Wavelog's currently active profile.

mod client;
mod station;

pub use client::{WavelogClient, WavelogError};
pub use station::{Station, StationSource};
