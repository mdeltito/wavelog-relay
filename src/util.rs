//! Crate-internal helpers shared across modules. Not part of the
//! public API.

use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

/// True if shutdown has fired or the sender was dropped.
pub(crate) fn shutdown_observed(
    changed: Result<(), watch::error::RecvError>,
    shutdown: &watch::Receiver<bool>,
) -> bool {
    changed.is_err() || *shutdown.borrow()
}

/// Future for axum's `with_graceful_shutdown`: resolves on `true` or sender drop.
pub(crate) async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
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

/// Current Unix time in milliseconds. Returns 0 if the system clock is
/// before the epoch.
pub(crate) fn epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
