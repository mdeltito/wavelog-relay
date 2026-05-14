use anyhow::Context;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;
use wavelog_bridge::config::Config;
use wavelog_bridge::wavelog::WavelogClient;
use wavelog_bridge::{listener, poller, rigctld};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_args()?;
    init_tracing(&config.log_level);
    tracing::info!(
        rigctld = %config.rigctld_endpoint,
        wavelog = %config.wavelog_url,
        radio = %*config.radio,
        listen = %config.listen_addr,
        interval = ?config.poll_interval,
        rig_timeout = ?config.rig_timeout,
        "wavelog-bridge starting"
    );

    // Bind the listener up front so port conflicts surface synchronously
    // and main can exit non-zero before any background tasks start.
    let tcp_listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .with_context(|| format!("failed to bind listener on {}", config.listen_addr))?;

    let (rig_handle, rig_join) = rigctld::spawn(config.rigctld_endpoint, config.rig_timeout);

    let client = WavelogClient::new(
        &config.wavelog_url,
        &config.key,
        &config.radio,
        config.power_max_watts,
    )
    .context("failed to build wavelog client")?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let poller_task = tokio::spawn(poller::run(
        rig_handle.clone(),
        client,
        config.poll_interval,
        shutdown_rx.clone(),
    ));

    let listener_task = tokio::spawn(listener::serve(
        tcp_listener,
        rig_handle.clone(),
        config.wavelog_origin,
        config.mode_overrides,
        shutdown_rx.clone(),
    ));

    // Drop the originals — the spawned tasks already hold their clones.
    // Keeping these alive would prevent the rig actor from exiting after
    // shutdown.
    drop(rig_handle);
    drop(shutdown_rx);

    wait_for_signal().await;
    tracing::info!("shutdown signal received");
    let _ = shutdown_tx.send(true);

    if let Err(e) = poller_task.await {
        tracing::error!(error = %e, "poller task panicked");
    }
    match listener_task.await {
        Ok(Ok(())) => {},
        Ok(Err(e)) => tracing::error!(error = %e, "listener returned an error"),
        Err(e) => tracing::error!(error = %e, "listener task panicked"),
    }

    // All RigHandle clones are now dropped; the actor will observe an
    // empty channel and exit on its own.
    let _ = rig_join.await;

    tracing::info!("shutdown complete");
    Ok(())
}

fn init_tracing(default_directive: &str) {
    let filter = std::env::var("RUST_LOG")
        .ok()
        .and_then(|s| EnvFilter::try_new(&s).ok())
        .or_else(|| EnvFilter::try_new(default_directive).ok())
        .unwrap_or_else(|| EnvFilter::new(DEFAULT_TRACING_FALLBACK));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

const DEFAULT_TRACING_FALLBACK: &str = "info";

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => tracing::debug!("SIGTERM received"),
        _ = sigint.recv() => tracing::debug!("SIGINT received"),
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
