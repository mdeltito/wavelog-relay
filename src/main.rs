use std::sync::Arc;

use anyhow::{Context, anyhow};
use clap::Parser;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;
use wavelog_relay::config::{Cli, Command, Config, StationsConfig};
use wavelog_relay::qso_queue::QsoQueue;
use wavelog_relay::wavelog::{Station, WavelogClient};
use wavelog_relay::ws::WsHandle;
use wavelog_relay::{listener, poller, rigctld, ws, wsjtx};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Stations) => run_stations(cli).await,
        None => run_daemon(cli).await,
    }
}

async fn run_stations(cli: Cli) -> anyhow::Result<()> {
    let config = StationsConfig::load(&cli)?;
    init_tracing("warn");

    let client = WavelogClient::new(&config.wavelog_url, &config.key)
        .context("failed to build wavelog client")?;
    let stations = client
        .list_stations()
        .await
        .context("failed to fetch Wavelog station list")?;
    if stations.is_empty() {
        return Err(anyhow!(
            "no stations are configured in Wavelog for this API key — \
             create one under Account → Station Locations"
        ));
    }
    print_station_table(&stations);
    Ok(())
}

fn print_station_table(stations: &[Station]) {
    let id_w = stations
        .iter()
        .map(|s| s.id.len())
        .max()
        .unwrap_or(2)
        .max(2);
    let name_w = stations
        .iter()
        .map(|s| s.name.len())
        .max()
        .unwrap_or(4)
        .max(4);
    let call_w = stations
        .iter()
        .map(|s| s.callsign.len())
        .max()
        .unwrap_or(8)
        .max(8);
    println!(
        "{:>id_w$}  {:<name_w$}  {:<call_w$}",
        "ID", "NAME", "CALLSIGN",
    );
    println!(
        "{:>id_w$}  {:<name_w$}  {:<call_w$}",
        "-".repeat(id_w),
        "-".repeat(name_w),
        "-".repeat(call_w),
    );
    for s in stations {
        println!(
            "{:>id_w$}  {:<name_w$}  {:<call_w$}",
            &*s.id, &*s.name, &*s.callsign,
        );
    }
}

async fn run_daemon(cli: Cli) -> anyhow::Result<()> {
    let config = Config::load(cli)?;
    init_tracing(&config.log_level);
    tracing::info!(
        rigctld = %config.rigctld_endpoint,
        wavelog = %config.wavelog_url,
        radio = %*config.radio,
        listen = %config.listen_addr,
        ws_listen = %config.ws_listen_addr,
        no_ws = config.no_ws,
        wsjtx = config.wsjtx,
        wsjtx_listen = %config.wsjtx_listen_addr,
        interval = ?config.poll_interval,
        rig_timeout = ?config.rig_timeout,
        "wavelog-relay starting"
    );

    // Bind all listeners up front so port conflicts exit non-zero before any task spawns.
    let tcp_listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .with_context(|| format!("failed to bind listener on {}", config.listen_addr))?;

    let ws_listener = if config.no_ws {
        None
    } else {
        Some(
            tokio::net::TcpListener::bind(config.ws_listen_addr)
                .await
                .with_context(|| {
                    format!("failed to bind ws server on {}", config.ws_listen_addr)
                })?,
        )
    };

    let wsjtx_socket = if config.wsjtx {
        Some(
            wsjtx::bind(config.wsjtx_listen_addr)
                .await
                .with_context(|| {
                    format!(
                        "failed to bind wsjtx listener on {}",
                        config.wsjtx_listen_addr
                    )
                })?,
        )
    } else {
        None
    };

    let (rig_handle, rig_join) = rigctld::spawn(config.rigctld_endpoint, config.rig_timeout);

    let client = WavelogClient::new(&config.wavelog_url, &config.key)
        .context("failed to build wavelog client")?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let ws_handle = WsHandle::new(config.radio.clone(), config.power_max_watts);

    let poller_task = tokio::spawn(poller::run(
        rig_handle.clone(),
        client.clone(),
        config.radio.clone(),
        config.power_max_watts,
        ws_handle.clone(),
        config.poll_interval,
        shutdown_rx.clone(),
    ));

    let listener_task = tokio::spawn(listener::serve(
        tcp_listener,
        rig_handle.clone(),
        config.wavelog_origin.clone(),
        config.mode_overrides,
        shutdown_rx.clone(),
    ));

    let ws_task = ws_listener.map(|listener| {
        tokio::spawn(ws::serve(
            listener,
            ws_handle.clone(),
            config.wavelog_origin,
            shutdown_rx.clone(),
        ))
    });

    // station_id is Some iff config.wsjtx (Config::merge enforces it).
    let wsjtx_tasks = match (wsjtx_socket, config.station_id) {
        (Some(socket), Some(station_id)) => {
            let (queue, replay) = QsoQueue::open(config.qso_queue_path.clone())
                .await
                .with_context(|| {
                    format!(
                        "failed to open QSO queue at {}",
                        config.qso_queue_path.display(),
                    )
                })?;
            tracing::info!(
                queue_path = %config.qso_queue_path.display(),
                replay_count = replay.len(),
                "wsjtx persistent queue opened",
            );
            Some(wsjtx::spawn(
                socket,
                client,
                station_id,
                Some(Arc::new(queue)),
                replay.into_vec(),
                shutdown_rx.clone(),
            ))
        },
        _ => None,
    };

    // Tasks hold the clones; dropping these here lets the rig actor and
    // WS broadcast sender shut down cleanly.
    drop(rig_handle);
    drop(ws_handle);
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
    if let Some(task) = ws_task {
        match task.await {
            Ok(Ok(())) => {},
            Ok(Err(e)) => tracing::error!(error = %e, "ws server returned an error"),
            Err(e) => tracing::error!(error = %e, "ws server task panicked"),
        }
    }
    if let Some((listener, worker)) = wsjtx_tasks {
        if let Err(e) = listener.await {
            tracing::error!(error = %e, "wsjtx listener task panicked");
        }
        if let Err(e) = worker.await {
            tracing::error!(error = %e, "wsjtx POST worker task panicked");
        }
    }

    // All RigHandle clones dropped; the actor exits on empty channel.
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
