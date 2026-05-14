use anyhow::{Context, anyhow};
use clap::Parser;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;
use wavelog_bridge::config::{Cli, Command, Config, StationsConfig};
use wavelog_bridge::wavelog::{Station, WavelogClient};
use wavelog_bridge::ws::WsBandmapHandle;
use wavelog_bridge::{listener, poller, rigctld, ws, wsjtx};

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
    // Bare-bones tracing for one-shots so any error path still produces
    // something readable on stderr.
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
    // Compute column widths so the output stays aligned even with
    // wide callsigns or names.
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
        "wavelog-bridge starting"
    );

    // Bind every listener socket up front so port conflicts surface
    // synchronously and main can exit non-zero before any background
    // tasks start.
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
                    format!("failed to bind ws bandmap on {}", config.ws_listen_addr)
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

    let ws_handle = WsBandmapHandle::new(config.radio.clone(), config.power_max_watts);

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

    // station_id is guaranteed Some when config.wsjtx is true — the
    // Config::merge check enforces it. Pair the socket with its
    // station_id, build a WSJT-X listener+worker tuple, or fall
    // through to no tasks.
    let wsjtx_tasks = match (wsjtx_socket, config.station_id) {
        (Some(socket), Some(station_id)) => Some(wsjtx::spawn(
            socket,
            client,
            station_id,
            shutdown_rx.clone(),
        )),
        _ => None,
    };

    // Drop the originals — the spawned tasks already hold their clones.
    // Keeping these alive would prevent the rig actor from exiting after
    // shutdown, and the ws handle from dropping its broadcast sender.
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
            Ok(Err(e)) => tracing::error!(error = %e, "ws bandmap returned an error"),
            Err(e) => tracing::error!(error = %e, "ws bandmap task panicked"),
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
