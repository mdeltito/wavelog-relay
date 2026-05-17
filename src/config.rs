//! CLI + TOML + env configuration.
//!
//! Precedence at merge time: CLI > env (`WAVELOG_RELAY_*`) > TOML > defaults.
//! The optional TOML file is auto-discovered at
//! `$XDG_CONFIG_HOME/wavelog-relay/config.toml` (or `$HOME/.config/...`)
//! unless `--config` overrides the path.
//!
//! The Wavelog API key is resolved separately from the rest of the
//! settings to keep it off `argv` (where it would show up in `ps`):
//! `WAVELOG_RELAY_KEY` (raw value) wins over a key file path supplied
//! through `--key-file` or `WAVELOG_RELAY_KEY_FILE`.

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::http::HeaderValue;
use clap::{Parser, Subcommand};
use serde::Deserialize;
use thiserror::Error;

use crate::modes::ModeOverrides;
use crate::rigctld::Endpoint;

const DEFAULT_RIGCTLD_ADDR: &str = "127.0.0.1:4532";
const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:54321";
const DEFAULT_WS_LISTEN_ADDR: &str = "127.0.0.1:54322";
const DEFAULT_WSJTX_LISTEN_ADDR: &str = "127.0.0.1:2237";
const DEFAULT_POWER_MAX_WATTS: f32 = 100.0;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_RIG_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_LOG_LEVEL: &str = "info";

#[derive(Debug, Default, Parser)]
#[command(version, about = "Relay between rigctld and Wavelog")]
pub struct Cli {
    /// Optional subcommand. When omitted, runs the daemon.
    #[command(subcommand)]
    pub command: Option<Command>,

    /// rigctld host:port. Accepts IPv4/IPv6 socket addresses
    /// (`127.0.0.1:4532`, `[::1]:4532`) or hostnames (`rig.local:4532`).
    /// Default 127.0.0.1:4532.
    #[arg(long, env = "WAVELOG_RELAY_RIGCTLD")]
    pub rigctld: Option<Endpoint>,

    /// Wavelog base URL (e.g. https://wavelog.example.com/index.php).
    #[arg(long, env = "WAVELOG_RELAY_WAVELOG_URL", global = true)]
    pub wavelog_url: Option<String>,

    /// Radio identifier sent to Wavelog (e.g. FT-710).
    #[arg(long, env = "WAVELOG_RELAY_RADIO")]
    pub radio: Option<String>,

    /// Path to a file containing the Wavelog API key.
    #[arg(long, env = "WAVELOG_RELAY_KEY_FILE", global = true)]
    pub key_file: Option<PathBuf>,

    /// Rig's max RF power in watts, used to scale rigctld's 0.0..=1.0
    /// RFPOWER reading. Default 100.
    #[arg(long, env = "WAVELOG_RELAY_POWER_MAX")]
    pub power_max: Option<f32>,

    /// Listener bind address. Default 127.0.0.1:54321.
    #[arg(long, env = "WAVELOG_RELAY_LISTEN")]
    pub listen: Option<SocketAddr>,

    /// WebSocket bind address. The Wavelog frontend
    /// (`assets/js/cat.js`) hardcodes a fallback to `ws://127.0.0.1:54322`,
    /// so changing this is only useful for local testing or to avoid
    /// a port conflict before fronting with a reverse proxy.
    /// Default 127.0.0.1:54322.
    #[arg(long, env = "WAVELOG_RELAY_WS_LISTEN")]
    pub ws_listen: Option<SocketAddr>,

    /// Disable the WebSocket server entirely (no bind on
    /// `--ws-listen`). The Wavelog frontend will fall back to its 3 s
    /// AJAX poll for rig-card updates.
    #[arg(long, env = "WAVELOG_RELAY_NO_WS")]
    pub no_ws: bool,

    /// Enable the WSJT-X UDP listener. Off by default — binds a UDP
    /// socket and forwards each `Logged ADIF` (type 12) datagram to
    /// Wavelog's `/api/qso`. Requires `--station-id` to be set as well
    /// (look one up with `wavelog-relay stations`).
    #[arg(long, env = "WAVELOG_RELAY_WSJTX")]
    pub wsjtx: bool,

    /// Bind address for the WSJT-X UDP listener. Honored only when
    /// `--wsjtx` is set. Default 127.0.0.1:2237 — matches the WSJT-X
    /// "UDP Server" Reporting setting.
    #[arg(long, env = "WAVELOG_RELAY_WSJTX_LISTEN")]
    pub wsjtx_listen: Option<SocketAddr>,

    /// Wavelog station profile ID for QSO submissions (the numeric
    /// `station_id` from `/api/station_info`). Required when
    /// `--wsjtx` is set. Use `wavelog-relay stations` to look up
    /// the IDs.
    #[arg(long, env = "WAVELOG_RELAY_STATION_ID")]
    pub station_id: Option<String>,

    /// Path to the persistent QSO queue (JSONL). Honored only when
    /// `--wsjtx` is set. Defaults to
    /// `$XDG_STATE_HOME/wavelog-relay/qso_queue.jsonl` (or
    /// `$HOME/.local/state/wavelog-relay/qso_queue.jsonl`). The file
    /// is created if absent.
    #[arg(long, env = "WAVELOG_RELAY_QSO_QUEUE_PATH")]
    pub qso_queue_path: Option<PathBuf>,

    /// Poll interval (e.g. 1s, 500ms). Default 1s.
    #[arg(long, env = "WAVELOG_RELAY_INTERVAL", value_parser = parse_duration)]
    pub interval: Option<Duration>,

    /// Per-command read timeout when talking to rigctld. If rigctld
    /// accepts a command but never replies within this window the
    /// connection is dropped and the actor reconnects via the standard
    /// backoff schedule. Default 3s.
    #[arg(long, env = "WAVELOG_RELAY_RIG_TIMEOUT", value_parser = parse_duration)]
    pub rig_timeout: Option<Duration>,

    /// Optional TOML config file. Auto-discovered if not given.
    #[arg(long, env = "WAVELOG_RELAY_CONFIG", global = true)]
    pub config: Option<PathBuf>,

    /// Default tracing filter directive. RUST_LOG overrides.
    #[arg(long, env = "WAVELOG_RELAY_LOG_LEVEL")]
    pub log_level: Option<String>,
}

fn parse_duration(s: &str) -> Result<Duration, humantime::DurationError> {
    humantime::parse_duration(s)
}

/// Subcommands that bypass the daemon and exit after running.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// List Wavelog station profile IDs (one-shot). Useful for
    /// looking up the `--station-id` value to pass to the daemon.
    Stations,
}

/// Minimal credentials needed by one-shot subcommands. Reuses the
/// daemon's TOML auto-discovery and key resolution so users don't need
/// a second mental model for `wavelog-relay stations`.
#[derive(Debug)]
pub struct StationsConfig {
    pub wavelog_url: Box<str>,
    pub key: Box<str>,
}

impl StationsConfig {
    pub fn load(cli: &Cli) -> Result<Self, ConfigError> {
        let toml = load_toml(cli.config.as_deref())?;
        let key = resolve_key(cli.key_file.as_deref())?;
        let wavelog_url = cli
            .wavelog_url
            .clone()
            .or(toml.wavelog_url)
            .ok_or(ConfigError::MissingWavelogUrl)?;
        // Validate the URL shape — same gate the daemon applies.
        let _ = parse_origin(&wavelog_url)?;
        Ok(Self {
            wavelog_url: wavelog_url.into(),
            key: key.into(),
        })
    }
}

/// Layered fields that may appear in the optional TOML config file.
/// Field names mirror the CLI flags (kebab-case → snake_case).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlConfig {
    rigctld: Option<Endpoint>,
    wavelog_url: Option<String>,
    radio: Option<String>,
    power_max: Option<f32>,
    listen: Option<SocketAddr>,
    ws_listen: Option<SocketAddr>,
    no_ws: Option<bool>,
    wsjtx: Option<bool>,
    wsjtx_listen: Option<SocketAddr>,
    station_id: Option<String>,
    qso_queue_path: Option<PathBuf>,
    #[serde(default, with = "humantime_serde::option")]
    interval: Option<Duration>,
    #[serde(default, with = "humantime_serde::option")]
    rig_timeout: Option<Duration>,
    log_level: Option<String>,
    #[serde(default)]
    mode_overrides: ModeOverrides,
}

/// Fully-resolved runtime configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub rigctld_endpoint: Endpoint,
    pub rig_timeout: Duration,
    pub wavelog_url: Box<str>,
    pub wavelog_origin: HeaderValue,
    pub radio: Box<str>,
    pub key: Box<str>,
    pub power_max_watts: f32,
    pub listen_addr: SocketAddr,
    pub ws_listen_addr: SocketAddr,
    pub no_ws: bool,
    pub wsjtx: bool,
    pub wsjtx_listen_addr: SocketAddr,
    /// Wavelog station profile ID for QSO submissions. `Some` iff
    /// [`Self::wsjtx`] is true — [`Config::merge`] enforces this:
    /// enabling WSJT-X without a station_id is a configuration error.
    pub station_id: Option<Box<str>>,
    /// Filesystem path for the persistent QSO queue. Always populated
    /// even when [`Self::wsjtx`] is false — the daemon only opens the
    /// file when wsjtx is enabled, so the path being unused is fine.
    pub qso_queue_path: PathBuf,
    pub poll_interval: Duration,
    pub log_level: Box<str>,
    pub mode_overrides: ModeOverrides,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required option: set --wavelog-url or WAVELOG_RELAY_WAVELOG_URL")]
    MissingWavelogUrl,

    #[error("missing required option: set --radio or WAVELOG_RELAY_RADIO")]
    MissingRadio,

    #[error("missing API key: set WAVELOG_RELAY_KEY (raw) or WAVELOG_RELAY_KEY_FILE / --key-file")]
    MissingKey,

    #[error(
        "--wsjtx requires --station-id: run `wavelog-relay stations` to look up your \
         Wavelog station profile ID, then pass it via --station-id or WAVELOG_RELAY_STATION_ID"
    )]
    MissingStationId,

    #[error("invalid wavelog URL `{0}`")]
    InvalidWavelogUrl(Box<str>),

    #[error("--power-max must be > 0, got {0}")]
    InvalidPowerMax(f32),

    #[error("--interval must be > 0")]
    InvalidInterval,

    #[error("--rig-timeout must be > 0")]
    InvalidRigTimeout,

    #[error("config file not found: {}", _0.display())]
    ConfigNotFound(Box<Path>),

    #[error("config file is not valid UTF-8: {}", _0.display())]
    ConfigInvalidUtf8(Box<Path>),

    #[error("failed to read config {}: {source}", path.display())]
    ConfigRead {
        path: Box<Path>,
        #[source]
        source: io::Error,
    },

    #[error("failed to parse config {}: {message}", path.display())]
    ConfigParse { path: Box<Path>, message: Box<str> },

    #[error("failed to read key file {}: {source}", path.display())]
    KeyFileRead {
        path: Box<Path>,
        #[source]
        source: io::Error,
    },

    #[error("key file is not valid UTF-8: {}", _0.display())]
    KeyFileInvalidUtf8(Box<Path>),

    #[error("key file is empty: {}", _0.display())]
    EmptyKeyFile(Box<Path>),
}

impl Config {
    /// Parse argv (exits the process on `--help`/`--version` or invalid
    /// CLI input via clap's standard behavior), then resolve TOML + env
    /// + defaults into a final [`Config`].
    pub fn from_args() -> Result<Self, ConfigError> {
        Self::load(Cli::parse())
    }

    pub fn load(cli: Cli) -> Result<Self, ConfigError> {
        let toml = load_toml(cli.config.as_deref())?;
        let key = resolve_key(cli.key_file.as_deref())?;
        Self::merge(cli, toml, key)
    }

    fn merge(cli: Cli, toml: TomlConfig, key: String) -> Result<Self, ConfigError> {
        let rigctld_endpoint = cli.rigctld.or(toml.rigctld).unwrap_or_else(|| {
            DEFAULT_RIGCTLD_ADDR
                .parse()
                .expect("hardcoded default valid")
        });

        let wavelog_url = cli
            .wavelog_url
            .or(toml.wavelog_url)
            .ok_or(ConfigError::MissingWavelogUrl)?;
        let wavelog_origin = parse_origin(&wavelog_url)?;

        let radio = cli.radio.or(toml.radio).ok_or(ConfigError::MissingRadio)?;

        let power_max_watts = cli
            .power_max
            .or(toml.power_max)
            .unwrap_or(DEFAULT_POWER_MAX_WATTS);
        if power_max_watts <= 0.0 || !power_max_watts.is_finite() {
            return Err(ConfigError::InvalidPowerMax(power_max_watts));
        }

        let listen_addr = cli.listen.or(toml.listen).unwrap_or_else(|| {
            DEFAULT_LISTEN_ADDR
                .parse()
                .expect("hardcoded default valid")
        });

        let ws_listen_addr = cli.ws_listen.or(toml.ws_listen).unwrap_or_else(|| {
            DEFAULT_WS_LISTEN_ADDR
                .parse()
                .expect("hardcoded default valid")
        });

        let no_ws = cli.no_ws || toml.no_ws.unwrap_or(false);
        let wsjtx = cli.wsjtx || toml.wsjtx.unwrap_or(false);

        let wsjtx_listen_addr = cli.wsjtx_listen.or(toml.wsjtx_listen).unwrap_or_else(|| {
            DEFAULT_WSJTX_LISTEN_ADDR
                .parse()
                .expect("hardcoded default valid")
        });

        let station_id: Option<Box<str>> = cli.station_id.or(toml.station_id).map(Into::into);

        if wsjtx && station_id.is_none() {
            return Err(ConfigError::MissingStationId);
        }

        let qso_queue_path = cli
            .qso_queue_path
            .or(toml.qso_queue_path)
            .unwrap_or_else(default_qso_queue_path);

        let poll_interval = cli
            .interval
            .or(toml.interval)
            .unwrap_or(DEFAULT_POLL_INTERVAL);
        if poll_interval.is_zero() {
            return Err(ConfigError::InvalidInterval);
        }

        let rig_timeout = cli
            .rig_timeout
            .or(toml.rig_timeout)
            .unwrap_or(DEFAULT_RIG_TIMEOUT);
        if rig_timeout.is_zero() {
            return Err(ConfigError::InvalidRigTimeout);
        }

        let log_level = cli
            .log_level
            .or(toml.log_level)
            .unwrap_or_else(|| DEFAULT_LOG_LEVEL.to_owned());

        Ok(Self {
            rigctld_endpoint,
            rig_timeout,
            wavelog_url: wavelog_url.into(),
            wavelog_origin,
            radio: radio.into(),
            key: key.into(),
            power_max_watts,
            listen_addr,
            ws_listen_addr,
            no_ws,
            wsjtx,
            wsjtx_listen_addr,
            station_id,
            qso_queue_path,
            poll_interval,
            log_level: log_level.into(),
            mode_overrides: toml.mode_overrides,
        })
    }
}

fn parse_origin(wavelog_url: &str) -> Result<HeaderValue, ConfigError> {
    let url = reqwest::Url::parse(wavelog_url)
        .map_err(|_| ConfigError::InvalidWavelogUrl(wavelog_url.into()))?;
    let origin = url.origin();
    if !origin.is_tuple() {
        return Err(ConfigError::InvalidWavelogUrl(wavelog_url.into()));
    }
    HeaderValue::from_str(&origin.ascii_serialization())
        .map_err(|_| ConfigError::InvalidWavelogUrl(wavelog_url.into()))
}

fn default_config_path() -> Option<PathBuf> {
    let dir = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))?;
    Some(dir.join("wavelog-relay").join("config.toml"))
}

/// XDG state path: `$XDG_STATE_HOME/wavelog-relay/qso_queue.jsonl`
/// (or `~/.local/state/wavelog-relay/qso_queue.jsonl`).
fn default_qso_queue_path() -> PathBuf {
    let dir = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local").join("state")))
        .unwrap_or_else(|| PathBuf::from("."));
    dir.join("wavelog-relay").join("qso_queue.jsonl")
}

fn load_toml(explicit: Option<&Path>) -> Result<TomlConfig, ConfigError> {
    let (path, required) = match explicit {
        Some(p) => (p.to_path_buf(), true),
        None => match default_config_path() {
            Some(p) => (p, false),
            None => return Ok(TomlConfig::default()),
        },
    };

    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            if required {
                return Err(ConfigError::ConfigNotFound(path.into_boxed_path()));
            }
            return Ok(TomlConfig::default());
        },
        Err(source) => {
            return Err(ConfigError::ConfigRead {
                path: path.into_boxed_path(),
                source,
            });
        },
    };

    let text = std::str::from_utf8(&bytes)
        .map_err(|_| ConfigError::ConfigInvalidUtf8(path.clone().into_boxed_path()))?;
    toml::from_str(text).map_err(|e| ConfigError::ConfigParse {
        path: path.into_boxed_path(),
        message: e.to_string().into(),
    })
}

fn resolve_key(cli_key_file: Option<&Path>) -> Result<String, ConfigError> {
    if let Ok(raw) = std::env::var("WAVELOG_RELAY_KEY")
        && !raw.is_empty()
    {
        return Ok(raw);
    }
    if let Some(path) = cli_key_file {
        return read_key_file(path);
    }
    Err(ConfigError::MissingKey)
}

fn read_key_file(path: &Path) -> Result<String, ConfigError> {
    let bytes = std::fs::read(path).map_err(|source| ConfigError::KeyFileRead {
        path: Box::<Path>::from(path),
        source,
    })?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| ConfigError::KeyFileInvalidUtf8(Box::<Path>::from(path)))?;
    let key = text.trim().to_owned();
    if key.is_empty() {
        return Err(ConfigError::EmptyKeyFile(Box::<Path>::from(path)));
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;
    use crate::modes::HamlibMode;

    fn full_cli() -> Cli {
        Cli {
            command: None,
            rigctld: Some("10.0.0.1:9999".parse().unwrap()),
            wavelog_url: Some("https://wavelog.example.com/index.php".to_owned()),
            radio: Some("FT-710".to_owned()),
            key_file: None,
            power_max: Some(50.0),
            listen: Some("0.0.0.0:54321".parse().unwrap()),
            ws_listen: Some("0.0.0.0:54322".parse().unwrap()),
            no_ws: false,
            wsjtx: true,
            wsjtx_listen: Some("0.0.0.0:2237".parse().unwrap()),
            station_id: Some("3".to_owned()),
            qso_queue_path: Some(PathBuf::from("/tmp/cli-queue.jsonl")),
            interval: Some(Duration::from_millis(500)),
            rig_timeout: Some(Duration::from_secs(5)),
            config: None,
            log_level: Some("debug".to_owned()),
        }
    }

    fn expected_endpoint(s: &str) -> Endpoint {
        s.parse().expect("test endpoint")
    }

    fn empty_cli() -> Cli {
        Cli::default()
    }

    #[test]
    fn parse_toml_full() {
        let s = r#"
            rigctld = "10.0.0.5:4532"
            wavelog_url = "https://wavelog.test/"
            radio = "IC-7300"
            power_max = 50.0
            listen = "127.0.0.1:54321"
            ws_listen = "127.0.0.1:54322"
            no_ws = true
            wsjtx = true
            wsjtx_listen = "127.0.0.1:12345"
            station_id = "7"
            interval = "500ms"
            log_level = "debug"

            [mode_overrides]
            pkt = "PKTLSB"
            dig = "PKTFM"
        "#;
        let parsed: TomlConfig = toml::from_str(s).unwrap();
        assert_eq!(
            parsed.rigctld.as_ref().unwrap(),
            &expected_endpoint("10.0.0.5:4532")
        );
        assert_eq!(parsed.wavelog_url.as_deref(), Some("https://wavelog.test/"));
        assert_eq!(parsed.radio.as_deref(), Some("IC-7300"));
        assert_eq!(parsed.power_max, Some(50.0));
        assert_eq!(
            parsed.ws_listen,
            Some("127.0.0.1:54322".parse::<SocketAddr>().unwrap())
        );
        assert_eq!(parsed.no_ws, Some(true));
        assert_eq!(parsed.wsjtx, Some(true));
        assert_eq!(
            parsed.wsjtx_listen,
            Some("127.0.0.1:12345".parse::<SocketAddr>().unwrap())
        );
        assert_eq!(parsed.station_id.as_deref(), Some("7"));
        assert_eq!(parsed.interval, Some(Duration::from_millis(500)));
        assert_eq!(parsed.log_level.as_deref(), Some("debug"));
        assert_eq!(parsed.mode_overrides.pkt, HamlibMode::PktLsb);
        assert_eq!(parsed.mode_overrides.dig, HamlibMode::PktFm);
    }

    #[test]
    fn parse_toml_with_rig_timeout() {
        let s = r#"rig_timeout = "5s""#;
        let parsed: TomlConfig = toml::from_str(s).unwrap();
        assert_eq!(parsed.rig_timeout, Some(Duration::from_secs(5)));
    }

    #[test]
    fn parse_toml_empty() {
        let parsed: TomlConfig = toml::from_str("").unwrap();
        assert!(parsed.rigctld.is_none());
        assert_eq!(parsed.mode_overrides, ModeOverrides::default());
    }

    #[test]
    fn parse_toml_rejects_unknown_field() {
        let s = r#"flrig_host = "127.0.0.1""#;
        assert!(toml::from_str::<TomlConfig>(s).is_err());
    }

    #[test]
    fn merge_prefers_cli_over_toml() {
        let cli = full_cli();
        let toml = TomlConfig {
            rigctld: Some(expected_endpoint("9.9.9.9:1")),
            wavelog_url: Some("https://other/".to_owned()),
            radio: Some("IGNORED".to_owned()),
            power_max: Some(1.0),
            listen: Some("127.0.0.1:1".parse().unwrap()),
            ws_listen: Some("127.0.0.1:2".parse().unwrap()),
            no_ws: Some(true),
            wsjtx: Some(false),
            wsjtx_listen: Some("127.0.0.1:9999".parse().unwrap()),
            station_id: Some("99".to_owned()),
            qso_queue_path: Some(PathBuf::from("/tmp/toml-queue.jsonl")),
            interval: Some(Duration::from_secs(60)),
            rig_timeout: Some(Duration::from_secs(99)),
            log_level: Some("trace".to_owned()),
            mode_overrides: ModeOverrides::default(),
        };
        let cfg = Config::merge(cli, toml, "key".to_owned()).unwrap();
        assert_eq!(cfg.rigctld_endpoint, expected_endpoint("10.0.0.1:9999"));
        assert_eq!(&*cfg.wavelog_url, "https://wavelog.example.com/index.php");
        assert_eq!(&*cfg.radio, "FT-710");
        assert_eq!(cfg.power_max_watts, 50.0);
        assert_eq!(
            cfg.listen_addr,
            "0.0.0.0:54321".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            cfg.ws_listen_addr,
            "0.0.0.0:54322".parse::<SocketAddr>().unwrap()
        );
        // TOML asked to disable; CLI didn't pass --no-ws → CLI wins
        // only when it was actually set, so TOML's `true` stands.
        assert!(cfg.no_ws);
        // CLI sets --wsjtx; TOML asks for false but CLI wins.
        assert!(cfg.wsjtx);
        assert_eq!(
            cfg.wsjtx_listen_addr,
            "0.0.0.0:2237".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(cfg.station_id.as_deref(), Some("3"));
        assert_eq!(cfg.qso_queue_path, PathBuf::from("/tmp/cli-queue.jsonl"));
        assert_eq!(cfg.poll_interval, Duration::from_millis(500));
        assert_eq!(cfg.rig_timeout, Duration::from_secs(5));
        assert_eq!(&*cfg.log_level, "debug");
    }

    #[test]
    fn merge_falls_through_to_toml_when_cli_absent() {
        let toml = TomlConfig {
            rigctld: Some(expected_endpoint("9.9.9.9:1")),
            wavelog_url: Some("https://wavelog.test/".to_owned()),
            radio: Some("IC-7300".to_owned()),
            power_max: Some(75.0),
            listen: Some("127.0.0.1:11111".parse().unwrap()),
            ws_listen: Some("127.0.0.1:11122".parse().unwrap()),
            no_ws: None,
            wsjtx: Some(true),
            wsjtx_listen: Some("127.0.0.1:22222".parse().unwrap()),
            station_id: Some("42".to_owned()),
            qso_queue_path: None,
            interval: Some(Duration::from_secs(2)),
            rig_timeout: Some(Duration::from_secs(7)),
            log_level: Some("warn".to_owned()),
            mode_overrides: ModeOverrides::default(),
        };
        let cfg = Config::merge(empty_cli(), toml, "key".to_owned()).unwrap();
        assert_eq!(cfg.rigctld_endpoint, expected_endpoint("9.9.9.9:1"));
        assert_eq!(&*cfg.wavelog_url, "https://wavelog.test/");
        assert_eq!(&*cfg.radio, "IC-7300");
        assert_eq!(cfg.power_max_watts, 75.0);
        assert_eq!(
            cfg.ws_listen_addr,
            "127.0.0.1:11122".parse::<SocketAddr>().unwrap()
        );
        assert!(!cfg.no_ws);
        assert!(cfg.wsjtx);
        assert_eq!(
            cfg.wsjtx_listen_addr,
            "127.0.0.1:22222".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(cfg.station_id.as_deref(), Some("42"));
        assert_eq!(cfg.poll_interval, Duration::from_secs(2));
        assert_eq!(cfg.rig_timeout, Duration::from_secs(7));
        assert_eq!(&*cfg.log_level, "warn");
    }

    #[test]
    fn merge_applies_defaults_for_optional_fields() {
        let cli = Cli {
            wavelog_url: Some("https://wavelog.test/".to_owned()),
            radio: Some("R".to_owned()),
            ..Cli::default()
        };
        let cfg = Config::merge(cli, TomlConfig::default(), "k".to_owned()).unwrap();
        assert_eq!(
            cfg.rigctld_endpoint,
            expected_endpoint(DEFAULT_RIGCTLD_ADDR)
        );
        assert_eq!(
            cfg.listen_addr,
            DEFAULT_LISTEN_ADDR.parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            cfg.ws_listen_addr,
            DEFAULT_WS_LISTEN_ADDR.parse::<SocketAddr>().unwrap()
        );
        assert!(!cfg.no_ws);
        assert!(!cfg.wsjtx, "WSJT-X listener must default to off");
        assert_eq!(
            cfg.wsjtx_listen_addr,
            DEFAULT_WSJTX_LISTEN_ADDR.parse::<SocketAddr>().unwrap()
        );
        assert!(cfg.station_id.is_none());
        assert_eq!(cfg.power_max_watts, DEFAULT_POWER_MAX_WATTS);
        assert_eq!(cfg.poll_interval, DEFAULT_POLL_INTERVAL);
        assert_eq!(cfg.rig_timeout, DEFAULT_RIG_TIMEOUT);
        assert_eq!(&*cfg.log_level, DEFAULT_LOG_LEVEL);
    }

    #[test]
    fn cli_wsjtx_flag_enables_listener() {
        let cli = Cli {
            wavelog_url: Some("https://wavelog.test/".to_owned()),
            radio: Some("R".to_owned()),
            wsjtx: true,
            station_id: Some("1".to_owned()),
            ..Cli::default()
        };
        let cfg = Config::merge(cli, TomlConfig::default(), "k".to_owned()).unwrap();
        assert!(cfg.wsjtx);
    }

    #[test]
    fn toml_wsjtx_true_enables_when_cli_absent() {
        let toml = TomlConfig {
            wavelog_url: Some("https://wavelog.test/".to_owned()),
            radio: Some("R".to_owned()),
            wsjtx: Some(true),
            station_id: Some("1".to_owned()),
            ..TomlConfig::default()
        };
        let cfg = Config::merge(Cli::default(), toml, "k".to_owned()).unwrap();
        assert!(cfg.wsjtx);
    }

    #[test]
    fn cli_station_id_overrides_toml() {
        let cli = Cli {
            wavelog_url: Some("https://wavelog.test/".to_owned()),
            radio: Some("R".to_owned()),
            station_id: Some("17".to_owned()),
            ..Cli::default()
        };
        let toml = TomlConfig {
            station_id: Some("99".to_owned()),
            ..TomlConfig::default()
        };
        let cfg = Config::merge(cli, toml, "k".to_owned()).unwrap();
        assert_eq!(cfg.station_id.as_deref(), Some("17"));
    }

    #[test]
    fn merge_rejects_wsjtx_without_station_id() {
        let cli = Cli {
            wavelog_url: Some("https://wavelog.test/".to_owned()),
            radio: Some("R".to_owned()),
            wsjtx: true,
            // station_id: None
            ..Cli::default()
        };
        let err = Config::merge(cli, TomlConfig::default(), "k".to_owned()).unwrap_err();
        assert!(matches!(err, ConfigError::MissingStationId), "got {err:?}");
    }

    #[test]
    fn merge_accepts_station_id_without_wsjtx() {
        // station_id set, but --wsjtx not enabled: harmless (the field
        // is just unused). No error.
        let cli = Cli {
            wavelog_url: Some("https://wavelog.test/".to_owned()),
            radio: Some("R".to_owned()),
            station_id: Some("1".to_owned()),
            ..Cli::default()
        };
        let cfg = Config::merge(cli, TomlConfig::default(), "k".to_owned()).unwrap();
        assert!(!cfg.wsjtx);
        assert_eq!(cfg.station_id.as_deref(), Some("1"));
    }

    #[test]
    fn cli_no_ws_overrides_toml_false() {
        let cli = Cli {
            wavelog_url: Some("https://wavelog.test/".to_owned()),
            radio: Some("R".to_owned()),
            no_ws: true,
            ..Cli::default()
        };
        let toml = TomlConfig {
            no_ws: Some(false),
            ..TomlConfig::default()
        };
        let cfg = Config::merge(cli, toml, "k".to_owned()).unwrap();
        assert!(cfg.no_ws, "CLI --no-ws must override TOML no_ws = false");
    }

    #[test]
    fn merge_rejects_zero_rig_timeout() {
        let cli = Cli {
            wavelog_url: Some("https://wavelog.test/".to_owned()),
            radio: Some("R".to_owned()),
            rig_timeout: Some(Duration::ZERO),
            ..Cli::default()
        };
        let err = Config::merge(cli, TomlConfig::default(), "k".to_owned()).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidRigTimeout), "got {err:?}");
    }

    #[test]
    fn merge_accepts_hostname_endpoint() {
        let cli = Cli {
            rigctld: Some(expected_endpoint("rig.local:4532")),
            wavelog_url: Some("https://wavelog.test/".to_owned()),
            radio: Some("R".to_owned()),
            ..Cli::default()
        };
        let cfg = Config::merge(cli, TomlConfig::default(), "k".to_owned()).unwrap();
        assert_eq!(cfg.rigctld_endpoint, expected_endpoint("rig.local:4532"));
    }

    #[test]
    fn merge_requires_wavelog_url() {
        let cli = Cli {
            radio: Some("R".to_owned()),
            ..Cli::default()
        };
        let err = Config::merge(cli, TomlConfig::default(), "k".to_owned()).unwrap_err();
        assert!(matches!(err, ConfigError::MissingWavelogUrl), "got {err:?}");
    }

    #[test]
    fn merge_requires_radio() {
        let cli = Cli {
            wavelog_url: Some("https://wavelog.test/".to_owned()),
            ..Cli::default()
        };
        let err = Config::merge(cli, TomlConfig::default(), "k".to_owned()).unwrap_err();
        assert!(matches!(err, ConfigError::MissingRadio), "got {err:?}");
    }

    #[test]
    fn merge_rejects_non_positive_power_max() {
        let cli = Cli {
            wavelog_url: Some("https://wavelog.test/".to_owned()),
            radio: Some("R".to_owned()),
            power_max: Some(0.0),
            ..Cli::default()
        };
        let err = Config::merge(cli, TomlConfig::default(), "k".to_owned()).unwrap_err();
        assert!(
            matches!(err, ConfigError::InvalidPowerMax(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn merge_rejects_zero_interval() {
        let cli = Cli {
            wavelog_url: Some("https://wavelog.test/".to_owned()),
            radio: Some("R".to_owned()),
            interval: Some(Duration::ZERO),
            ..Cli::default()
        };
        let err = Config::merge(cli, TomlConfig::default(), "k".to_owned()).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidInterval), "got {err:?}");
    }

    #[test]
    fn merge_rejects_invalid_url() {
        let cli = Cli {
            wavelog_url: Some("not a url".to_owned()),
            radio: Some("R".to_owned()),
            ..Cli::default()
        };
        let err = Config::merge(cli, TomlConfig::default(), "k".to_owned()).unwrap_err();
        assert!(
            matches!(err, ConfigError::InvalidWavelogUrl(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn origin_extracted_for_https_url() {
        let origin = parse_origin("https://wavelog.mdel.io/index.php").unwrap();
        assert_eq!(origin, "https://wavelog.mdel.io");
    }

    #[test]
    fn origin_includes_nonstandard_port() {
        let origin = parse_origin("http://wavelog.mdel.io:8080/").unwrap();
        assert_eq!(origin, "http://wavelog.mdel.io:8080");
    }

    #[test]
    fn origin_rejects_opaque_scheme() {
        // file:// URLs produce opaque origins per the URL spec.
        let result = parse_origin("file:///tmp/x");
        assert!(matches!(result, Err(ConfigError::InvalidWavelogUrl(_))));
    }

    #[test]
    fn read_key_file_trims_whitespace_and_newlines() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "  my-secret-key  ").unwrap();
        let key = read_key_file(f.path()).unwrap();
        assert_eq!(key, "my-secret-key");
    }

    #[test]
    fn read_key_file_errors_on_empty_contents() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "   \n  \n").unwrap();
        let err = read_key_file(f.path()).unwrap_err();
        assert!(matches!(err, ConfigError::EmptyKeyFile(_)), "got {err:?}");
    }

    #[test]
    fn read_key_file_errors_on_missing() {
        let path = PathBuf::from("/nonexistent/key");
        let err = read_key_file(&path).unwrap_err();
        assert!(
            matches!(err, ConfigError::KeyFileRead { .. }),
            "got {err:?}"
        );
    }
}
