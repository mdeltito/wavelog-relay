//! Tokio actor that owns the single TCP connection to rigctld.
//!
//! Callers interact only through the cloned [`RigHandle`], which sends
//! [`RigCommand`]s over an mpsc and awaits a oneshot reply. The actor
//! processes one command at a time, so rigctld's untagged line-protocol
//! responses line up positionally with the commands that produced them
//! — no request-id correlation needed.
//!
//! On any I/O error the actor sends `Disconnected` to the in-flight
//! oneshot, drops the connection, and reconnects with capped
//! exponential backoff (500 ms, 1 s, 2 s, 5 s, 10 s). Per-command
//! hamlib errors (`RPRT -N`) are surfaced to the caller without
//! disturbing the connection.

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use std::{fmt, io};

use serde::Deserialize;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::modes::HamlibMode;

/// Spawn the rigctld actor task. The returned [`JoinHandle`] completes
/// once all [`RigHandle`]s have been dropped (clean shutdown).
///
/// `read_timeout` bounds how long the actor will wait for a single
/// response line from rigctld. On expiry the actor treats it as an I/O
/// error, fails the in-flight command with [`RigError::Disconnected`],
/// and reconnects via the standard backoff schedule.
#[must_use]
pub fn spawn(endpoint: impl Into<Endpoint>, read_timeout: Duration) -> (RigHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(8);
    let actor = RigActor {
        endpoint: endpoint.into(),
        read_timeout,
        rx,
    };
    let join = tokio::spawn(actor.run());
    (RigHandle { tx }, join)
}

/// Connection target for rigctld. Accepts both pre-resolved socket
/// addresses (`127.0.0.1:4532`, `[::1]:4532`) and `host:port` strings
/// (`rig.local:4532`); the latter defers DNS resolution to connect time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Resolved(SocketAddr),
    Host { host: Box<str>, port: u16 },
}

#[derive(Debug, Error)]
pub enum EndpointParseError {
    #[error("missing port in rigctld endpoint `{0}`")]
    MissingPort(Box<str>),

    #[error("invalid port in rigctld endpoint `{0}`")]
    InvalidPort(Box<str>),

    #[error("empty host in rigctld endpoint `{0}`")]
    EmptyHost(Box<str>),

    #[error("IPv6 endpoint must be bracketed: `[{addr}]:port` (got `{raw}`)")]
    AmbiguousIpv6 { raw: Box<str>, addr: Box<str> },
}

impl FromStr for Endpoint {
    type Err = EndpointParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Pre-resolved SocketAddr handles `127.0.0.1:4532` and bracketed
        // IPv6 (`[::1]:4532`) without DNS.
        if let Ok(addr) = s.parse::<SocketAddr>() {
            return Ok(Self::Resolved(addr));
        }
        let (host, port) = s
            .rsplit_once(':')
            .ok_or_else(|| EndpointParseError::MissingPort(s.into()))?;
        if host.is_empty() {
            return Err(EndpointParseError::EmptyHost(s.into()));
        }
        // Unbracketed IPv6 (`::1:4532`) is ambiguous: is the trailing
        // `:4532` a port or part of the address? Refuse rather than
        // guess; users must bracket.
        if host.contains(':') {
            return Err(EndpointParseError::AmbiguousIpv6 {
                raw: s.into(),
                addr: host.into(),
            });
        }
        let port: u16 = port
            .parse()
            .map_err(|_| EndpointParseError::InvalidPort(port.into()))?;
        Ok(Self::Host {
            host: host.into(),
            port,
        })
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resolved(addr) => fmt::Display::fmt(addr, f),
            Self::Host { host, port } => write!(f, "{host}:{port}"),
        }
    }
}

impl From<SocketAddr> for Endpoint {
    fn from(addr: SocketAddr) -> Self {
        Self::Resolved(addr)
    }
}

impl<'de> Deserialize<'de> for Endpoint {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Cloneable client for the rigctld actor.
#[derive(Debug, Clone)]
pub struct RigHandle {
    tx: mpsc::Sender<RigCommand>,
}

/// Snapshot of rig state captured by a single poll cycle.
#[derive(Debug, Clone)]
pub struct RigState {
    pub freq: u64,
    /// Hamlib mode name as reported by rigctld (e.g. `USB`, `CW-U`).
    /// Passed through to Wavelog unmodified — Wavelog normalizes
    /// server-side.
    pub mode: Box<str>,
    /// Raw RFPOWER fraction, `0.0..=1.0`. Convert to watts by
    /// multiplying by the rig's max RF power. `None` when the rig
    /// backend doesn't expose RFPOWER readback (`RPRT -11` etc.) —
    /// the consumer should omit the field rather than substitute a
    /// fake value.
    pub power: Option<f32>,
}

#[derive(Debug, Error, Clone)]
pub enum RigError {
    #[error("rigctld connection lost")]
    Disconnected,

    #[error("rigctld returned error code {0}")]
    Hamlib(i32),

    #[error("rigctld returned an unparseable response: {0}")]
    BadResponse(Box<str>),
}

impl RigHandle {
    pub async fn get_freq(&self) -> Result<u64, RigError> {
        self.request(RigCommand::GetFreq).await
    }

    pub async fn get_mode(&self) -> Result<Box<str>, RigError> {
        self.request(RigCommand::GetMode).await
    }

    pub async fn get_power(&self) -> Result<f32, RigError> {
        self.request(RigCommand::GetPower).await
    }

    pub async fn set_freq(&self, hz: u64) -> Result<(), RigError> {
        self.request(|reply| RigCommand::SetFreq { hz, reply })
            .await
    }

    pub async fn set_mode(&self, mode: HamlibMode) -> Result<(), RigError> {
        self.request(|reply| RigCommand::SetMode { mode, reply })
            .await
    }

    /// Set frequency and mode atomically on the actor's socket. The
    /// actor writes `F`/reads `RPRT`/writes `M`/reads `RPRT` inside a
    /// single command dispatch — no other queued command (notably the
    /// poller's `f`/`m`/`\get_level RFPOWER`) can land between them.
    pub async fn set_freq_mode(&self, hz: u64, mode: HamlibMode) -> Result<(), RigError> {
        self.request(|reply| RigCommand::SetFreqMode { hz, mode, reply })
            .await
    }

    /// Read freq, mode and RFPOWER as a single atomic snapshot.
    ///
    /// All three reads happen inside one actor command dispatch — no
    /// other queued command (notably a click-to-tune `set_freq_mode`)
    /// can land between them, so the resulting [`RigState`] is always
    /// internally consistent.
    ///
    /// RFPOWER is treated specially: backends that don't support
    /// `\get_level RFPOWER` (returning `RPRT -11` or similar) yield
    /// `power = None` rather than failing the whole snapshot. Other
    /// I/O errors propagate.
    pub async fn poll(&self) -> Result<RigState, RigError> {
        self.request(RigCommand::Poll).await
    }

    async fn request<T, F>(&self, make: F) -> Result<T, RigError>
    where
        F: FnOnce(oneshot::Sender<Result<T, RigError>>) -> RigCommand,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(make(reply_tx))
            .await
            .map_err(|_| RigError::Disconnected)?;
        reply_rx.await.map_err(|_| RigError::Disconnected)?
    }
}

enum RigCommand {
    GetFreq(oneshot::Sender<Result<u64, RigError>>),
    GetMode(oneshot::Sender<Result<Box<str>, RigError>>),
    GetPower(oneshot::Sender<Result<f32, RigError>>),
    Poll(oneshot::Sender<Result<RigState, RigError>>),
    SetFreq {
        hz: u64,
        reply: oneshot::Sender<Result<(), RigError>>,
    },
    SetMode {
        mode: HamlibMode,
        reply: oneshot::Sender<Result<(), RigError>>,
    },
    SetFreqMode {
        hz: u64,
        mode: HamlibMode,
        reply: oneshot::Sender<Result<(), RigError>>,
    },
}

struct RigActor {
    endpoint: Endpoint,
    read_timeout: Duration,
    rx: mpsc::Receiver<RigCommand>,
}

impl RigActor {
    async fn run(mut self) {
        let mut backoff_idx: usize = 0;
        loop {
            match self.connect().await {
                Ok(stream) => {
                    tracing::info!(endpoint = %self.endpoint, "rigctld connected");
                    backoff_idx = 0;
                    if let ServeOutcome::ChannelClosed = self.serve(stream).await {
                        return;
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, endpoint = %self.endpoint, "rigctld connect failed");
                },
            }
            let delay = backoff_delay(backoff_idx);
            backoff_idx = backoff_idx.saturating_add(1);
            if !self.sleep_draining(delay).await {
                return;
            }
        }
    }

    async fn connect(&self) -> io::Result<TcpStream> {
        match &self.endpoint {
            Endpoint::Resolved(addr) => TcpStream::connect(addr).await,
            Endpoint::Host { host, port } => TcpStream::connect((host.as_ref(), *port)).await,
        }
    }

    async fn serve(&mut self, stream: TcpStream) -> ServeOutcome {
        let mut conn = Connection::new(stream, self.read_timeout);
        while let Some(cmd) = self.rx.recv().await {
            if let Err(e) = handle_command(&mut conn, cmd).await {
                tracing::warn!(error = %e, "rigctld i/o error; reconnecting");
                return ServeOutcome::Io;
            }
        }
        ServeOutcome::ChannelClosed
    }

    /// Sleep for `delay`, replying `Disconnected` to any commands that
    /// arrive in the meantime. Returns `false` if the command channel
    /// closes (all handles dropped) and the actor should exit.
    async fn sleep_draining(&mut self, delay: Duration) -> bool {
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                () = &mut sleep => return true,
                cmd = self.rx.recv() => match cmd {
                    Some(cmd) => fail_command(cmd, RigError::Disconnected),
                    None => return false,
                }
            }
        }
    }
}

enum ServeOutcome {
    ChannelClosed,
    Io,
}

const BACKOFF: [Duration; 5] = [
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(5),
    Duration::from_secs(10),
];

fn backoff_delay(idx: usize) -> Duration {
    BACKOFF[idx.min(BACKOFF.len() - 1)]
}

fn fail_command(cmd: RigCommand, err: RigError) {
    match cmd {
        RigCommand::GetFreq(reply) => {
            let _ = reply.send(Err(err));
        },
        RigCommand::GetMode(reply) => {
            let _ = reply.send(Err(err));
        },
        RigCommand::GetPower(reply) => {
            let _ = reply.send(Err(err));
        },
        RigCommand::Poll(reply) => {
            let _ = reply.send(Err(err));
        },
        RigCommand::SetFreq { reply, .. } => {
            let _ = reply.send(Err(err));
        },
        RigCommand::SetMode { reply, .. } => {
            let _ = reply.send(Err(err));
        },
        RigCommand::SetFreqMode { reply, .. } => {
            let _ = reply.send(Err(err));
        },
    }
}

async fn handle_command(conn: &mut Connection, cmd: RigCommand) -> io::Result<()> {
    match cmd {
        RigCommand::GetFreq(reply) => dispatch(reply, exec_get_freq(conn)).await,
        RigCommand::GetMode(reply) => dispatch(reply, exec_get_mode(conn)).await,
        RigCommand::GetPower(reply) => dispatch(reply, exec_get_power(conn)).await,
        RigCommand::Poll(reply) => dispatch(reply, exec_poll(conn)).await,
        RigCommand::SetFreq { hz, reply } => dispatch(reply, exec_set_freq(conn, hz)).await,
        RigCommand::SetMode { mode, reply } => dispatch(reply, exec_set_mode(conn, mode)).await,
        RigCommand::SetFreqMode { hz, mode, reply } => {
            dispatch(reply, exec_set_freq_mode(conn, hz, mode)).await
        },
    }
}

async fn dispatch<T>(
    reply: oneshot::Sender<Result<T, RigError>>,
    exec: impl std::future::Future<Output = io::Result<Result<T, RigError>>>,
) -> io::Result<()> {
    match exec.await {
        Ok(r) => {
            let _ = reply.send(r);
            Ok(())
        },
        Err(e) => {
            let _ = reply.send(Err(RigError::Disconnected));
            Err(e)
        },
    }
}

struct Connection {
    stream: BufStream<TcpStream>,
    line_buf: String,
    read_timeout: Duration,
}

impl Connection {
    fn new(stream: TcpStream, read_timeout: Duration) -> Self {
        Self {
            stream: BufStream::new(stream),
            line_buf: String::new(),
            read_timeout,
        }
    }

    async fn send(&mut self, cmd: &str) -> io::Result<()> {
        self.stream.write_all(cmd.as_bytes()).await?;
        self.stream.write_all(b"\n").await?;
        self.stream.flush().await
    }

    async fn read_line(&mut self) -> io::Result<&str> {
        self.line_buf.clear();
        let read = self.stream.read_line(&mut self.line_buf);
        let n = tokio::time::timeout(self.read_timeout, read)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "rigctld read timed out"))??;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "rigctld closed the connection",
            ));
        }
        Ok(self.line_buf.trim_end_matches(['\r', '\n']))
    }
}

fn parse_rprt(line: &str) -> Option<i32> {
    line.strip_prefix("RPRT ")
        .and_then(|n| n.trim().parse().ok())
}

fn parse_set_response(line: &str) -> Result<(), RigError> {
    match parse_rprt(line) {
        Some(0) => Ok(()),
        Some(code) => Err(RigError::Hamlib(code)),
        None => Err(RigError::BadResponse(line.into())),
    }
}

async fn exec_get_freq(conn: &mut Connection) -> io::Result<Result<u64, RigError>> {
    conn.send("f").await?;
    let line = conn.read_line().await?;
    if let Some(code) = parse_rprt(line) {
        return Ok(Err(RigError::Hamlib(code)));
    }
    Ok(line
        .parse::<u64>()
        .map_err(|_| RigError::BadResponse(line.into())))
}

async fn exec_get_mode(conn: &mut Connection) -> io::Result<Result<Box<str>, RigError>> {
    conn.send("m").await?;
    let line1 = conn.read_line().await?;
    if let Some(code) = parse_rprt(line1) {
        return Ok(Err(RigError::Hamlib(code)));
    }
    let token = line1.split_whitespace().next().unwrap_or("");
    if token.is_empty() {
        // Empty mode line is a malformed reply — surfacing it as
        // BadResponse beats letting "" leak into the wavelog payload.
        // Owned snapshot so we can release the read buffer borrow and
        // still drain the passband line that follows. Suppress drain
        // I/O errors so the original BadResponse isn't masked by a
        // disconnect during cleanup; the next command will surface
        // the disconnect on its own.
        let snapshot: Box<str> = line1.into();
        let _ = conn.read_line().await;
        return Ok(Err(RigError::BadResponse(snapshot)));
    }
    let mode: Box<str> = token.into();
    // Drain the passband line that follows on a successful response.
    conn.read_line().await?;
    Ok(Ok(mode))
}

async fn exec_get_power(conn: &mut Connection) -> io::Result<Result<f32, RigError>> {
    conn.send("\\get_level RFPOWER").await?;
    let line = conn.read_line().await?;
    if let Some(code) = parse_rprt(line) {
        return Ok(Err(RigError::Hamlib(code)));
    }
    let raw: f32 = match line.parse() {
        Ok(v) => v,
        Err(_) => return Ok(Err(RigError::BadResponse(line.into()))),
    };
    if !raw.is_finite() {
        return Ok(Err(RigError::BadResponse(line.into())));
    }
    // Some hamlib backends overshoot the documented [0.0, 1.0] range
    // by a hair due to internal scaling rounding (e.g. 1.001). Clamp
    // rather than reject — the alternative is dropping every snapshot
    // whenever the rig sits at full power.
    let clamped = raw.clamp(0.0, 1.0);
    if !(0.0..=1.0).contains(&raw) {
        tracing::warn!(raw, clamped, "rigctld RFPOWER out of [0,1] range; clamping");
    }
    Ok(Ok(clamped))
}

/// Read freq → mode → RFPOWER inside a single actor dispatch. The
/// `?`-on-`io::Result` cascade is what gives us atomicity: nothing else
/// in the actor's `recv` loop runs until this future resolves.
///
/// RFPOWER is special: a backend that returns `RPRT -N` for the level
/// query produces `power = None` (the rig simply doesn't report it),
/// not a failed snapshot. Other paths (Disconnected, BadResponse) still
/// surface to the caller.
async fn exec_poll(conn: &mut Connection) -> io::Result<Result<RigState, RigError>> {
    let freq = match exec_get_freq(conn).await? {
        Ok(v) => v,
        Err(e) => return Ok(Err(e)),
    };
    let mode = match exec_get_mode(conn).await? {
        Ok(v) => v,
        Err(e) => return Ok(Err(e)),
    };
    let power = match exec_get_power(conn).await? {
        Ok(v) => Some(v),
        Err(RigError::Hamlib(_)) => None,
        Err(e) => return Ok(Err(e)),
    };
    Ok(Ok(RigState { freq, mode, power }))
}

async fn exec_set_freq(conn: &mut Connection, hz: u64) -> io::Result<Result<(), RigError>> {
    conn.send(&format!("F {hz}")).await?;
    let line = conn.read_line().await?;
    Ok(parse_set_response(line))
}

async fn exec_set_mode(
    conn: &mut Connection,
    mode: HamlibMode,
) -> io::Result<Result<(), RigError>> {
    let mode_str = mode.as_str();
    // Passband -1 == RIG_PASSBAND_NOCHANGE in hamlib: change the mode
    // but leave the rig's current DSP filter width alone. Sending `0`
    // (RIG_PASSBAND_NORMAL) makes the backend apply the rig's default
    // passband for the new mode, which clobbers a user's tuned filter
    // every click-to-tune (e.g. FT-710 snapping back to 2400 Hz USB).
    conn.send(&format!("M {mode_str} -1")).await?;
    let line = conn.read_line().await?;
    Ok(parse_set_response(line))
}

async fn exec_set_freq_mode(
    conn: &mut Connection,
    hz: u64,
    mode: HamlibMode,
) -> io::Result<Result<(), RigError>> {
    conn.send(&format!("F {hz}")).await?;
    let line = conn.read_line().await?;
    if let Err(e) = parse_set_response(line) {
        return Ok(Err(e));
    }
    let mode_str = mode.as_str();
    conn.send(&format!("M {mode_str} -1")).await?;
    let line = conn.read_line().await?;
    Ok(parse_set_response(line))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(3);

    struct MockConn {
        inner: BufStream<TcpStream>,
        line: String,
    }

    impl MockConn {
        fn new(stream: TcpStream) -> Self {
            Self {
                inner: BufStream::new(stream),
                line: String::new(),
            }
        }

        async fn expect(&mut self, expected: &str) {
            self.line.clear();
            let n = self.inner.read_line(&mut self.line).await.unwrap();
            assert_ne!(
                n, 0,
                "client closed connection while expecting `{expected}`"
            );
            assert_eq!(self.line.trim_end_matches(['\r', '\n']), expected);
        }

        async fn reply(&mut self, body: &str) {
            self.inner.write_all(body.as_bytes()).await.unwrap();
            if !body.ends_with('\n') {
                self.inner.write_all(b"\n").await.unwrap();
            }
            self.inner.flush().await.unwrap();
        }
    }

    async fn spawn_mock<F, Fut>(handler: F) -> SocketAddr
    where
        F: FnOnce(MockConn) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handler(MockConn::new(stream)).await;
        });
        addr
    }

    #[test]
    fn parse_rprt_recognizes_codes() {
        assert_eq!(parse_rprt("RPRT 0"), Some(0));
        assert_eq!(parse_rprt("RPRT -1"), Some(-1));
        assert_eq!(parse_rprt("RPRT -42"), Some(-42));
        assert_eq!(parse_rprt("RPRT  -7"), Some(-7));
        assert_eq!(parse_rprt("RPRT garbage"), None);
        assert_eq!(parse_rprt("14000000"), None);
        assert_eq!(parse_rprt(""), None);
    }

    #[test]
    fn parse_set_response_branches() {
        assert!(matches!(parse_set_response("RPRT 0"), Ok(())));
        assert!(matches!(
            parse_set_response("RPRT -1"),
            Err(RigError::Hamlib(-1))
        ));
        assert!(matches!(
            parse_set_response("garbage"),
            Err(RigError::BadResponse(_))
        ));
    }

    #[test]
    fn backoff_caps_at_ten_seconds() {
        assert_eq!(backoff_delay(0), Duration::from_millis(500));
        assert_eq!(backoff_delay(1), Duration::from_secs(1));
        assert_eq!(backoff_delay(2), Duration::from_secs(2));
        assert_eq!(backoff_delay(3), Duration::from_secs(5));
        assert_eq!(backoff_delay(4), Duration::from_secs(10));
        assert_eq!(backoff_delay(5), Duration::from_secs(10));
        assert_eq!(backoff_delay(100), Duration::from_secs(10));
    }

    #[tokio::test]
    async fn get_freq_parses_decimal_hz() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("f").await;
            conn.reply("14074000").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        assert_eq!(handle.get_freq().await.unwrap(), 14_074_000);
    }

    #[tokio::test]
    async fn get_mode_returns_first_token_only() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("m").await;
            conn.reply("USB\n2400").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        assert_eq!(&*handle.get_mode().await.unwrap(), "USB");
    }

    #[tokio::test]
    async fn get_power_parses_fractional_rfpower() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("\\get_level RFPOWER").await;
            conn.reply("0.5").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let p = handle.get_power().await.unwrap();
        assert!((p - 0.5).abs() < 1e-6, "got {p}");
    }

    #[tokio::test]
    async fn set_freq_sends_capital_f_command_then_acks() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("F 14074000").await;
            conn.reply("RPRT 0").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        handle.set_freq(14_074_000).await.unwrap();
    }

    #[tokio::test]
    async fn set_mode_sends_uppercase_hamlib_with_nochange_passband() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("M USB -1").await;
            conn.reply("RPRT 0").await;
            conn.expect("M PKTUSB -1").await;
            conn.reply("RPRT 0").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        handle.set_mode(HamlibMode::Usb).await.unwrap();
        handle.set_mode(HamlibMode::PktUsb).await.unwrap();
    }

    #[tokio::test]
    async fn set_freq_mode_writes_f_then_m_back_to_back() {
        // The mock asserts the exact ordering with no other commands in
        // between — this is the property that prevents a poll tick from
        // interleaving on the shared actor socket.
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("F 28270000").await;
            conn.reply("RPRT 0").await;
            conn.expect("M USB -1").await;
            conn.reply("RPRT 0").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        handle
            .set_freq_mode(28_270_000, HamlibMode::Usb)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn set_freq_mode_skips_m_when_f_fails() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("F 28270000").await;
            conn.reply("RPRT -9").await;
            // Hold open long enough to detect a stray `M` write.
            tokio::time::sleep(Duration::from_millis(200)).await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let err = handle
            .set_freq_mode(28_270_000, HamlibMode::Usb)
            .await
            .unwrap_err();
        assert!(matches!(err, RigError::Hamlib(-9)), "got {err:?}");
    }

    #[tokio::test]
    async fn set_freq_mode_surfaces_m_error() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("F 28270000").await;
            conn.reply("RPRT 0").await;
            conn.expect("M USB -1").await;
            conn.reply("RPRT -1").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let err = handle
            .set_freq_mode(28_270_000, HamlibMode::Usb)
            .await
            .unwrap_err();
        assert!(matches!(err, RigError::Hamlib(-1)), "got {err:?}");
    }

    #[tokio::test]
    async fn set_freq_mode_is_atomic_against_concurrent_get_freq() {
        // While set_freq_mode is in flight, another caller queues a
        // get_freq. The mock asserts F → M → f at the wire; if the
        // actor yielded between F and M the `f` would land in between
        // and the mock's `expect("M ...")` would fail.
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("F 28270000").await;
            // Brief pause to give the racing get_freq time to queue.
            tokio::time::sleep(Duration::from_millis(50)).await;
            conn.reply("RPRT 0").await;
            conn.expect("M USB -1").await;
            conn.reply("RPRT 0").await;
            conn.expect("f").await;
            conn.reply("28270000").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let h2 = handle.clone();
        let click =
            tokio::spawn(async move { handle.set_freq_mode(28_270_000, HamlibMode::Usb).await });
        // Queue the racing poll after the set_freq_mode has at least
        // started writing to the socket.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let poll = tokio::spawn(async move { h2.get_freq().await });
        click.await.unwrap().unwrap();
        assert_eq!(poll.await.unwrap().unwrap(), 28_270_000);
    }

    #[tokio::test]
    async fn rprt_negative_surfaces_as_hamlib_error() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("f").await;
            conn.reply("RPRT -1").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let err = handle.get_freq().await.unwrap_err();
        assert!(matches!(err, RigError::Hamlib(-1)), "got {err:?}");
    }

    #[tokio::test]
    async fn set_freq_hamlib_error_surfaces_with_negative_code() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("F 14074000").await;
            conn.reply("RPRT -9").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let err = handle.set_freq(14_074_000).await.unwrap_err();
        assert!(matches!(err, RigError::Hamlib(-9)), "got {err:?}");
    }

    #[tokio::test]
    async fn get_mode_rejects_empty_response_as_bad_response() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("m").await;
            // First line empty, second line is the (also-meaningless)
            // passband — exec_get_mode must drain both to keep the
            // connection in sync.
            conn.reply("\n0").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let err = handle.get_mode().await.unwrap_err();
        assert!(matches!(err, RigError::BadResponse(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn get_power_rejects_non_finite_value() {
        for reply in ["NaN", "inf", "-inf"] {
            let r = reply;
            let addr = spawn_mock(move |mut conn| async move {
                conn.expect("\\get_level RFPOWER").await;
                conn.reply(r).await;
            })
            .await;
            let (handle, _join) = spawn(addr, TEST_TIMEOUT);
            let err = handle.get_power().await.unwrap_err();
            assert!(
                matches!(err, RigError::BadResponse(_)),
                "input {reply}: got {err:?}",
            );
        }
    }

    #[tokio::test]
    async fn get_power_clamps_out_of_range_value() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("\\get_level RFPOWER").await;
            conn.reply("1.001").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let p = handle.get_power().await.unwrap();
        assert!((p - 1.0).abs() < 1e-6, "got {p}");
    }

    #[tokio::test]
    async fn get_power_clamps_negative_value() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("\\get_level RFPOWER").await;
            conn.reply("-0.001").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let p = handle.get_power().await.unwrap();
        assert!(p == 0.0, "got {p}");
    }

    #[tokio::test]
    async fn unparseable_response_surfaces_as_bad_response() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("f").await;
            conn.reply("garbage").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let err = handle.get_freq().await.unwrap_err();
        match err {
            RigError::BadResponse(s) => assert_eq!(&*s, "garbage"),
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn server_drop_during_response_surfaces_disconnected() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("f").await;
            drop(conn);
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let err = handle.get_freq().await.unwrap_err();
        assert!(matches!(err, RigError::Disconnected), "got {err:?}");
    }

    #[tokio::test]
    async fn poll_aggregates_freq_mode_and_power() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("f").await;
            conn.reply("14074000").await;
            conn.expect("m").await;
            conn.reply("USB\n2400").await;
            conn.expect("\\get_level RFPOWER").await;
            conn.reply("0.25").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let state = handle.poll().await.unwrap();
        assert_eq!(state.freq, 14_074_000);
        assert_eq!(&*state.mode, "USB");
        let power = state.power.expect("power should be Some when rig replies");
        assert!((power - 0.25).abs() < 1e-6);
    }

    #[tokio::test]
    async fn poll_returns_none_power_when_rig_lacks_rfpower() {
        // Rigs / hamlib backends without RFPOWER readback return
        // `RPRT -11` (`RIG_ENAVAIL`). The whole snapshot must still
        // succeed — power = None is the documented contract.
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("f").await;
            conn.reply("14074000").await;
            conn.expect("m").await;
            conn.reply("USB\n2400").await;
            conn.expect("\\get_level RFPOWER").await;
            conn.reply("RPRT -11").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let state = handle.poll().await.unwrap();
        assert_eq!(state.freq, 14_074_000);
        assert_eq!(&*state.mode, "USB");
        assert!(state.power.is_none(), "power should be None on RPRT -11");
    }

    #[tokio::test]
    async fn poll_is_atomic_against_concurrent_get_freq() {
        // While poll() is in flight, a racing get_freq queues. The
        // mock asserts f → m → \get_level RFPOWER → f at the wire;
        // if poll yielded back to the actor's recv loop between any
        // two reads, the racing `f` would land in between and the
        // mock would observe the wrong sequence.
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("f").await;
            // Brief pause so the racing get_freq has time to queue.
            tokio::time::sleep(Duration::from_millis(50)).await;
            conn.reply("14074000").await;
            conn.expect("m").await;
            conn.reply("USB\n2400").await;
            conn.expect("\\get_level RFPOWER").await;
            conn.reply("0.10").await;
            conn.expect("f").await;
            conn.reply("14100000").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let h2 = handle.clone();
        let poll = tokio::spawn(async move { handle.poll().await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let racing = tokio::spawn(async move { h2.get_freq().await });
        let state = poll.await.unwrap().unwrap();
        assert_eq!(state.freq, 14_074_000);
        assert_eq!(racing.await.unwrap().unwrap(), 14_100_000);
    }

    #[tokio::test]
    async fn two_concurrent_callers_both_get_replies() {
        let addr = spawn_mock(|mut conn| async move {
            conn.expect("f").await;
            conn.reply("14000000").await;
            conn.expect("f").await;
            conn.reply("14100000").await;
        })
        .await;
        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        let h1 = handle.clone();
        let h2 = handle.clone();
        let t1 = tokio::spawn(async move { h1.get_freq().await.unwrap() });
        let t2 = tokio::spawn(async move { h2.get_freq().await.unwrap() });
        let (r1, r2) = tokio::join!(t1, t2);
        let mut got = [r1.unwrap(), r2.unwrap()];
        got.sort_unstable();
        assert_eq!(got, [14_000_000, 14_100_000]);
    }

    #[tokio::test]
    async fn actor_reconnects_after_server_force_close() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            // First connection: one freq exchange, then drop.
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = MockConn::new(stream);
            conn.expect("f").await;
            conn.reply("14000000").await;
            drop(conn);
            // Second connection (after the actor reconnects): another exchange.
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = MockConn::new(stream);
            conn.expect("f").await;
            conn.reply("14100000").await;
        });

        let (handle, _join) = spawn(addr, TEST_TIMEOUT);
        assert_eq!(handle.get_freq().await.unwrap(), 14_000_000);

        // The second call may either succeed on the reconnected socket
        // or fail with Disconnected if it races the EOF detection. Spin
        // briefly until the actor has cycled through backoff + reconnect.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let second = loop {
            match handle.get_freq().await {
                Ok(v) => break v,
                Err(RigError::Disconnected) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                },
                Err(e) => panic!("unexpected error: {e:?}"),
            }
        };
        assert_eq!(second, 14_100_000);
    }

    #[tokio::test]
    async fn read_timeout_surfaces_as_disconnected() {
        // Server accepts the connection and the `f` line but never
        // replies. With a 200 ms timeout the actor must fail the
        // request and tear down the socket within ~the timeout window.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = MockConn::new(stream);
            conn.expect("f").await;
            // Hold the connection open without replying.
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(conn);
        });

        let (handle, _join) = spawn(addr, Duration::from_millis(200));
        let err = tokio::time::timeout(Duration::from_secs(1), handle.get_freq())
            .await
            .expect("actor did not honour read timeout")
            .unwrap_err();
        assert!(matches!(err, RigError::Disconnected), "got {err:?}");
    }

    #[test]
    fn endpoint_parses_ipv4_socketaddr() {
        let e: Endpoint = "127.0.0.1:4532".parse().unwrap();
        assert!(matches!(e, Endpoint::Resolved(_)));
        assert_eq!(e.to_string(), "127.0.0.1:4532");
    }

    #[test]
    fn endpoint_parses_bracketed_ipv6_as_socketaddr() {
        let e: Endpoint = "[::1]:4532".parse().unwrap();
        assert!(matches!(e, Endpoint::Resolved(_)));
    }

    #[test]
    fn endpoint_parses_hostname() {
        let e: Endpoint = "rig.local:4532".parse().unwrap();
        match e {
            Endpoint::Host { host, port } => {
                assert_eq!(&*host, "rig.local");
                assert_eq!(port, 4532);
            },
            other => panic!("expected Host variant, got {other:?}"),
        }
    }

    #[test]
    fn endpoint_rejects_missing_port() {
        let err = "rig.local".parse::<Endpoint>().unwrap_err();
        assert!(
            matches!(err, EndpointParseError::MissingPort(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn endpoint_rejects_bad_port() {
        let err = "rig.local:notaport".parse::<Endpoint>().unwrap_err();
        assert!(
            matches!(err, EndpointParseError::InvalidPort(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn endpoint_rejects_empty_host() {
        let err = ":4532".parse::<Endpoint>().unwrap_err();
        assert!(
            matches!(err, EndpointParseError::EmptyHost(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn endpoint_rejects_unbracketed_ipv6() {
        let err = "::1:4532".parse::<Endpoint>().unwrap_err();
        assert!(
            matches!(err, EndpointParseError::AmbiguousIpv6 { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn endpoint_from_socket_addr_conversion() {
        let addr: SocketAddr = "127.0.0.1:4532".parse().unwrap();
        let endpoint: Endpoint = addr.into();
        assert_eq!(endpoint, Endpoint::Resolved(addr));
    }

    #[test]
    fn endpoint_deserializes_from_toml() {
        #[derive(Deserialize)]
        struct Wrap {
            rigctld: Endpoint,
        }
        let resolved: Wrap = toml::from_str(r#"rigctld = "127.0.0.1:4532""#).unwrap();
        assert!(matches!(resolved.rigctld, Endpoint::Resolved(_)));
        let host: Wrap = toml::from_str(r#"rigctld = "rig.local:4532""#).unwrap();
        assert!(matches!(host.rigctld, Endpoint::Host { .. }));
    }
}
