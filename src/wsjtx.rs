//! WSJT-X UDP listener.
//!
//! WSJT-X (and its forks JTDX/MSHV) emit a binary "network message"
//! protocol over UDP, documented in upstream's `Network/NetworkMessage.hpp`.
//! The default listener is `127.0.0.1:2237`. Every datagram is framed:
//! `magic` (`0xadbccbda`), `schema_version` (`quint32`), `message_type`
//! (`quint32`), and a type-specific body. Strings are encoded as Qt's
//! `QDataStream` `QString`: a signed `qint32` length prefix (big-endian)
//! followed by that many UTF-8 bytes. A length of `-1` means a null
//! string (treated as empty).
//!
//! We care about exactly one message: **Logged ADIF (type 12)**. It
//! carries the complete ADIF record from a completed QSO and is what
//! we forward to Wavelog's `/api/qso` endpoint. WSJT-X also emits
//! `QSO Logged` (type 5) with structured fields immediately before
//! type 12 for each logged QSO — we discard it to avoid double-logging.
//! All other types (Heartbeat, Status, …) are accepted and discarded
//! at debug level.
//!
//! Architecture: a UDP listener task receives + parses, and pipes the
//! ADIF string through a bounded mpsc (32) to a POST worker that calls
//! [`WavelogClient::push_qso`]. Two tasks (rather than one inline) so
//! a slow Wavelog POST can't stall UDP receive — overflowed bursts
//! drop with a warn log instead of relying on the kernel's UDP buffer
//! quietly catching them.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::qso_queue::QsoQueue;
use crate::wavelog::{WavelogClient, WavelogError};

const MAGIC: u32 = 0xadbc_cbda;
const MSG_TYPE_LOGGED_ADIF: u32 = 12;
const QUEUE_CAPACITY: usize = 32;
// UDP max datagram size; WSJT-X messages are typically <1 KB, but a
// fixed full-size buffer is the simplest correct allocation for
// reusing across recv_from calls.
const RECV_BUF_SIZE: usize = 65_535;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WsjtxError {
    #[error("malformed WSJT-X packet: {0}")]
    Parse(Box<str>),
}

/// Bind a UDP socket for receiving WSJT-X datagrams. Detects whether
/// `addr` is unicast or IPv4 multicast and configures the socket
/// accordingly:
///
/// - **Unicast** (e.g. `127.0.0.1:2237`): a plain `UdpSocket::bind`.
///   Only one process can hold the port.
/// - **IPv4 multicast** (e.g. `224.0.0.1:2237`): `SO_REUSEADDR` +
///   `SO_REUSEPORT` (Unix), bind to `0.0.0.0:<port>`, then join the
///   multicast group on the wildcard interface. Multiple processes
///   (GridTracker, JTAlert, us, …) can all subscribe to the same
///   WSJT-X feed without fighting for the socket.
///
/// IPv6 multicast is not supported — WSJT-X is IPv4-only.
pub async fn bind(addr: SocketAddr) -> io::Result<UdpSocket> {
    if addr.ip().is_multicast() {
        bind_multicast(addr)
    } else {
        UdpSocket::bind(addr).await
    }
}

fn bind_multicast(addr: SocketAddr) -> io::Result<UdpSocket> {
    let group = match addr.ip() {
        IpAddr::V4(v4) => v4,
        IpAddr::V6(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IPv6 multicast is not supported (WSJT-X is IPv4-only)",
            ));
        },
    };
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    // SO_REUSEADDR + SO_REUSEPORT let multiple processes co-bind the
    // same multicast port. Without both, only the first process to
    // bind wins on Linux/macOS.
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    // Bind to the wildcard address (not the multicast addr itself):
    // on Linux either works, on macOS/BSD only the wildcard does.
    let bind_addr: SocketAddr = (Ipv4Addr::UNSPECIFIED, addr.port()).into();
    socket.bind(&bind_addr.into())?;
    let std_socket: std::net::UdpSocket = socket.into();
    let tokio_socket = UdpSocket::from_std(std_socket)?;
    // INADDR_ANY on the interface arg = "any interface that's in the
    // multicast routing table". For loopback-only setups (WSJT-X TTL 1
    // on the same host) the `lo` interface picks the packets up.
    tokio_socket.join_multicast_v4(group, Ipv4Addr::UNSPECIFIED)?;
    Ok(tokio_socket)
}

/// Spawn the WSJT-X listener and POST worker.
///
/// The listener reads from the pre-bound `socket`, parses
/// Logged-ADIF messages, and pipes them through a bounded queue to
/// the worker, which submits each QSO via
/// [`WavelogClient::push_qso`] with the given `station_id`.
///
/// `queue` enables on-disk persistence: the listener appends every
/// accepted ADIF to disk before queueing, and the worker removes the
/// entry only after a successful POST or a permanent
/// [`WavelogError::Rejected`] response. Pass `None` to run in pure
/// in-memory mode (the v1 behaviour); transient outages drop QSOs
/// after the standard `[0, 1, 4] s` retries exhaust.
///
/// `replay` is the set of entries already on disk at startup; they
/// get pushed onto the worker queue ahead of any new arrivals. Pass
/// an empty vec when persistence is disabled.
///
/// Returns both task join handles. Both observe the shutdown watch
/// channel and exit cleanly when it flips to `true` or its sender is
/// dropped.
#[must_use]
pub fn spawn(
    socket: UdpSocket,
    client: WavelogClient,
    station_id: Box<str>,
    queue: Option<Arc<QsoQueue>>,
    replay: Vec<(u64, Box<str>)>,
    shutdown: watch::Receiver<bool>,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<QueueItem>(QUEUE_CAPACITY);
    let listener = tokio::spawn(listen(socket, tx, queue.clone(), replay, shutdown.clone()));
    let worker = tokio::spawn(post_loop(rx, client, station_id, queue, shutdown));
    (listener, worker)
}

#[derive(Debug)]
struct QueueItem {
    /// Sequence number from the on-disk queue, so the worker can call
    /// `queue.remove(seq)` after a successful POST. `None` when
    /// persistence is disabled or the entry came from a path that
    /// didn't go through the queue.
    seq: Option<u64>,
    adif: Box<str>,
}

async fn listen(
    socket: UdpSocket,
    tx: mpsc::Sender<QueueItem>,
    queue: Option<Arc<QsoQueue>>,
    replay: Vec<(u64, Box<str>)>,
    mut shutdown: watch::Receiver<bool>,
) {
    if let Ok(addr) = socket.local_addr() {
        tracing::info!(addr = %addr, "wsjtx listener serving");
    }
    if *shutdown.borrow_and_update() {
        return;
    }

    // Prime the worker with replay entries before pulling any new
    // datagrams off the socket. send().await back-pressures on a full
    // channel so a deep replay (longer than QUEUE_CAPACITY) is fully
    // delivered to the worker rather than silently dropped. New
    // datagrams sit in the kernel UDP buffer until the recv loop
    // starts; in practice the priming completes in milliseconds even
    // for a maxed-out queue.
    for (seq, adif) in replay {
        tokio::select! {
            res = tx.send(QueueItem { seq: Some(seq), adif }) => {
                if res.is_err() {
                    tracing::warn!("wsjtx POST worker channel closed during replay; exiting listener");
                    return;
                }
            }
            result = shutdown.changed() => {
                let should_stop = result.is_err() || *shutdown.borrow();
                if should_stop {
                    tracing::info!("wsjtx listener shutting down (during replay)");
                    return;
                }
            }
        }
    }

    let mut buf = vec![0u8; RECV_BUF_SIZE];
    loop {
        tokio::select! {
            recv = socket.recv_from(&mut buf) => {
                let (n, from) = match recv {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(error = %e, "wsjtx recv_from failed");
                        continue;
                    }
                };
                match parse_logged_adif(&buf[..n]) {
                    Ok(Some(adif)) => {
                        tracing::debug!(
                            from = %from,
                            adif_len = adif.len(),
                            "wsjtx logged ADIF received",
                        );
                        let item = match &queue {
                            Some(q) => {
                                // Persist BEFORE handing off so a
                                // crash between accept and POST
                                // doesn't lose the QSO. Cloning the
                                // ADIF is cheap (typically <1 KB).
                                match q.append(adif.clone()).await {
                                    Ok(seq) => QueueItem { seq: Some(seq), adif },
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            from = %from,
                                            "wsjtx queue append failed; dropping ADIF",
                                        );
                                        continue;
                                    }
                                }
                            }
                            None => QueueItem { seq: None, adif },
                        };
                        match tx.try_send(item) {
                            Ok(()) => {},
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                tracing::warn!(
                                    "wsjtx POST queue full; dropping ADIF (still on disk if persistence is on)",
                                );
                            },
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                tracing::warn!("wsjtx POST worker channel closed; exiting listener");
                                return;
                            },
                        }
                    }
                    Ok(None) => {
                        tracing::debug!(from = %from, "wsjtx non-LoggedADIF message ignored");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, from = %from, "wsjtx parse failed");
                    }
                }
            },
            result = shutdown.changed() => {
                let should_stop = result.is_err() || *shutdown.borrow();
                if should_stop {
                    tracing::info!("wsjtx listener shutting down");
                    return;
                }
            }
        }
    }
}

async fn post_loop(
    mut rx: mpsc::Receiver<QueueItem>,
    client: WavelogClient,
    station_id: Box<str>,
    queue: Option<Arc<QsoQueue>>,
    mut shutdown: watch::Receiver<bool>,
) {
    if *shutdown.borrow_and_update() {
        return;
    }
    loop {
        tokio::select! {
            item = rx.recv() => match item {
                Some(item) => {
                    // Inner select makes the in-flight POST cancellable.
                    // Without it, a Wavelog outage holds the shutdown
                    // signal for up to ~15s ([0,1,4]s sleeps × 5s
                    // timeout) — longer than systemd's default
                    // TimeoutStopSec patience.
                    tokio::select! {
                        push_res = client.push_qso(&station_id, &item.adif) => {
                            handle_post_outcome(push_res, item.seq, queue.as_deref()).await;
                        },
                        result = shutdown.changed() => {
                            let should_stop = result.is_err() || *shutdown.borrow();
                            if should_stop {
                                tracing::info!("wsjtx POST worker shutting down (mid-push)");
                                return;
                            }
                        }
                    }
                }
                None => return,
            },
            result = shutdown.changed() => {
                let should_stop = result.is_err() || *shutdown.borrow();
                if should_stop {
                    tracing::info!("wsjtx POST worker shutting down");
                    return;
                }
            }
        }
    }
}

async fn handle_post_outcome(
    res: Result<(), WavelogError>,
    seq: Option<u64>,
    queue: Option<&QsoQueue>,
) {
    match res {
        Ok(()) => {
            tracing::info!("wsjtx QSO logged to wavelog");
            remove_persisted(seq, queue, "completed").await;
        },
        Err(WavelogError::Rejected { ref reason }) => {
            // Wavelog answered cleanly with a permanent "no" (duplicate,
            // validation error, etc). Retrying will produce the same
            // answer; drop the entry from disk too.
            tracing::warn!(reason = %reason, "wsjtx QSO rejected by wavelog");
            remove_persisted(seq, queue, "rejected").await;
        },
        Err(e) => {
            // Transport / 5xx / 4xx / BadResponse — keep on disk so
            // the next startup (or a future outage-recovery pass)
            // gets another shot.
            tracing::warn!(error = %e, "wsjtx QSO POST failed; entry retained on disk for retry");
        },
    }
}

async fn remove_persisted(seq: Option<u64>, queue: Option<&QsoQueue>, why: &'static str) {
    let (Some(seq), Some(queue)) = (seq, queue) else {
        return;
    };
    if let Err(e) = queue.remove(seq).await {
        tracing::warn!(error = %e, seq, why, "wsjtx queue remove failed");
    }
}

/// Parse a WSJT-X UDP datagram. Returns `Ok(Some(adif))` for a
/// `LoggedADIF` (type 12) message, `Ok(None)` for any other valid
/// WSJT-X message we don't care about, and `Err` for packets that
/// don't look like WSJT-X at all (bad magic, truncated, invalid UTF-8).
fn parse_logged_adif(bytes: &[u8]) -> Result<Option<Box<str>>, WsjtxError> {
    let mut c = Cursor::new(bytes);
    let magic = c.read_u32()?;
    if magic != MAGIC {
        return Err(WsjtxError::Parse(
            format!("bad magic: 0x{magic:08x}").into(),
        ));
    }
    // schema_version is read for the side effect of validating it
    // exists but we don't gate on a specific version — WSJT-X has only
    // ever appended fields to existing message types, so the prefix
    // we read for type 12 is stable across schemas.
    let _schema = c.read_u32()?;
    let message_type = c.read_u32()?;
    if message_type != MSG_TYPE_LOGGED_ADIF {
        return Ok(None);
    }
    // Type-12 body: `id` (sender program id like "WSJT-X"), `adif_text`.
    let _id = c.read_qstring()?;
    let adif = c.read_qstring()?;
    Ok(Some(adif))
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn read_u32(&mut self) -> Result<u32, WsjtxError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Read a Qt-encoded `QString`: signed `qint32` byte-length (big-
    /// endian) followed by UTF-8 bytes. A length of `-1` is "null" and
    /// resolves to an empty string.
    fn read_qstring(&mut self) -> Result<Box<str>, WsjtxError> {
        let len = self.read_u32()? as i32;
        if len < 0 {
            return Ok("".into());
        }
        let len = len as usize;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes)
            .map(Into::into)
            .map_err(|_| WsjtxError::Parse("invalid UTF-8 in QString field".into()))
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], WsjtxError> {
        let end = self.pos.checked_add(n).ok_or_else(|| {
            WsjtxError::Parse(format!("length overflow at offset {}", self.pos).into())
        })?;
        if end > self.buf.len() {
            return Err(WsjtxError::Parse(
                format!(
                    "truncated: need {n} bytes at offset {pos}, have {remaining}",
                    pos = self.pos,
                    remaining = self.buf.len() - self.pos,
                )
                .into(),
            ));
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::net::UdpSocket;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    /// Encode a Qt `QString` into a buffer: signed `qint32` big-endian
    /// length, then UTF-8 bytes.
    fn push_qstring(out: &mut Vec<u8>, s: &str) {
        let len = s.len() as i32;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(s.as_bytes());
    }

    fn encode_packet(message_type: u32, id: &str, body: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC.to_be_bytes());
        out.extend_from_slice(&3u32.to_be_bytes()); // schema version 3
        out.extend_from_slice(&message_type.to_be_bytes());
        push_qstring(&mut out, id);
        body(&mut out);
        out
    }

    fn encode_logged_adif(id: &str, adif: &str) -> Vec<u8> {
        encode_packet(MSG_TYPE_LOGGED_ADIF, id, |out| push_qstring(out, adif))
    }

    fn encode_qso_logged(id: &str) -> Vec<u8> {
        // Type 5 — we don't need to match the full body, just the
        // header + id is enough to assert the parser ignores it.
        encode_packet(5, id, |_| {})
    }

    #[test]
    fn parse_logged_adif_extracts_string() {
        let pkt = encode_logged_adif("WSJT-X", "<CALL:3>K1B <MODE:3>FT8 <EOR>");
        let adif = parse_logged_adif(&pkt).unwrap().unwrap();
        assert_eq!(&*adif, "<CALL:3>K1B <MODE:3>FT8 <EOR>");
    }

    #[test]
    fn parse_returns_none_for_non_logged_adif_messages() {
        let pkt = encode_qso_logged("WSJT-X");
        assert_eq!(parse_logged_adif(&pkt).unwrap(), None);
    }

    #[test]
    fn parse_rejects_wrong_magic() {
        let mut pkt = encode_logged_adif("WSJT-X", "x");
        pkt[0] = 0; // corrupt magic
        let err = parse_logged_adif(&pkt).unwrap_err();
        assert!(matches!(err, WsjtxError::Parse(_)), "got {err:?}");
    }

    #[test]
    fn parse_rejects_truncated_packet() {
        let pkt = encode_logged_adif("WSJT-X", "hello");
        // Cut off mid-way through the ADIF string.
        let err = parse_logged_adif(&pkt[..pkt.len() - 3]).unwrap_err();
        assert!(matches!(err, WsjtxError::Parse(_)), "got {err:?}");
    }

    #[test]
    fn parse_rejects_invalid_utf8_string() {
        let mut pkt = encode_logged_adif("WSJT-X", "");
        // Tack on a length prefix that looks valid, then non-UTF-8 bytes.
        pkt.extend_from_slice(&2i32.to_be_bytes());
        pkt.push(0xff);
        pkt.push(0xfe);
        // The packet now has a stray trailing string after the real
        // ADIF; the parser already consumed the valid payload, so the
        // garbage at the end is silently ignored. Build a fresh
        // pathological packet instead.
        let mut bad = Vec::new();
        bad.extend_from_slice(&MAGIC.to_be_bytes());
        bad.extend_from_slice(&3u32.to_be_bytes());
        bad.extend_from_slice(&MSG_TYPE_LOGGED_ADIF.to_be_bytes());
        push_qstring(&mut bad, "WSJT-X");
        bad.extend_from_slice(&2i32.to_be_bytes()); // len 2 for adif
        bad.push(0xff);
        bad.push(0xfe);
        let err = parse_logged_adif(&bad).unwrap_err();
        assert!(
            matches!(err, WsjtxError::Parse(ref msg) if msg.contains("UTF-8")),
            "got {err:?}",
        );
    }

    #[test]
    fn parse_handles_null_qstring_as_empty() {
        // schema 3, type 12, id len = -1 (null), adif len = 0
        let mut bad = Vec::new();
        bad.extend_from_slice(&MAGIC.to_be_bytes());
        bad.extend_from_slice(&3u32.to_be_bytes());
        bad.extend_from_slice(&MSG_TYPE_LOGGED_ADIF.to_be_bytes());
        bad.extend_from_slice(&(-1i32).to_be_bytes()); // null id
        bad.extend_from_slice(&0i32.to_be_bytes()); // empty adif
        let adif = parse_logged_adif(&bad).unwrap().unwrap();
        assert_eq!(&*adif, "");
    }

    #[tokio::test]
    async fn listener_to_worker_round_trip_posts_to_wavelog() {
        // Real UDP socket on an ephemeral port + wiremock standing in
        // for Wavelog's `/api/qso`.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "created",
            })))
            .mount(&server)
            .await;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = socket.local_addr().unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) =
            spawn(socket, client, "7".into(), None, Vec::new(), shutdown_rx);

        // Send the WSJT-X packet from a separate socket.
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let pkt = encode_logged_adif("WSJT-X", "<CALL:5>VK3AB <MODE:3>FT8 <EOR>");
        sender.send_to(&pkt, listen_addr).await.unwrap();

        // Spin for up to a few seconds for the POST to land.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let requests = server.received_requests().await.unwrap_or_default();
            if !requests.is_empty() {
                let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
                assert_eq!(body["key"], "test-key");
                assert_eq!(body["station_profile_id"], "7");
                assert_eq!(body["type"], "adif");
                assert_eq!(body["string"], "<CALL:5>VK3AB <MODE:3>FT8 <EOR>");
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("wavelog never received the QSO POST");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    }

    #[tokio::test]
    async fn listener_ignores_non_logged_adif_messages() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "created",
            })))
            .mount(&server)
            .await;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = socket.local_addr().unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) =
            spawn(socket, client, "1".into(), None, Vec::new(), shutdown_rx);

        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .send_to(&encode_qso_logged("WSJT-X"), listen_addr)
            .await
            .unwrap();

        // Give the listener time to receive + discard.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let requests = server.received_requests().await.unwrap_or_default();
        assert!(
            requests.is_empty(),
            "type 5 (QSO Logged) should not have produced a POST: got {requests:?}",
        );

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    }

    // -- bind() helper --

    #[tokio::test]
    async fn bind_unicast_loopback_works() {
        let sock = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = sock.local_addr().unwrap();
        assert_eq!(addr.ip(), std::net::IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert!(addr.port() > 0);
    }

    #[tokio::test]
    async fn bind_rejects_ipv6_multicast() {
        // ff02::1 = all-nodes link-local multicast. WSJT-X doesn't use
        // this, but the path must produce a clean error rather than a
        // panic or a confusing libc error.
        let err = bind("[ff02::1]:2237".parse().unwrap()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn multicast_bind_succeeds() {
        // Just verify the multicast bind path produces a usable
        // socket. End-to-end receive is OS-dependent and tested via
        // manual smoke (see README) rather than CI.
        let port = pick_free_udp_port().await;
        let addr: SocketAddr = format!("224.0.7.7:{port}").parse().unwrap();
        let sock = bind(addr).await.expect("multicast bind failed");
        // After bind, local_addr reflects the wildcard (0.0.0.0:port)
        // since we deliberately bind INADDR_ANY for multicast.
        assert_eq!(sock.local_addr().unwrap().port(), port);
    }

    #[tokio::test]
    async fn multicast_bind_allows_concurrent_binders() {
        // The whole point of SO_REUSEADDR/SO_REUSEPORT for multicast:
        // multiple processes (us + GridTracker, in production) must
        // be able to claim the same UDP port without one losing to
        // `EADDRINUSE`.
        let port = pick_free_udp_port().await;
        let addr: SocketAddr = format!("224.0.7.7:{port}").parse().unwrap();
        let _a = bind(addr).await.expect("first multicast bind");
        let _b = bind(addr).await.expect("second multicast bind");
    }

    /// Pick a UDP port that's currently free by binding 127.0.0.1:0
    /// and reading what the kernel allocated. The probe socket is
    /// dropped before return; there's a race window where another
    /// process could steal it, but for in-process tests it's fine.
    async fn pick_free_udp_port() -> u16 {
        let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        port
    }

    #[tokio::test]
    async fn shutdown_signal_stops_listener_and_worker() {
        let server = MockServer::start().await;
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client = WavelogClient::new(&server.uri(), "k").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) =
            spawn(socket, client, "1".into(), None, Vec::new(), shutdown_rx);

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_millis(500), listener_task)
            .await
            .expect("listener didn't exit within 500ms")
            .expect("listener panicked");
        tokio::time::timeout(Duration::from_millis(500), worker_task)
            .await
            .expect("worker didn't exit within 500ms")
            .expect("worker panicked");
    }

    #[tokio::test]
    async fn shutdown_during_inflight_qso_post_returns_promptly() {
        // Wavelog mock holds every POST for 60s. Without cancellation-
        // aware shutdown the worker would block for the full 5s
        // timeout × 3 retries. Tight the assert to 1s to lock that in.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(60)))
            .mount(&server)
            .await;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = socket.local_addr().unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) =
            spawn(socket, client, "1".into(), None, Vec::new(), shutdown_rx);

        // Push one ADIF in to start a POST.
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .send_to(
                &encode_logged_adif("WSJT-X", "<CALL:5>VK3AB <EOR>"),
                listen_addr,
            )
            .await
            .unwrap();

        // Give the worker time to pick up the ADIF and start the POST.
        tokio::time::sleep(Duration::from_millis(150)).await;

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), listener_task)
            .await
            .expect("listener did not exit within 1s of shutdown")
            .expect("listener panicked");
        tokio::time::timeout(Duration::from_secs(1), worker_task)
            .await
            .expect("worker did not exit within 1s of shutdown (in-flight POST not cancelled)")
            .expect("worker panicked");
    }

    #[tokio::test]
    async fn persisted_adif_is_removed_from_disk_after_successful_post() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "created",
            })))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let queue_path = dir.path().join("queue.jsonl");
        let (queue, _replay) = QsoQueue::open(queue_path.clone()).await.unwrap();
        let queue = std::sync::Arc::new(queue);

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = socket.local_addr().unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) = spawn(
            socket,
            client,
            "7".into(),
            Some(queue.clone()),
            Vec::new(),
            shutdown_rx,
        );

        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .send_to(
                &encode_logged_adif("WSJT-X", "<CALL:5>VK3AB <EOR>"),
                listen_addr,
            )
            .await
            .unwrap();

        // Spin until the queue drains (POST landed → remove called).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            if queue.len().await == 0 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "queue still holds {} entries after success",
                    queue.len().await,
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;

        // Re-open the file: zero entries means the worker actually
        // wrote the removal back to disk, not just memory.
        drop(queue);
        let (_reopened, replay) = QsoQueue::open(queue_path).await.unwrap();
        assert!(
            replay.is_empty(),
            "queue file should be empty after success"
        );
    }

    #[tokio::test]
    async fn persisted_adif_is_kept_when_wavelog_returns_5xx() {
        // 5xx is retryable. After exhausting [0,1,4]s retries the
        // worker must leave the entry on disk for next-startup replay.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let queue_path = dir.path().join("queue.jsonl");
        let (queue, _replay) = QsoQueue::open(queue_path.clone()).await.unwrap();
        let queue = std::sync::Arc::new(queue);

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = socket.local_addr().unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) = spawn(
            socket,
            client,
            "7".into(),
            Some(queue.clone()),
            Vec::new(),
            shutdown_rx,
        );

        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .send_to(
                &encode_logged_adif("WSJT-X", "<CALL:5>VK3AB <EOR>"),
                listen_addr,
            )
            .await
            .unwrap();

        // Wait long enough for [0,1,4]s retry exhaustion (~5s).
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert_eq!(
            queue.len().await,
            1,
            "5xx after retry exhaustion must keep entry on disk for next-startup replay",
        );

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    }

    #[tokio::test]
    async fn replay_entries_are_pushed_through_worker_at_startup() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/qso"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "created",
            })))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let queue_path = dir.path().join("queue.jsonl");

        // Pre-seed the queue with 2 entries (simulating leftover from
        // a prior daemon run).
        let (queue, _) = QsoQueue::open(queue_path.clone()).await.unwrap();
        queue.append("<CALL:5>K1AAA <EOR>".into()).await.unwrap();
        queue.append("<CALL:5>K1BBB <EOR>".into()).await.unwrap();
        drop(queue);

        // Re-open and feed the replay back into a fresh spawn.
        let (queue, replay) = QsoQueue::open(queue_path.clone()).await.unwrap();
        let queue = std::sync::Arc::new(queue);
        assert_eq!(replay.len(), 2);

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client = WavelogClient::new(&server.uri(), "test-key").unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (listener_task, worker_task) = spawn(
            socket,
            client,
            "7".into(),
            Some(queue.clone()),
            replay.into_vec(),
            shutdown_rx,
        );

        // Spin for both replays to land.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let posts = server.received_requests().await.unwrap_or_default().len();
            if posts >= 2 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("expected 2 replayed POSTs, got {posts}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // And the queue should now be empty in memory + on disk.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if queue.len().await == 0 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("queue did not drain after replay+success");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), listener_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    }
}
