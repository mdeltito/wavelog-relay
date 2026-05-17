//! UDP socket binding for the WSJT-X listener.
//!
//! Handles unicast and IPv4 multicast. Multicast sets both
//! `SO_REUSEADDR` and `SO_REUSEPORT` so we co-bind a port with
//! GridTracker2 / JTAlert / log4om without `EADDRINUSE`; both flags
//! are needed because peers using either alone won't share a socket
//! with a strict subset.
//!
//! Linux binds the multicast group address directly so the listener
//! only sees that group's traffic. macOS/BSD reject that and require
//! a wildcard bind, which also catches unicast UDP to the same port —
//! a foot-gun with no clean userland filter short of `IP_PKTINFO`.
//!
//! WSJT-X is IPv4-only, so IPv6 multicast is rejected explicitly.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use tokio::net::UdpSocket;

/// Bind a UDP socket for receiving WSJT-X datagrams. Detects whether
/// `addr` is unicast or IPv4 multicast and configures the socket
/// accordingly:
///
/// - **Unicast** (e.g. `127.0.0.1:2237`): a plain `UdpSocket::bind`.
///   Only one process can hold the port.
/// - **IPv4 multicast** (e.g. `224.0.0.1:2237`): `SO_REUSEADDR` +
///   `SO_REUSEPORT` (Unix), bind to the group address on Linux or
///   `0.0.0.0:<port>` elsewhere, then join the multicast group on the
///   wildcard interface. Multiple processes (GridTracker, JTAlert,
///   us, …) can all subscribe to the same WSJT-X feed without
///   fighting for the socket.
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
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;

    #[cfg(target_os = "linux")]
    let bind_addr: SocketAddr = (group, addr.port()).into();
    #[cfg(not(target_os = "linux"))]
    let bind_addr: SocketAddr = (Ipv4Addr::UNSPECIFIED, addr.port()).into();
    socket.bind(&bind_addr.into())?;

    let std_socket: std::net::UdpSocket = socket.into();
    let tokio_socket = UdpSocket::from_std(std_socket)?;
    // INADDR_ANY = any multicast-enabled interface (incl. `lo` for loopback TTL=1).
    tokio_socket.join_multicast_v4(group, Ipv4Addr::UNSPECIFIED)?;
    Ok(tokio_socket)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

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

    /// Multicast sender pinned to `lo` so tests work without a default route.
    fn make_loopback_multicast_sender() -> UdpSocket {
        let s = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )
        .unwrap();
        s.set_nonblocking(true).unwrap();
        let bind_addr: SocketAddr = (Ipv4Addr::UNSPECIFIED, 0).into();
        s.bind(&bind_addr.into()).unwrap();
        s.set_multicast_if_v4(&Ipv4Addr::LOCALHOST).unwrap();
        s.set_multicast_loop_v4(true).unwrap();
        let std_s: std::net::UdpSocket = s.into();
        UdpSocket::from_std(std_s).unwrap()
    }

    #[tokio::test]
    async fn bind_unicast_loopback_works() {
        let sock = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = sock.local_addr().unwrap();
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
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
        // Verify the multicast bind path produces a usable socket.
        let port = pick_free_udp_port().await;
        let addr: SocketAddr = format!("224.0.0.1:{port}").parse().unwrap();
        let sock = bind(addr).await.expect("multicast bind failed");
        assert_eq!(sock.local_addr().unwrap().port(), port);
    }

    #[tokio::test]
    async fn multicast_two_binders_both_receive_sent_datagram() {
        // Real proof that co-binding works *and* both sockets actually
        // receive: the bind-only assertion missed the case where
        // delivery breaks downstream (the foot-gun the previous staged
        // change was meant to fix).
        let port = pick_free_udp_port().await;
        let group: Ipv4Addr = "224.0.0.1".parse().unwrap();
        let group_addr: SocketAddr = (group, port).into();

        let a = bind(group_addr).await.expect("first multicast bind");
        let b = bind(group_addr).await.expect("second multicast bind");

        let sender = make_loopback_multicast_sender();
        sender.send_to(b"hello", group_addr).await.unwrap();

        let mut buf_a = [0u8; 64];
        let mut buf_b = [0u8; 64];
        let (got_a, got_b) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(1), a.recv_from(&mut buf_a)),
            tokio::time::timeout(Duration::from_secs(1), b.recv_from(&mut buf_b)),
        );
        let (n_a, _) = got_a
            .expect("socket A timed out waiting for multicast")
            .expect("socket A recv_from failed");
        let (n_b, _) = got_b
            .expect("socket B timed out waiting for multicast")
            .expect("socket B recv_from failed");
        assert_eq!(&buf_a[..n_a], b"hello");
        assert_eq!(&buf_b[..n_b], b"hello");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn multicast_bind_ignores_unicast_to_same_port_on_linux() {
        // Regression guard for the "incidentally catches unicast" foot-
        // gun: when the multicast bind was 0.0.0.0:port + group join,
        // it picked up unicast packets to 127.0.0.1:port too. Locking
        // in the group-address bind on Linux prevents that and makes
        // misconfiguration (WSJT-X unicasting when wavelog-relay is
        // bound to multicast) visible immediately instead of silently
        // appearing to work until a peer like GT2 takes the port.
        let port = pick_free_udp_port().await;
        let group: Ipv4Addr = "224.0.0.1".parse().unwrap();
        let group_addr: SocketAddr = (group, port).into();

        let mcast = bind(group_addr).await.expect("multicast bind");

        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let unicast_dst: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        sender
            .send_to(b"should-not-arrive", unicast_dst)
            .await
            .unwrap();

        let mut buf = [0u8; 64];
        let res = tokio::time::timeout(Duration::from_millis(300), mcast.recv_from(&mut buf)).await;
        assert!(
            res.is_err(),
            "multicast socket received unicast packet (got {:?})",
            res.map(|r| r.map(|(n, _)| std::str::from_utf8(&buf[..n])
                .unwrap_or("<binary>")
                .to_owned())),
        );
    }
}
