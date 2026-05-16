//! UDP socket binding for the WSJT-X listener.
//!
//! Handles both unicast and IPv4 multicast addresses. The multicast
//! path uses `socket2` to set `SO_REUSEADDR`/`SO_REUSEPORT` before
//! binding the wildcard interface and joining the group, which is what
//! lets wavelog-bridge co-bind the same multicast port as
//! GridTracker2 / JTAlert / log4om without `EADDRINUSE`. WSJT-X is
//! IPv4-only, so IPv6 multicast is rejected explicitly rather than
//! produced as a confusing libc error.

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

#[cfg(test)]
mod tests {
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
}
