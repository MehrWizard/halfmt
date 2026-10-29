//! halfmt server mode — the split's "back half".
//!
//! Accepts connections the front half opened through the S3 tunnel, reads the
//! one-line DC-address preamble, dials Telegram, and pipes raw bytes. All
//! Telegram-facing sockets (and therefore keepalives) live on this side, so
//! tunnel blips or outages only stall byte flow: sessions resume without any
//! reconnect to Telegram, which is the entire point of the split.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, split};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

use crate::config::ProxyConfig;
use crate::error::{ProxyError, Result};
use crate::proxy::direct_buffer_budget::DirectBufferBudget;
use crate::proxy::relay::relay_direct_adaptive;
use crate::stats::Stats;
use crate::transport::UpstreamManager;

const PREAMBLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Parses the front half's DC preamble: a single "ip:port" line.
pub(crate) fn parse_preamble(line: &str) -> std::result::Result<SocketAddr, String> {
    let s = line.trim();
    s.parse::<SocketAddr>()
        .map_err(|e| format!("bad halfmt preamble {s:?}: {e}"))
}

/// Runs the split-connection accept loop until the process exits.
pub(crate) async fn run_listener(
    listener: TcpListener,
    config: Arc<ProxyConfig>,
    stats: Arc<Stats>,
    upstream_manager: Arc<UpstreamManager>,
    direct_buffer_budget: Arc<DirectBufferBudget>,
) {
    let addr = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "?".to_string());
    info!(%addr, "halfmt server: split listener ready");
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let config = config.clone();
                let stats = stats.clone();
                let upstreams = upstream_manager.clone();
                let budget = direct_buffer_budget.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_conn(stream, peer, config, stats, upstreams, budget).await
                    {
                        warn!(%peer, error = %e, "halfmt server: connection ended");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "halfmt server: accept failed");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn serve_conn(
    stream: tokio::net::TcpStream,
    peer: SocketAddr,
    config: Arc<ProxyConfig>,
    stats: Arc<Stats>,
    upstream_manager: Arc<UpstreamManager>,
    direct_buffer_budget: Arc<DirectBufferBudget>,
) -> Result<()> {
    let _ = stream.set_nodelay(true);
    let mut stream = stream;

    // Preamble: one line, read byte-exact so no client bytes can be consumed
    // past the newline (they must all reach Telegram untouched).
    let deadline = tokio::time::Instant::now() + PREAMBLE_TIMEOUT;
    let mut line = Vec::with_capacity(48);
    loop {
        let mut byte = [0u8; 1];
        tokio::time::timeout_at(deadline, stream.read_exact(&mut byte))
            .await
            .map_err(|_| ProxyError::ConnectionTimeout {
                addr: format!("halfmt preamble from {peer}"),
            })?
            .map_err(ProxyError::Io)?;
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
        if line.len() > 256 {
            return Err(ProxyError::Config("halfmt preamble too long".to_string()));
        }
    }
    let text = String::from_utf8_lossy(&line);
    let dc_addr = parse_preamble(&text).map_err(ProxyError::Config)?;
    debug!(%peer, %dc_addr, "halfmt server: preamble ok, dialing Telegram");

    let connect_budget = Duration::from_secs(config.general.tg_connect.max(1));
    let tg_stream = tokio::time::timeout(
        connect_budget,
        upstream_manager.connect(dc_addr, None, None),
    )
    .await
    .map_err(|_| ProxyError::ConnectionTimeout {
        addr: dc_addr.to_string(),
    })?
    .map_err(|e| ProxyError::Config(format!("dc connect {dc_addr}: {e}")))?;

    let (client_reader, client_writer) = stream.into_split();
    let (server_reader, server_writer) = split(tg_stream);
    let user = format!("halfmt-{}", peer.ip());

    relay_direct_adaptive(
        client_reader,
        client_writer,
        server_reader,
        server_writer,
        config.general.direct_relay_copy_buf_c2s_bytes,
        config.general.direct_relay_copy_buf_s2c_bytes,
        config.server.max_connections,
        &user,
        Arc::clone(&stats),
        stats.current_user_quota_handle(&user),
        None,
        None,
        Duration::from_secs(1800),
        tokio_util::sync::CancellationToken::new(),
        direct_buffer_budget,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::parse_preamble;

    #[test]
    fn preamble_parses_v4_and_v6() {
        assert_eq!(
            parse_preamble("149.154.167.50:443").unwrap(),
            "149.154.167.50:443"
                .parse::<std::net::SocketAddr>()
                .unwrap()
        );
        assert_eq!(
            parse_preamble("  [2001:db8::1]:443\n").unwrap(),
            "[2001:db8::1]:443".parse::<std::net::SocketAddr>().unwrap()
        );
    }

    #[test]
    fn preamble_rejects_garbage() {
        assert!(parse_preamble("").is_err());
        assert!(parse_preamble("dc=2").is_err());
        assert!(parse_preamble("149.154.167.50").is_err());
    }
}
