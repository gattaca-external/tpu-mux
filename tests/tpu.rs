use std::{
    io::{BufRead, BufReader},
    net::SocketAddr,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use quinn::{
    ClientConfig, Connection, Endpoint, ServerConfig, TransportConfig,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
};
use solana_keypair::Keypair;
use solana_tls_utils::{
    get_remote_pubkey, new_dummy_x509_certificate, socket_addr_to_quic_server_name,
    tls_client_config_builder, tls_server_config_builder,
};
use tokio::time::timeout;

struct Mux(Child);

impl Drop for Mux {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn receiver(stalled: bool) -> Result<Endpoint> {
    let (cert, key) = new_dummy_x509_certificate(&Keypair::new());
    let mut tls = tls_server_config_builder().with_single_cert(vec![cert], key)?;
    tls.alpn_protocols = vec![b"solana-tpu".to_vec()];
    let mut config = ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls)?));
    if stalled {
        let mut limits = TransportConfig::default();
        limits
            .max_concurrent_uni_streams(0u8.into())
            .stream_receive_window(512u32.into());
        config.transport_config(Arc::new(limits));
    }
    Ok(Endpoint::server(config, "127.0.0.1:0".parse()?)?)
}

async fn next(conn: &Connection) -> Result<Vec<u8>> {
    Ok(conn.accept_uni().await?.read_to_end(4096).await?)
}

#[tokio::test]
async fn forwards_opaque_streams_independently() -> Result<()> {
    let a = receiver(false)?;
    let b = receiver(true)?;
    let mut mux = Mux(Command::new(env!("CARGO_BIN_EXE_tpu-mux"))
        .args(["--listen", "127.0.0.1:0", "--outputs"])
        .args([a.local_addr()?.to_string(), b.local_addr()?.to_string()])
        .stderr(Stdio::piped())
        .spawn()?);
    let mut log = BufReader::new(mux.0.stderr.take().unwrap());
    let mut line = String::new();
    let listen: SocketAddr = loop {
        line.clear();
        anyhow::ensure!(log.read_line(&mut line)? > 0, "mux exited during startup");
        if let Some(addr) = line
            .strip_prefix("listening on ")
            .and_then(|s| s.split_whitespace().next())
        {
            break addr.parse()?;
        }
    };
    timeout(Duration::from_secs(10), async {
        let left = a.accept().await.unwrap().await?;
        let right = b.accept().await.unwrap().await?;
        let sender_key = Keypair::new();
        let (cert, key) = new_dummy_x509_certificate(&sender_key);
        let mut tls = tls_client_config_builder().with_client_auth_cert(vec![cert.clone()], key)?;
        tls.alpn_protocols = vec![b"solana-tpu".to_vec()];
        let mut sender = Endpoint::client("127.0.0.1:0".parse()?)?;
        sender.set_default_client_config(ClientConfig::new(Arc::new(QuicClientConfig::try_from(
            tls,
        )?)));
        let conn = sender
            .connect(listen, &socket_addr_to_quic_server_name(listen))?
            .await?;

        let mux_key = get_remote_pubkey(&left).expect("mux certificate");
        assert_eq!(Some(mux_key), get_remote_pubkey(&right));
        assert_ne!(
            Some(mux_key),
            solana_tls_utils::get_pubkey_from_tls_certificate(&cert)
        );

        let mut oversized = conn.open_uni().await?;
        oversized.write_all(&[0; 4097]).await?;
        oversized.finish()?;
        let payload: Vec<u8> = (0..4096).map(|i| (i / 256) as u8).collect();
        let mut valid = conn.open_uni().await?;
        valid.write_all(&payload[..17]).await?;
        let mut short = conn.open_uni().await?;
        short.write_all(b"complete").await?;
        short.finish()?;
        assert_eq!(next(&left).await?, b"complete");
        valid.write_all(&payload[17..]).await?;
        valid.finish()?;
        assert_eq!(next(&left).await?, payload);
        right.set_max_concurrent_uni_streams(32u8.into());
        assert_eq!(next(&right).await?, b"complete");
        assert_eq!(next(&right).await?, payload);

        right.close(0u8.into(), b"offline");
        let mut stream = conn.open_uni().await?;
        stream.write_all(b"still live").await?;
        stream.finish()?;
        assert_eq!(next(&left).await?, b"still live");
        let recovered = b.accept().await.unwrap().await?;
        // This endpoint advertises zero stream credit. Outlive the local write deadline.
        for _ in 0..5 {
            let mut stream = conn.open_uni().await?;
            stream.write_all(b"waiting").await?;
            stream.finish()?;
            assert_eq!(next(&left).await?, b"waiting");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(recovered.close_reason().is_none());
        recovered.set_max_concurrent_uni_streams(32u8.into());
        // Once this handshake completes the second path can resume at the live edge.
        // Probe until the reconnecting output has advanced past its outage backlog.
        let mut probe = tokio::time::interval(Duration::from_millis(10));
        let delivery = async {
            loop {
                let bytes = next(&recovered).await?;
                if bytes != b"waiting" {
                    return anyhow::Ok(bytes);
                }
            }
        };
        tokio::pin!(delivery);
        loop {
            tokio::select! {
                result = &mut delivery => { assert_eq!(result?, b"fresh"); break; }
                _ = probe.tick() => {
                    let mut stream = conn.open_uni().await?;
                    stream.write_all(b"fresh").await?;
                    stream.finish()?;
                }
            }
        }
        anyhow::Ok(())
    })
    .await??;
    Ok(())
}
