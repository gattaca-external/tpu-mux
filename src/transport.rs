use std::{sync::Arc, time::Duration};

use anyhow::Result;
use proto::{
    ClientConfig, ServerConfig, TransportConfig, VarInt,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
};
use solana_keypair::Keypair;
use solana_tls_utils::{
    new_dummy_x509_certificate, tls_client_config_builder, tls_server_config_builder,
};

use crate::{MAX_PAYLOAD, STREAMS_PER_PEER};

pub fn configs(key: &Keypair) -> Result<(ServerConfig, ClientConfig)> {
    let (cert, private_key) = new_dummy_x509_certificate(key);
    let mut tls = tls_server_config_builder()
        .with_single_cert(vec![cert.clone()], private_key.clone_key())?;
    tls.alpn_protocols = vec![b"solana-tpu".to_vec()];

    let mut transport = TransportConfig::default();
    transport
        .max_concurrent_bidi_streams(0u8.into())
        .max_concurrent_uni_streams(VarInt::from_u32(STREAMS_PER_PEER as u32))
        .stream_receive_window(VarInt::from_u32((MAX_PAYLOAD + 1) as u32))
        .receive_window(VarInt::from_u32(
            ((MAX_PAYLOAD + 1) * STREAMS_PER_PEER) as u32,
        ))
        .datagram_receive_buffer_size(None);
    let mut server = ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls)?));
    server.transport_config(Arc::new(transport));

    let mut tls = tls_client_config_builder().with_client_auth_cert(vec![cert], private_key)?;
    tls.alpn_protocols = vec![b"solana-tpu".to_vec()];
    let mut transport = TransportConfig::default();
    transport
        .send_fairness(false)
        .keep_alive_interval(Some(Duration::from_secs(5)))
        .max_concurrent_bidi_streams(0u8.into())
        .max_concurrent_uni_streams(0u8.into())
        .datagram_receive_buffer_size(None);
    let mut client = ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls)?));
    client.transport_config(Arc::new(transport));
    Ok((server, client))
}
