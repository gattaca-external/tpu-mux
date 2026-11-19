# tpu-mux

A standalone Solana TPU multiplexer. It accepts QUIC connections on one address and forwards each completed, nonempty unidirectional stream to two TPU destinations. Both destinations receive the original payload bytes on a fresh stream over their own warm connection.

## Run

The mux uses three busy-polling Flux tiles. The first CPU in its Linux affinity mask drives the listener. The next two CPUs each drive one destination. Select three separate physical cores with `taskset`.

```sh
cargo build --release --locked
taskset -c 4,5,6 ./target/release/tpu-mux \
  --listen 0.0.0.0:8006 \
  --outputs 192.0.2.10:8006 192.0.2.20:8006 \
  --identity /etc/tpu-mux/identity.json
```

Use the actual QUIC ports for both destinations. Keep routes to the destinations independent of the advertised listener address to avoid forwarding loops. The optional identity is a Solana JSON keypair. Omitting it generates a new identity on each start.

## Identity and delivery

Both destinations see the mux's public key and source address. Receivers that use stake-based admission apply it to the mux identity for the entire feed. Use the normal TPU port unless the receiver admits that identity on its forwards port.

The mux forwards payloads of 1 to 4,096 bytes without signature checks or deduplication. Each incoming stream has two seconds to complete.

Delivery is best effort. A slow or disconnected destination can miss transactions while the other continues receiving. After reconnecting, the mux resumes with live traffic. Check stderr for messages about skipped transactions.
