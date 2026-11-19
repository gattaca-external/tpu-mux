use crate::buffers::BufferPool;
use anyhow::Result;
use proto::{Connection, ConnectionHandle, DatagramEvent, Endpoint, ServerConfig, Transmit};
use quinn_udp::{RecvMeta, UdpSocketState};
use std::{
    io::{self, IoSliceMut},
    net::{SocketAddr, UdpSocket},
    sync::Arc,
    time::Instant,
};

const LISTENER_RCVBUF: usize = 4 * 1024 * 1024;
pub const INGRESS_BATCH: usize = 8;
const RX_BYTES: usize = 65_536;
const MAX_GSO_SEGMENTS: usize = 8;

pub struct Driver<const RX_BATCH: usize = 1> {
    pub ep: Endpoint,
    sock: UdpSocket,
    io: UdpSocketState,
    rx: [Box<[u8]>; RX_BATCH],
    packets: BufferPool,
    meta: [RecvMeta; RX_BATCH],
    slot: usize,
    filled: usize,
    offset: usize,
    pub tx: Vec<u8>,
}
impl<const RX_BATCH: usize> Driver<RX_BATCH> {
    pub fn bind(addr: SocketAddr, server: Option<ServerConfig>) -> Result<Self> {
        let sock = UdpSocket::bind(addr)?;
        sock.set_nonblocking(true)?;
        let io = UdpSocketState::new((&sock).into())?;
        if server.is_some()
            && io
                .recv_buffer_size((&sock).into())
                .is_ok_and(|bytes| bytes < LISTENER_RCVBUF)
            && let Err(error) = io.set_recv_buffer_size((&sock).into(), LISTENER_RCVBUF)
        {
            eprintln!("could not enlarge listener receive buffer: {error}");
        }
        let packets = BufferPool::new(if server.is_some() { 64 } else { 4 }, RX_BYTES);
        let ep = Endpoint::new(
            Default::default(),
            server.map(Arc::new),
            !io.may_fragment(),
            None,
        );
        Ok(Self {
            ep,
            sock,
            io,
            rx: std::array::from_fn(|_| vec![0; RX_BYTES].into_boxed_slice()),
            packets,
            slot: 0,
            filled: 0,
            offset: 0,
            meta: [RecvMeta::default(); RX_BATCH],
            tx: Vec::with_capacity(8192),
        })
    }
    pub fn addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }
    pub fn has_buffered_datagram(&self) -> bool {
        self.slot < self.filled
    }

    pub fn recv(&mut self, now: Instant) -> io::Result<Option<DatagramEvent>> {
        if self.slot == self.filled {
            let mut bufs = self.rx.each_mut().map(|buf| IoSliceMut::new(buf));
            match self.io.recv((&self.sock).into(), &mut bufs, &mut self.meta) {
                Ok(0) => return Ok(None),
                Ok(n) => {
                    self.slot = 0;
                    self.filled = n;
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        }
        let meta = self.meta[self.slot];
        if meta.stride == 0 || meta.len == 0 {
            self.slot += 1;
            self.offset = 0;
            return Ok(None);
        }
        let end = (self.offset + meta.stride).min(meta.len);
        let datagram = &self.rx[self.slot][self.offset..end];
        self.offset = end;
        if end == meta.len {
            self.slot += 1;
            self.offset = 0;
        }
        let Some(buf) = self.packets.writable(datagram.len()) else {
            return Ok(None);
        };
        buf.extend_from_slice(datagram);
        let bytes = buf.split();
        self.tx.clear();
        Ok(self.ep.handle(
            now,
            meta.addr,
            meta.dst_ip,
            meta.ecn
                .and_then(|e| proto::EcnCodepoint::from_bits(e as u8)),
            bytes,
            &mut self.tx,
        ))
    }
    pub fn send(&self, t: Transmit) -> io::Result<()> {
        let msg = quinn_udp::Transmit {
            destination: t.destination,
            ecn: t
                .ecn
                .and_then(|e| quinn_udp::EcnCodepoint::from_bits(e as u8)),
            contents: &self.tx[..t.size],
            segment_size: t.segment_size,
            src_ip: t.src_ip,
        };
        match self.io.send((&self.sock).into(), &msg) {
            Ok(()) => Ok(()),
            // A congested socket drops this datagram. QUIC can retransmit stream frames.
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
    pub fn flush(
        &mut self,
        h: ConnectionHandle,
        conn: &mut Connection,
        now: Instant,
    ) -> io::Result<()> {
        self.flush_some(h, conn, now, usize::MAX).map(|_| ())
    }

    // Return true if the next poll should continue transmitting immediately.
    pub fn flush_some(
        &mut self,
        h: ConnectionHandle,
        conn: &mut Connection,
        now: Instant,
        budget: usize,
    ) -> io::Result<bool> {
        while let Some(ev) = conn.poll_endpoint_events() {
            if let Some(ev) = self.ep.handle_event(h, ev) {
                conn.handle_event(ev);
            }
        }
        for _ in 0..budget {
            self.tx.clear();
            let Some(tx) = conn.poll_transmit(
                now,
                self.io.max_gso_segments().min(MAX_GSO_SEGMENTS),
                &mut self.tx,
            ) else {
                return Ok(false);
            };
            self.send(tx)?;
        }
        Ok(true)
    }
}
