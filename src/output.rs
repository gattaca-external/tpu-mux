use crate::{MAX_PAYLOAD, MuxSpine, STREAM_TIMEOUT, buffers::BufferPool, socket::Driver};
use anyhow::{Result, bail};
use bytes::Bytes;
use flux::{
    communication::{ReadError, queue::ConsumerBare},
    core_affinity::CoreId,
    spine::SpineAdapter,
    tile::{Tile, TileName},
    utils::{DCache, DCacheRef},
};
use proto::{
    ClientConfig, Connection, ConnectionHandle, DatagramEvent, Dir, Event, StreamId, WriteError,
};
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

const SEND_BLOCK_SIZE: usize = 64 * 1024;
const SEND_BLOCKS: usize = (128 * 1024 * 1024) / SEND_BLOCK_SIZE;
const MAX_STREAMS_PER_FLUSH: usize = 8;
const IDLE_RX_INTERVAL: Duration = Duration::from_micros(1);

struct Pending {
    id: Option<StreamId>,
    payload: Bytes,
    deadline: Instant,
}

struct Backlog {
    reader: ConsumerBare<DCacheRef>,
    cache: Arc<DCache>,
    cursor: usize,
    buffers: BufferPool,
    waiting_for_buffer: bool,
}

impl Backlog {
    fn next_payload(&mut self, addr: SocketAddr) -> Result<Option<Bytes>> {
        let Some(buf) = self.buffers.writable(MAX_PAYLOAD) else {
            self.waiting_for_buffer = true;
            return Ok(None);
        };
        self.waiting_for_buffer = false;
        let mut entry = DCacheRef::NONE;
        let (slot, epoch) = match self.reader.try_consume_with_epoch(&mut entry) {
            Ok(position) => position,
            Err(ReadError::Empty) => return Ok(None),
            Err(ReadError::SpedPast) => {
                self.skip(addr, "queue overrun");
                return Ok(None);
            }
        };
        self.cursor += 1;
        // QUIC retains these bytes for retransmission. Check the queue epoch after the copy.
        let bytes = self.cache.map(entry, |payload| {
            buf.extend_from_slice(payload);
            buf.split().freeze()
        })?;
        if self.reader.slot_version(slot) != epoch {
            self.skip(addr, "payload overrun");
            return Ok(None);
        }
        Ok(Some(bytes))
    }
    fn skip(&mut self, addr: SocketAddr, reason: &str) {
        let head = self.reader.recover_after_error();
        eprintln!(
            "{addr}: {reason}, skipped {} payloads",
            head.wrapping_sub(self.cursor)
        );
        self.cursor = head;
    }
}

struct Session {
    addr: SocketAddr,
    driver: Driver,
    h: ConnectionHandle,
    conn: Connection,
    start: Instant,
    timer: Option<Instant>,
    poll_rx_at: Instant,
    pending: Option<Pending>,
    connected: bool,
    more_tx: bool,
}

impl Session {
    fn connect(addr: SocketAddr, name: &str, config: ClientConfig, start: Instant) -> Result<Self> {
        let bind = match addr {
            SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
            SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
        };
        let mut driver = Driver::<1>::bind(bind, None)?;
        let (h, mut conn) = driver.ep.connect(start, config, addr, name)?;
        driver.flush(h, &mut conn, start)?;
        let timer = conn.poll_timeout();
        Ok(Self {
            addr,
            driver,
            h,
            conn,
            start,
            timer,
            poll_rx_at: start,
            pending: None,
            connected: false,
            more_tx: false,
        })
    }

    fn poll(&mut self, now: Instant, backlog: &mut Backlog, reconnecting: bool) -> Result<()> {
        let mut dirty = false;
        // Poll the queue between empty socket reads. A blocked writer needs peer credit.
        if now >= self.poll_rx_at || self.pending.is_some() {
            match self.driver.recv(now)? {
                Some(DatagramEvent::ConnectionEvent(k, e)) if k == self.h => {
                    self.conn.handle_event(e);
                    dirty = true;
                }
                Some(DatagramEvent::Response(t)) => self.driver.send(t)?,
                None => self.poll_rx_at = now + IDLE_RX_INTERVAL,
                _ => {}
            }
        }
        if self.timer.is_some_and(|t| t <= now) {
            self.conn.handle_timeout(now);
            dirty = true;
        }
        if dirty {
            while let Some(ev) = self.conn.poll() {
                match ev {
                    Event::Connected => {
                        self.connected = true;
                        if reconnecting {
                            backlog.skip(self.addr, "reconnected");
                        }
                        eprintln!("{}: connected", self.addr);
                    }
                    Event::ConnectionLost { reason } => bail!("{reason}"),
                    _ => {}
                }
            }
        }
        if !self.connected && now.duration_since(self.start) >= Duration::from_secs(5) {
            bail!("connect timeout")
        }
        if let Some(expired) = self.pending.take_if(|p| now >= p.deadline) {
            if let Some(id) = expired.id {
                let _ = self.conn.send_stream(id).reset(0u8.into());
                dirty = true;
            }
            backlog.skip(self.addr, "stream timeout");
        }
        // Retry a blocked stream after QUIC receives input or a timer fires.
        if self.connected && ((self.pending.is_none() && !backlog.waiting_for_buffer) || dirty) {
            for _ in 0..MAX_STREAMS_PER_FLUSH {
                let p = match &mut self.pending {
                    Some(p) => p,
                    None => {
                        let Some(payload) = backlog.next_payload(self.addr)? else {
                            break;
                        };
                        self.pending.insert(Pending {
                            id: None,
                            payload,
                            deadline: now + STREAM_TIMEOUT,
                        })
                    }
                };
                if p.id.is_none() {
                    p.id = self.conn.streams().open(Dir::Uni);
                }
                let Some(id) = p.id else { break };
                let mut stream = self.conn.send_stream(id);
                dirty = true;
                match stream.write_chunks(std::slice::from_mut(&mut p.payload)) {
                    Ok(_) => {}
                    Err(WriteError::Blocked) => break,
                    Err(WriteError::Stopped(code)) => {
                        let _ = stream.reset(code);
                        self.pending = None;
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                }
                if !p.payload.is_empty() {
                    break;
                }
                stream.finish()?;
                self.pending = None;
            }
        }
        if dirty || self.more_tx {
            self.more_tx = self.driver.flush_some(self.h, &mut self.conn, now, 2)?;
            self.timer = self.conn.poll_timeout();
        } else {
            std::hint::spin_loop();
        }
        Ok(())
    }
}

const INITIAL_BACKOFF: Duration = Duration::from_millis(250);

pub struct OutputTile {
    label: TileName,
    core: CoreId,
    addr: SocketAddr,
    server_name: String,
    config: ClientConfig,
    backlog: Backlog,
    session: Option<Session>,
    reconnecting: bool,
    retry_at: Instant,
    backoff: Duration,
}

impl OutputTile {
    pub fn new(
        label: &'static str,
        core: CoreId,
        addr: SocketAddr,
        config: ClientConfig,
        reader: ConsumerBare<DCacheRef>,
        cache: Arc<DCache>,
    ) -> Self {
        let server_name = match addr.ip() {
            IpAddr::V4(_) => solana_tls_utils::socket_addr_to_quic_server_name(addr),
            IpAddr::V6(ip) => format!("ip6-{:032x}.{}.sol", u128::from(ip), addr.port()),
        };
        Self {
            label: TileName::from_str_truncate(label),
            core,
            addr,
            server_name,
            config,
            backlog: Backlog {
                reader,
                cache,
                cursor: 0,
                buffers: BufferPool::new(SEND_BLOCKS, SEND_BLOCK_SIZE),
                waiting_for_buffer: false,
            },
            session: None,
            reconnecting: false,
            retry_at: Instant::now(),
            backoff: INITIAL_BACKOFF,
        }
    }

    fn poll(&mut self, now: Instant) -> Result<()> {
        if let Some(session) = &mut self.session {
            session.poll(now, &mut self.backlog, self.reconnecting)?;
        } else if now >= self.retry_at {
            self.session = Some(Session::connect(
                self.addr,
                &self.server_name,
                self.config.clone(),
                now,
            )?);
        } else {
            std::hint::spin_loop();
        }
        Ok(())
    }
}

impl Tile<MuxSpine> for OutputTile {
    fn name(&self) -> TileName {
        self.label
    }

    fn try_init(&mut self, _: &mut SpineAdapter<MuxSpine>) -> bool {
        crate::check_core(self.core);
        self.backlog.buffers.prefault();
        true
    }

    fn loop_body(&mut self, _: &mut SpineAdapter<MuxSpine>) {
        if let Err(err) = self.poll(Instant::now()) {
            eprintln!("{}: disconnected ({err})", self.addr);
            if self.session.take().is_some_and(|s| s.connected) {
                self.backoff = INITIAL_BACKOFF;
            }
            self.reconnecting = true;
            self.retry_at = Instant::now() + self.backoff;
            self.backoff = (self.backoff * 2).min(Duration::from_secs(8));
        }
    }
}
