use crate::{
    MAX_PAYLOAD, MuxSpine, Publisher, STREAM_TIMEOUT, STREAMS_PER_PEER, buffers::BufferPool,
    socket::Driver,
};
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use flux::{core_affinity::CoreId, spine::SpineAdapter, tile::Tile};
use proto::{
    Connection, ConnectionHandle, DatagramEvent, Dir, Event, ReadError, StreamEvent, StreamId,
};
use std::{
    collections::{BTreeSet, HashMap},
    time::{Duration, Instant},
};

enum Payload {
    Chunk(Bytes),
    Fragments(BytesMut),
}

impl Payload {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Chunk(bytes) => bytes,
            Self::Fragments(bytes) => bytes,
        }
    }
    fn assemble(&mut self, buffers: &mut BufferPool) -> bool {
        if let Self::Chunk(first) = self {
            let Some(buf) = buffers.writable(MAX_PAYLOAD) else {
                return false;
            };
            // Claim the whole slot so later fragments can append in place.
            let tail = buf.split_off(MAX_PAYLOAD);
            let mut slot = std::mem::replace(buf, tail);
            slot.extend_from_slice(first);
            *self = Self::Fragments(slot);
        }
        true
    }
    fn append(&mut self, next: Bytes, buffers: &mut BufferPool) -> bool {
        if let Self::Chunk(first) = self
            && first.is_empty()
        {
            *first = next;
            return true;
        }
        if !self.assemble(buffers) {
            return false;
        }
        if let Self::Fragments(buf) = self {
            buf.extend_from_slice(&next);
        }
        true
    }
}

struct Partial {
    payload: Payload,
    expires: Instant,
}
struct Peer {
    conn: Connection,
    reads: HashMap<StreamId, Partial>,
    fragments: BufferPool,
    deadline: Option<Instant>,
    handshake: Option<Instant>,
    dirty: bool,
}

impl Peer {
    fn read(&mut self, id: StreamId, mut part: Partial, producer: &mut Publisher) {
        let mut stream = self.conn.recv_stream(id);
        let Ok(mut chunks) = stream.read(true) else {
            return;
        };
        let mut discard = false;
        loop {
            match chunks.next(MAX_PAYLOAD + 1) {
                Ok(Some(chunk)) => {
                    if part.payload.bytes().len() + chunk.bytes.len() > MAX_PAYLOAD
                        || !part.payload.append(chunk.bytes, &mut self.fragments)
                    {
                        discard = true;
                        break;
                    }
                }
                Ok(None) => {
                    if !part.payload.bytes().is_empty() {
                        producer.produce(part.payload.bytes());
                    }
                    break;
                }
                Err(ReadError::Blocked) => {
                    // Release the UDP buffer even when the peer delays FIN.
                    if part.payload.bytes().is_empty() || part.payload.assemble(&mut self.fragments)
                    {
                        self.reads.insert(id, part);
                    } else {
                        discard = true;
                    }
                    break;
                }
                Err(ReadError::Reset(_)) => break,
            }
        }
        let _ = chunks.finalize();
        if discard {
            let _ = stream.stop(0u8.into());
        }
    }
    fn events(&mut self, now: Instant, producer: &mut Publisher) {
        while let Some(ev) = self.conn.poll() {
            match ev {
                Event::Connected => self.handshake = None,
                Event::Stream(StreamEvent::Opened { dir: Dir::Uni }) => {
                    while let Some(id) = self.conn.streams().accept(Dir::Uni) {
                        let part = Partial {
                            payload: Payload::Chunk(Bytes::new()),
                            expires: now + STREAM_TIMEOUT,
                        };
                        self.read(id, part, producer);
                    }
                }
                Event::Stream(StreamEvent::Readable { id }) => {
                    if let Some(part) = self.reads.remove(&id) {
                        self.read(id, part, producer);
                    }
                }
                _ => {}
            }
        }
    }
    fn expire(&mut self, now: Instant) {
        self.conn.handle_timeout(now);
        if self.handshake.is_some_and(|t| t <= now) {
            self.conn.close(now, 0u8.into(), Default::default());
            self.handshake = None;
        }
        self.reads.retain(|&id, p| {
            if p.expires > now {
                return true;
            }
            let _ = self.conn.recv_stream(id).stop(0u8.into());
            false
        });
    }
}

const RX_QUANTUM: usize = 4;

pub struct InputTile {
    core: CoreId,
    driver: Driver<{ crate::socket::INGRESS_BATCH }>,
    producer: Publisher,
    peers: HashMap<ConnectionHandle, Peer>,
    timers: BTreeSet<(Instant, usize)>,
    dirty: Vec<ConnectionHandle>,
}

impl InputTile {
    pub fn new(
        core: CoreId,
        driver: Driver<{ crate::socket::INGRESS_BATCH }>,
        producer: Publisher,
    ) -> Self {
        Self {
            core,
            driver,
            producer,
            peers: HashMap::new(),
            timers: BTreeSet::new(),
            dirty: Vec::with_capacity(RX_QUANTUM + 1),
        }
    }

    fn poll(&mut self) -> Result<()> {
        let now = Instant::now();
        if let Some(&(t, n)) = self.timers.first().filter(|(t, _)| *t <= now) {
            self.timers.remove(&(t, n));
            let h = ConnectionHandle(n);
            let peer = self.peers.get_mut(&h).unwrap();
            peer.deadline = None;
            peer.expire(now);
            peer.events(now, &mut self.producer);
            peer.dirty = true;
            self.dirty.push(h);
        }
        // Always publish readable streams before servicing the next datagram.
        for index in 0..RX_QUANTUM {
            if index > 0 && !self.driver.has_buffered_datagram() {
                break;
            }
            let h = match self.driver.recv(now)? {
                Some(DatagramEvent::Response(tx)) => {
                    self.driver.send(tx)?;
                    continue;
                }
                Some(DatagramEvent::ConnectionEvent(h, ev)) => {
                    let Some(peer) = self.peers.get_mut(&h) else {
                        continue;
                    };
                    peer.conn.handle_event(ev);
                    h
                }
                Some(DatagramEvent::NewConnection(incoming)) => {
                    self.driver.tx.clear();
                    if self.peers.len() >= 512 {
                        let t = self.driver.ep.refuse(incoming, &mut self.driver.tx);
                        self.driver.send(t)?;
                        continue;
                    }
                    match self
                        .driver
                        .ep
                        .accept(incoming, now, &mut self.driver.tx, None)
                    {
                        Ok((h, conn)) => {
                            self.peers.insert(
                                h,
                                Peer {
                                    conn,
                                    reads: HashMap::with_capacity(STREAMS_PER_PEER),
                                    fragments: BufferPool::new(STREAMS_PER_PEER, MAX_PAYLOAD),
                                    deadline: None,
                                    handshake: Some(now + Duration::from_secs(5)),
                                    dirty: false,
                                },
                            );
                            h
                        }
                        Err(e) => {
                            if let Some(t) = e.response {
                                self.driver.send(t)?;
                            }
                            continue;
                        }
                    }
                }
                None => break,
            };
            let peer = self.peers.get_mut(&h).unwrap();
            peer.events(now, &mut self.producer);
            if !peer.dirty {
                peer.dirty = true;
                self.dirty.push(h);
            }
        }
        if self.dirty.is_empty() {
            std::hint::spin_loop();
            return Ok(());
        }
        let now = Instant::now();
        for h in self.dirty.drain(..) {
            let peer = self.peers.get_mut(&h).unwrap();
            peer.dirty = false;
            self.driver.flush(h, &mut peer.conn, now)?;
            let next = peer
                .conn
                .poll_timeout()
                .into_iter()
                .chain(peer.handshake)
                .chain(peer.reads.values().map(|p| p.expires))
                .min();
            if next != peer.deadline || peer.conn.is_drained() {
                if let Some(old) = peer.deadline {
                    self.timers.remove(&(old, h.0));
                }
                if peer.conn.is_drained() {
                    self.peers.remove(&h);
                    continue;
                }
                if let Some(t) = next {
                    self.timers.insert((t, h.0));
                }
                peer.deadline = next;
            }
        }
        Ok(())
    }
}

impl Tile<MuxSpine> for InputTile {
    fn try_init(&mut self, _: &mut SpineAdapter<MuxSpine>) -> bool {
        crate::check_core(self.core);
        true
    }

    fn loop_body(&mut self, _: &mut SpineAdapter<MuxSpine>) {
        self.poll().expect("poll TPU listener");
    }
}
