use bytes::BytesMut;

pub struct BufferPool {
    blocks: Box<[BytesMut]>,
    current: usize,
}

impl BufferPool {
    pub fn new(count: usize, size: usize) -> Self {
        let blocks = (0..count)
            .map(|_| {
                let mut block = BytesMut::with_capacity(size);
                // Promote to shared storage now. Later splits only increment its reference count.
                drop(block.split());
                block
            })
            .collect();
        Self { blocks, current: 0 }
    }

    pub fn prefault(&mut self) {
        for block in &mut self.blocks {
            if let Some((last, rest)) = block.spare_capacity_mut().split_last_mut() {
                // The final byte covers the trailing page of an unaligned block.
                for byte in rest.iter_mut().step_by(4096).chain([last]) {
                    // SAFETY: each pointer addresses a writable byte of spare capacity.
                    unsafe { byte.as_mut_ptr().write_volatile(0) };
                }
            }
        }
    }

    /// Returns an empty tail with at least `size` bytes. A block can wrap only
    /// after all slices previously handed to QUIC have been released.
    pub fn writable(&mut self, size: usize) -> Option<&mut BytesMut> {
        let start = self.current;
        loop {
            if self.blocks[self.current].try_reclaim(size) {
                return Some(&mut self.blocks[self.current]);
            }
            self.current += 1;
            if self.current == self.blocks.len() {
                self.current = 0;
            }
            if self.current == start {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn space_is_reclaimed_only_after_the_last_slice_is_released() {
        let mut pool = BufferPool::new(1, 8);
        let buf = pool.writable(4).unwrap();
        buf.extend_from_slice(b"abcd");
        let first = buf.split().freeze();
        let buf = pool.writable(4).unwrap();
        buf.extend_from_slice(b"efgh");
        let second = buf.split().freeze();
        let retransmit = second.clone();
        drop(first);
        drop(second);
        assert!(pool.writable(4).is_none());
        assert_eq!(&retransmit[..], b"efgh");
        drop(retransmit);
        let buf = pool.writable(8).unwrap();
        buf.extend_from_slice(b"recycled");
        assert_eq!(&buf[..], b"recycled");
    }
}
