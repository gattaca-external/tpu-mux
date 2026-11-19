mod buffers;
mod input;
mod output;
mod socket;
mod transport;

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use clap::Parser;
use flux::{
    communication::{
        ShmemData, cleanup_shmem,
        queue::{ConsumerBare, Producer, Queue, QueueType},
    },
    core_affinity::{self, CoreId},
    spine_derive::from_spine,
    tile::{TileConfig, TileInfo, attach_tile, tile_runner},
    utils::{DCache, DCacheRef},
};
use solana_keypair::{Keypair, read_keypair_file};

const MAX_PAYLOAD: usize = 4096;
const RING_SIZE: usize = 4096;
const STREAMS_PER_PEER: usize = 32;
const STREAM_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Parser)]
#[command(
    version,
    about = "Forward each completed TPU stream to two QUIC destinations"
)]
struct Args {
    /// Local TPU listen address (IP:port).
    #[arg(long)]
    listen: SocketAddr,
    /// Two distinct destination TPU addresses (IP:port).
    #[arg(long, num_args = 2, required = true, value_names = ["A", "B"])]
    outputs: Vec<SocketAddr>,
    /// Solana JSON keypair for the multiplexer. Generates an ephemeral identity if omitted.
    #[arg(long)]
    identity: Option<PathBuf>,
}

#[from_spine("tpu-mux")]
struct MuxSpine {
    tile_info: ShmemData<TileInfo>,
}

fn check_core(core: CoreId) {
    assert_eq!(
        core_affinity::get_core_ids().as_deref(),
        Some([core].as_slice()),
        "tile must run on CPU {}",
        core.id
    );
}

struct Publisher {
    queue: Producer<DCacheRef>,
    cache: Arc<DCache>,
}

impl Publisher {
    fn produce(&mut self, payload: &[u8]) {
        let entry = self
            .cache
            .write(payload.len(), |buf| buf.copy_from_slice(payload))
            .expect("completed TPU stream fits the data cache");
        self.queue.produce(&entry);
    }
}

fn run(args: Args) -> Result<()> {
    let [a, b] = args.outputs[..] else {
        anyhow::bail!("expected two destinations")
    };
    ensure!(a != b, "destinations must be distinct");
    for addr in [a, b] {
        ensure!(
            addr.port() != 0 && !addr.ip().is_unspecified() && !addr.ip().is_multicast(),
            "expected a unicast destination: {addr}"
        );
        ensure!(
            addr != args.listen
                && !(args.listen.ip().is_unspecified()
                    && addr.port() == args.listen.port()
                    && addr.ip().is_loopback()),
            "destination points at listener: {addr}"
        );
    }
    let key = match args.identity {
        Some(path) => read_keypair_file(&path)
            .map_err(|e| anyhow::anyhow!("read identity {}: {e}", path.display()))?,
        None => Keypair::new(),
    };
    let (server, client) = transport::configs(&key)?;

    let input = socket::Driver::bind(args.listen, Some(server))?;
    let listen = input.addr()?;
    let cores = core_affinity::get_core_ids().context("read process CPU affinity")?;
    ensure!(
        cores.len() >= 3,
        "three CPUs required; select them with taskset"
    );
    let cache = DCache::new(DCache::required_capacity(RING_SIZE, MAX_PAYLOAD).next_power_of_two());
    let queue = Queue::<DCacheRef>::new(RING_SIZE, QueueType::SPMC);
    let dir = tempfile::tempdir()?;
    let spine = MuxSpine::new_with_base_dir(dir.path(), None);
    // Only the tile registry is mapped. Unlink it while live so even SIGKILL leaves no segment.
    cleanup_shmem(dir.path());
    spine.start(None, None, |scope| {
        eprintln!("listening on {listen} -> {a}, {b}");
        for (i, (addr, name)) in [(a, "output-a"), (b, "output-b")].into_iter().enumerate() {
            let mut reader = ConsumerBare::new(queue, name);
            reader.subscribe_broadcast();
            let core = cores[i + 1];
            let tile =
                output::OutputTile::new(name, core, addr, client.clone(), reader, cache.clone());
            attach_tile(
                tile,
                scope,
                TileConfig::new(core.id, None).without_metrics(),
            );
        }
        let tile = input::InputTile::new(
            cores[0],
            input,
            Publisher {
                queue: Producer::from(queue),
                cache,
            },
        );
        tile_runner(
            tile,
            scope,
            TileConfig::new(cores[0].id, None).without_metrics(),
        )();
    });
    Ok(())
}
fn main() -> Result<()> {
    run(Args::parse())
}
