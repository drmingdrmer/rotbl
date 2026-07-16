//! Profile V003 row-group compression on a real snapshot.
//!
//! Opens a V001 snapshot, re-encodes every block in the current V003 format
//! (common-prefix dedup + row-group zstd), and reports the on-disk size reduction. The
//! V001 baseline is the table's own recorded block-data size; the V003 figure
//! is the sum of the freshly framed blocks.
//!
//! ```bash
//! cargo run --release --bin measure-v002 -- /path/to/snapshot.snap
//! ```
//!
//! Round-trip correctness of the V003 codec is covered by the unit and
//! corruption tests in `rotbl`; this binary only measures size.

use std::path::PathBuf;

use codeq::Encode;
use rotbl::storage::impls::fs::FsStorage;
use rotbl::v001::BlockCacheConfig;
use rotbl::v001::Config;
use rotbl::v001::Rotbl;

const CACHE_CAPACITY: usize = 256 * 1024 * 1024;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arg = std::env::args().nth(1).ok_or("usage: measure-v002 <snapshot-file>")?;
    let path = PathBuf::from(&arg);
    let dir = path.parent().map(|p| p.to_path_buf()).unwrap_or_default();
    let file = path.file_name().and_then(|s| s.to_str()).ok_or("snapshot path has no file name")?;

    let config = Config::default()
        .with_block_cache_config(BlockCacheConfig::default().with_capacity(CACHE_CAPACITY));
    let r = Rotbl::open(FsStorage::new(dir), config, file)?;

    let stat = r.stat();
    let block_num = stat.block_num;
    let v001_total = stat.data_size; // framed V001 block bytes, the baseline

    let mut v003_total = 0u64; // re-encoded framed V003 block bytes
    for bn in 0..block_num {
        let block = r.load_block(bn)?;

        let mut buf = Vec::new();
        block.encode(&mut buf)?;
        v003_total += buf.len() as u64;

        if bn % 100 == 0 {
            eprintln!("  .. block {bn}/{block_num}");
        }
    }

    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    println!("snapshot           : {}", path.display());
    println!("blocks / keys      : {block_num} / {}", stat.key_num);
    println!(
        "V001 blocks on-disk: {v001_total} B ({:.1} MiB)",
        mib(v001_total)
    );
    println!(
        "V003 blocks on-disk: {v003_total} B ({:.1} MiB)",
        mib(v003_total)
    );
    println!(
        "V001 -> V003       : {:.2}x  ({:.1}% smaller; prefix dedup + row groups)",
        v001_total as f64 / v003_total as f64,
        100.0 * (1.0 - v003_total as f64 / v001_total as f64),
    );
    Ok(())
}
