use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use rotbl::storage::impls::fs::FsStorage;
use rotbl::v001::BlockCacheConfig;
use rotbl::v001::Config;
use rotbl::v001::Rotbl;

#[derive(Clone, Debug, PartialEq, Eq, clap::Parser)]
#[clap(about = "dump Rotbl v001 data", author)]
pub struct Args {
    #[arg(value_name = "PATH")]
    path: PathBuf,

    /// Print common-prefix statistics instead of dumping every key-value.
    #[arg(long)]
    stat: bool,

    /// Max number of blocks to sample for `--stat`, spread evenly. 0 = all blocks.
    #[arg(long, default_value_t = 512)]
    sample: usize,
}

fn main() -> Result<(), io::Error> {
    let args = Args::parse();

    let config = Config::default()
        .with_block_cache_config(BlockCacheConfig::default().with_capacity(256 * 1024 * 1024));

    let path = args.path.clone();
    // split path into dir and file
    let dir = path.parent().unwrap();
    let file = path.file_name().unwrap();

    let storage = FsStorage::new(dir.to_path_buf());

    let r = Rotbl::open(storage, config, file.to_str().unwrap()).unwrap();
    let r = Arc::new(r);

    if args.stat {
        return print_prefix_stat(&r, args.sample);
    }

    for s in r.dump() {
        println!("{}", s?);
    }
    Ok(())
}

/// Longest common prefix length in bytes, floored to a char boundary so the
/// slice `&s[..len]` is always valid UTF-8.
fn common_prefix_len(a: &str, b: &str) -> usize {
    let mut n = 0;
    for ((idx, ca), cb) in a.char_indices().zip(b.chars()) {
        if ca == cb {
            n = idx + ca.len_utf8();
        } else {
            break;
        }
    }
    n
}

fn mib(bytes: f64) -> f64 {
    bytes / (1024.0 * 1024.0)
}

/// Sample blocks and report how much a per-block common prefix would save.
///
/// For each sampled block the block-wide common prefix is `lcp(first_key, last_key)`
/// (equal to the LCP of all keys, since the block is sorted). From that we derive:
/// - in-memory key-byte savings  = sum over blocks of `keys * prefix_len`
/// - on-disk savings (net)       = sum over blocks of `(keys - 1) * prefix_len`
fn print_prefix_stat(r: &Arc<Rotbl>, sample: usize) -> Result<(), io::Error> {
    let stat = r.stat();
    let block_num = stat.block_num;
    let key_num = stat.key_num;
    let data_size = stat.data_size;
    let file_size = r.file_size();

    println!("file:  header={}, file_size={} ({:.1} MiB)", r.header(), file_size, mib(file_size as f64));
    println!("stat:  {}", stat);

    let step = if sample == 0 || (block_num as usize) <= sample {
        1
    } else {
        (block_num as usize) / sample
    };

    let mut s_blocks = 0u64;
    let mut s_keys = 0u64;
    let mut s_key_bytes = 0u64;
    let mut s_prefix_weighted = 0u64; // sum keys * plen
    let mut s_disk_savings = 0u64; // sum (keys-1) * plen
    let mut s_plen_sum = 0u64; // sum plen (per-block)
    let mut min_plen = usize::MAX;
    let mut max_plen = 0usize;
    let mut examples: Vec<(u32, usize, String)> = Vec::new();

    let mut bn = 0u32;
    while bn < block_num {
        let block = r.load_block(bn)?;

        let mut first: Option<String> = None;
        let mut last: Option<String> = None;
        let mut keys = 0u64;
        let mut key_bytes = 0u64;
        for (k, _v) in block.range::<String, _>(..) {
            if first.is_none() {
                first = Some(k.clone());
            }
            last = Some(k.clone());
            keys += 1;
            key_bytes += k.len() as u64;
        }

        let plen = match (&first, &last) {
            (Some(f), Some(l)) => common_prefix_len(f, l),
            _ => 0,
        };

        s_blocks += 1;
        s_keys += keys;
        s_key_bytes += key_bytes;
        s_prefix_weighted += keys * plen as u64;
        s_disk_savings += keys.saturating_sub(1) * plen as u64;
        s_plen_sum += plen as u64;
        min_plen = min_plen.min(plen);
        max_plen = max_plen.max(plen);

        if examples.len() < 8 {
            if let Some(f) = &first {
                examples.push((bn, plen, f[..plen].to_string()));
            }
        }

        bn = bn.saturating_add(step.max(1) as u32);
    }

    if s_blocks == 0 || s_keys == 0 {
        println!("no blocks/keys to sample");
        return Ok(());
    }

    let avg_keys = s_keys as f64 / s_blocks as f64;
    let avg_key_len = s_key_bytes as f64 / s_keys as f64;
    let avg_plen_block = s_plen_sum as f64 / s_blocks as f64;
    let avg_plen_key = s_prefix_weighted as f64 / s_keys as f64;
    let prefix_share_of_keys = s_prefix_weighted as f64 / s_key_bytes as f64;

    // Extrapolate to the whole file using exact totals where available.
    let est_total_key_bytes = avg_key_len * key_num as f64;
    let est_inmem_savings = prefix_share_of_keys * est_total_key_bytes;
    let disk_savings_share = s_disk_savings as f64 / s_key_bytes as f64;
    let est_disk_savings = disk_savings_share * est_total_key_bytes;

    println!();
    println!("--- sampled {} of {} blocks (step={}) ---", s_blocks, block_num, step.max(1));
    println!("avg keys/block          : {:.0}", avg_keys);
    println!("avg key length          : {:.1} B", avg_key_len);
    println!("block prefix length     : min={} avg={:.1} max={} B", min_plen, avg_plen_block, max_plen);
    println!("key-weighted avg prefix : {:.1} B", avg_plen_key);
    println!("prefix share of key bytes: {:.1} %", prefix_share_of_keys * 100.0);
    println!();
    println!("--- extrapolated to whole file ---");
    println!("est. total key bytes    : {:.1} MiB ({:.1} % of data_size)", mib(est_total_key_bytes), est_total_key_bytes / data_size as f64 * 100.0);
    println!("est. in-memory savings  : {:.1} MiB", mib(est_inmem_savings));
    println!("est. on-disk savings    : {:.1} MiB ({:.2} % of data_size, {:.2} % of file_size)", mib(est_disk_savings), est_disk_savings / data_size as f64 * 100.0, est_disk_savings / file_size as f64 * 100.0);
    println!();
    println!("--- example block prefixes ---");
    for (bn, plen, p) in &examples {
        println!("  block {:>5}: plen={:>3}  prefix={:?}", bn, plen, p);
    }

    Ok(())
}
