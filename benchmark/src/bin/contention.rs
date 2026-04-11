//! Contention benchmark for `rotbl`'s block cache and file reader.
//!
//! Measures the scenarios where the current `Mutex<LruCache>` + `Mutex<BoxReader>`
//! design is expected to show worst-case behavior:
//!
//!   Phase 1 — Cold-cache thundering herd:
//!     N tasks concurrently call `get(same_key)` against a freshly opened Rotbl.
//!     Baseline serializes all N loads through `file.lock()`; a singleflight cache
//!     coalesces them into one disk read.
//!
//!   Phase 2 — Concurrent random gets under cache pressure:
//!     N tasks each do M random `get(key)` calls with a cache too small to hold
//!     the working set. Exercises both `cache.lock()` and `file.lock()` under
//!     sustained miss pressure.
//!
//!   Phase 3 — Mixed workload (scanner + random getters):
//!     One task runs `range(..)` while N tasks do random `get` calls on the same
//!     Rotbl. Mirrors the databend-meta compactor + apply race pattern.
//!
//! Run with:
//!     cargo run --release --bin contention
//!
//! Record the output on the current branch, then re-run after the refactor and diff.

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use futures::TryStreamExt;
use rotbl::storage::impls::fs::FsStorage;
use rotbl::v001::BlockCacheConfig;
use rotbl::v001::BlockConfig;
use rotbl::v001::Builder;
use rotbl::v001::Config;
use rotbl::v001::Rotbl;
use rotbl::v001::RotblMeta;
use rotbl::v001::SeqMarked;
use rotbl::v001::DB;
use tokio::sync::Barrier;

const ROOT: &str = "./_rotbl_contention";
const TABLE: &str = "contention.rot";

// Target: ~500 MB on disk.
// 1M keys × (64 key + 400 val + ~32 encoding overhead) ≈ 520 MB raw.
const TOTAL_KEYS: u64 = 1_048_576;
const KEYS_PER_BLOCK: usize = 32;
const KEY_LEN: usize = 64;
const VAL_LEN: usize = 400;

// Tight cache: ~256 blocks out of ~32768 total → ~99% miss under random access.
// Forces sustained contention on file.lock() rather than hitting cache.
const CACHE_CAPACITY: usize = 4 * 1024 * 1024;
const CACHE_MAX_ITEMS: usize = 512;

#[tokio::main]
async fn main() {
    let keys = Arc::new(build_table());

    let file_size = std::fs::metadata(format!("{}/{}", ROOT, TABLE))
        .map(|m| m.len())
        .unwrap_or(0);

    println!("=== rotbl contention benchmark ===");
    println!(
        "build: total_keys={} keys_per_block={} blocks≈{} key_len={} val_len={} file={:.1} MB",
        TOTAL_KEYS,
        KEYS_PER_BLOCK,
        TOTAL_KEYS / KEYS_PER_BLOCK as u64,
        KEY_LEN,
        VAL_LEN,
        file_size as f64 / (1024.0 * 1024.0)
    );
    println!(
        "cache: capacity={} MB max_items={}",
        CACHE_CAPACITY / (1024 * 1024),
        CACHE_MAX_ITEMS
    );
    println!();

    for concurrency in [4, 16, 64, 256] {
        phase_cold_herd(&keys, concurrency).await;
    }

    for concurrency in [4, 16, 64] {
        phase_concurrent_random(&keys, concurrency, 500).await;
    }

    for getters in [4, 16, 64] {
        phase_mixed(&keys, getters, 500).await;
    }
}

/// Phase 1: N tasks all request the same key from a cold-cache Rotbl.
///
/// Under Mutex<LruCache>, all N spawn_blocking loads serialize on `file.lock()`,
/// producing a fan-out in per-task latencies (1×, 2×, 3×, ... of one disk read).
/// Under a singleflight cache, all N coalesce into one load → latencies cluster.
async fn phase_cold_herd(keys: &Arc<Vec<String>>, concurrency: usize) {
    let r = Arc::new(open_fresh());

    // Pick a key in the middle of the table so the lookup is a non-trivial block.
    let target = keys[keys.len() / 2].clone();

    let barrier = Arc::new(Barrier::new(concurrency));
    let mut handles = Vec::with_capacity(concurrency);

    let wall = Instant::now();
    for _ in 0..concurrency {
        let r = r.clone();
        let target = target.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let t0 = Instant::now();
            let v = r.get(&target).await.unwrap();
            let elapsed = t0.elapsed();
            black_box(v);
            elapsed
        }));
    }

    let mut lats = Vec::with_capacity(concurrency);
    for h in handles {
        lats.push(h.await.unwrap());
    }
    let wall = wall.elapsed();
    lats.sort();

    println!(
        "--- Phase 1: cold-cache thundering herd (concurrency={}) ---",
        concurrency
    );
    report(&lats);
    println!(
        "  wall={:?} cache_stat={:?} access={}",
        wall,
        r.cache_stat(),
        r.access_stat()
    );
    println!();
}

/// Phase 2: N tasks each do random `get`s against a cold Rotbl with a tight cache.
///
/// Forces sustained miss pressure so every op walks `spawn_blocking → file.lock()
/// → cache.lock()`. Reveals how the serialization tax scales with concurrency.
async fn phase_concurrent_random(
    keys: &Arc<Vec<String>>,
    concurrency: usize,
    ops_per_task: usize,
) {
    let r = Arc::new(open_fresh());

    let barrier = Arc::new(Barrier::new(concurrency));
    let mut handles = Vec::with_capacity(concurrency);

    let wall = Instant::now();
    for task_id in 0..concurrency {
        let r = r.clone();
        let keys = keys.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            let mut lats = Vec::with_capacity(ops_per_task);
            let mut rng = seed(task_id as u64);
            barrier.wait().await;
            for _ in 0..ops_per_task {
                let idx = (next_rand(&mut rng) as usize) % keys.len();
                let t0 = Instant::now();
                let v = r.get(&keys[idx]).await.unwrap();
                lats.push(t0.elapsed());
                black_box(v);
            }
            lats
        }));
    }

    let mut lats = Vec::with_capacity(concurrency * ops_per_task);
    for h in handles {
        lats.extend(h.await.unwrap());
    }
    let wall = wall.elapsed();
    lats.sort();

    let total = (concurrency * ops_per_task) as f64;
    println!(
        "--- Phase 2: concurrent random gets (concurrency={}, ops/task={}) ---",
        concurrency, ops_per_task
    );
    report(&lats);
    println!(
        "  wall={:?} throughput={:.0} ops/s cache_stat={:?} access={}",
        wall,
        total / wall.as_secs_f64(),
        r.cache_stat(),
        r.access_stat()
    );
    println!();
}

/// Phase 3: one scanner + N random getters racing on the same Rotbl.
///
/// Matches the databend-meta pattern where the compactor is scanning while apply
/// dispatches per-key `get`s on the same snapshot. The scanner holds `file.lock()`
/// during each block fetch; getters compete for the same lock.
async fn phase_mixed(keys: &Arc<Vec<String>>, getters: usize, ops_per_task: usize) {
    let r = Arc::new(open_fresh());

    let barrier = Arc::new(Barrier::new(getters + 1));

    let scanner = {
        let r = r.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            let t0 = Instant::now();
            let mut strm = r.range(..);
            let mut n: u64 = 0;
            while let Some(kv) = strm.try_next().await.unwrap() {
                black_box(kv);
                n += 1;
            }
            (n, t0.elapsed())
        })
    };

    let mut handles = Vec::with_capacity(getters);
    for task_id in 0..getters {
        let r = r.clone();
        let keys = keys.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            let mut lats = Vec::with_capacity(ops_per_task);
            let mut rng = seed(task_id as u64 ^ 0xDEAD_BEEF);
            barrier.wait().await;
            for _ in 0..ops_per_task {
                let idx = (next_rand(&mut rng) as usize) % keys.len();
                let t0 = Instant::now();
                let v = r.get(&keys[idx]).await.unwrap();
                lats.push(t0.elapsed());
                black_box(v);
            }
            lats
        }));
    }

    let wall = Instant::now();
    let mut lats = Vec::with_capacity(getters * ops_per_task);
    for h in handles {
        lats.extend(h.await.unwrap());
    }
    let (scan_n, scan_elapsed) = scanner.await.unwrap();
    let wall = wall.elapsed();
    lats.sort();

    let total = (getters * ops_per_task) as f64;
    println!(
        "--- Phase 3: mixed scanner + getters (getters={}, ops/task={}) ---",
        getters, ops_per_task
    );
    println!("  scanner: {} keys in {:?}", scan_n, scan_elapsed);
    print!("  getters: ");
    report(&lats);
    println!(
        "  wall={:?} getter_throughput={:.0} ops/s cache_stat={:?} access={}",
        wall,
        total / wall.as_secs_f64(),
        r.cache_stat(),
        r.access_stat()
    );
    println!();
}

fn build_table() -> Vec<String> {
    std::fs::remove_dir_all(ROOT).ok();
    std::fs::create_dir_all(ROOT).unwrap();

    let config = Config::default()
        .with_root_path(ROOT)
        .with_block_config(BlockConfig::default().with_max_items(KEYS_PER_BLOCK))
        .with_block_cache_config(
            BlockCacheConfig::default()
                .with_max_items(CACHE_MAX_ITEMS)
                .with_capacity(CACHE_CAPACITY),
        );

    let db = DB::open(config).unwrap();
    let storage = FsStorage::new(PathBuf::from(ROOT));
    let mut b = Builder::new(storage, db.config(), TABLE).unwrap();

    let mut k = "a".repeat(KEY_LEN);
    let mut v = "a".repeat(VAL_LEN);
    let mut keys = Vec::with_capacity(TOTAL_KEYS as usize);

    let start = Instant::now();
    for _ in 0..TOTAL_KEYS {
        b.append_kv(&k, SeqMarked::new_normal(1, v.clone().into_bytes()))
            .unwrap();
        keys.push(k.clone());
        k = next_perm(&k);
        v = next_perm(&v);
    }
    b.commit(RotblMeta::new(1, "bench")).unwrap();
    println!("built {} keys in {:?}", TOTAL_KEYS, start.elapsed());

    keys
}

/// Open the table fresh so `Rotbl::open` installs a new, empty block cache.
fn open_fresh() -> Rotbl {
    let config = Config::default()
        .with_root_path(ROOT)
        .with_block_config(BlockConfig::default().with_max_items(KEYS_PER_BLOCK))
        .with_block_cache_config(
            BlockCacheConfig::default()
                .with_max_items(CACHE_MAX_ITEMS)
                .with_capacity(CACHE_CAPACITY),
        );
    let storage = FsStorage::new(PathBuf::from(ROOT));
    Rotbl::open(storage, config, TABLE).unwrap()
}

fn report(sorted: &[Duration]) {
    if sorted.is_empty() {
        println!("n=0");
        return;
    }
    let n = sorted.len();
    let pct = |q: f64| sorted[(((n - 1) as f64) * q).round() as usize];
    let sum: Duration = sorted.iter().sum();
    let mean = sum / n as u32;
    println!(
        "n={:<6} min={:>9?} p50={:>9?} p90={:>9?} p99={:>9?} p999={:>9?} max={:>9?} mean={:>9?}",
        n,
        sorted[0],
        pct(0.50),
        pct(0.90),
        pct(0.99),
        pct(0.999),
        sorted[n - 1],
        mean
    );
}

// Cheap deterministic PRNG (LCG) — no external crates, reproducible per task.
fn seed(s: u64) -> u64 {
    s.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1)
}

fn next_rand(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

fn next_perm(k: &str) -> String {
    let mut chars: Vec<char> = k.chars().collect();
    for i in (0..chars.len()).rev() {
        if chars[i] == 'z' {
            chars[i] = 'a';
            if i == 0 {
                unreachable!("exhausted key space");
            }
        } else {
            chars[i] = (chars[i] as u8 + 1) as char;
            break;
        }
    }
    chars.into_iter().collect()
}
