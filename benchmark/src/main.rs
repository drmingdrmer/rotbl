use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use futures::TryStreamExt;
use rotbl::num::format_num;
use rotbl::storage::impls::fs::FsStorage;
use rotbl::v001::BlockCacheConfig;
use rotbl::v001::BlockConfig;
use rotbl::v001::Builder;
use rotbl::v001::Config;
use rotbl::v001::Rotbl;
use rotbl::v001::RotblMeta;
use rotbl::v001::SeqMarked;
use rotbl::v001::DB;

#[allow(clippy::identity_op)]
#[tokio::main]
async fn main() {
    let n_keys_per_block = 80;
    // let n_keys_per_block = 1024 * 16;
    let build_keys = 1 * 1024;
    // let r = build(&db, 32 * 1024 * 1024).await;

    let config = Config::default()
        .with_root_path("./_rotbl")
        .with_block_config(BlockConfig::default().with_max_items(n_keys_per_block))
        .with_block_cache_config(
            BlockCacheConfig::default().with_max_items(5).with_capacity(256 * 1024 * 1024),
        );

    let db = DB::open(config).unwrap();
    let r = build(&db, build_keys).await;

    // let r = Rotbl::open(config, "./_rotbl/foo").unwrap();

    let r = Arc::new(r);

    // {
    //     let n = 1024 * 16 * 6;
    //     // let n = 5_000_000;
    //     scan(&r, n).await;
    //     scan(&r, n).await;
    // }

    let _ = r;
}

#[allow(dead_code)]
async fn build(db: &DB, n_keys: i32) -> Rotbl {
    let key_len = 64;
    let val_len = 256;

    let storage = FsStorage::new(PathBuf::from("./_rotbl"));

    let meta = RotblMeta::new(1, "hello");

    let mut b = Builder::new(storage, db.config(), "foo").unwrap();

    let mut k = "a".repeat(key_len);
    let mut v = "a".repeat(val_len);

    let start = Instant::now();

    // generate 1024 keys with permutation of four letters "abcd" in alphabetical order
    for _i in 0..n_keys {
        b.append_kv(&k, SeqMarked::new_normal(1, bb(&v))).unwrap();
        k = next(&k);
        v = next(&v);
    }

    let elapsed = start.elapsed();
    println!("Elapsed building: {:?}", elapsed);

    let start = Instant::now();

    let r = b.commit(meta).unwrap();

    let elapsed = start.elapsed();
    println!("Elapsed commit: {:?}", elapsed);
    r
}

#[allow(dead_code)]
async fn scan(r: &Arc<Rotbl>, max: u64) {
    let mut n = 0;
    let chunk_size = 100_000;

    let mut strm = r.range(..);

    let mut start = Instant::now();

    while let Some((k, v)) = strm.try_next().await.unwrap() {
        let (k, _v) = black_box((k, v));

        if n % chunk_size == 0 {
            let elapsed = start.elapsed();
            start = Instant::now();
            let kps = chunk_size as f64 / elapsed.as_secs_f64();

            println!(
                "{} : {}, {:>.1} key/sec; stat: {} access: {}",
                format_num(n),
                k,
                kps,
                r.stat(),
                r.access_stat()
            );

            println!("{:?}", r.cache_stat());
        }
        n += 1;
        if n >= max {
            break;
        }
    }

    let elapsed = start.elapsed();
    println!("Elapsed scan: {:?}", elapsed);
}

#[allow(dead_code)]
pub(crate) fn bb(x: impl ToString) -> Vec<u8> {
    x.to_string().into_bytes()
}

/// Next permutation of the string of the same length.
///
/// Replace the last character with next character in the alphabet.
/// If the last character is 'z', replace it with 'a' and replace the second last character with
/// next character in the alphabet.
pub fn next(k: &str) -> String {
    let mut chars: Vec<char> = k.chars().collect();

    // Iterate from the end towards the beginning
    for i in (0..chars.len()).rev() {
        if chars[i] == 'z' {
            chars[i] = 'a';
            if i == 0 {
                unreachable!("exhausted");
            }
        } else {
            chars[i] = (chars[i] as u8 + 1) as char;
            break;
        }
    }

    // Collect the characters back into a String
    chars.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_increment() {
        assert_eq!(next("aaaaa"), "aaaab");
        assert_eq!(next("aaaay"), "aaaaz");
    }

    #[test]
    fn test_wrap_around() {
        assert_eq!(next("aaaaz"), "aaaba");
    }

    #[test]
    fn test_multiple_wrap_around() {
        assert_eq!(next("aaazz"), "aabaa");
        assert_eq!(next("aazzz"), "abaaa");
    }

    #[test]
    fn test_no_initial_z() {
        assert_eq!(next("abcde"), "abcdf");
        assert_eq!(next("asdfg"), "asdfh");
    }

    #[test]
    fn test_3_chars() {
        assert_eq!(next("abc"), "abd");
    }
}
