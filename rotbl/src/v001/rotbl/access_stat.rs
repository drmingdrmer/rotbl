use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::num::format_num;

#[derive(Debug)]
#[derive(Default)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct AccessStat {
    read_key: AtomicU64,
    read_block: AtomicU64,
    read_block_from_cache: AtomicU64,
    read_block_from_disk: AtomicU64,
}

impl fmt::Display for AccessStat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "read key: {}, read block: ({}, from cache: {}, from disk: {})",
            format_num(self.read_key.load(Ordering::Relaxed)),
            format_num(self.read_block.load(Ordering::Relaxed)),
            format_num(self.read_block_from_cache.load(Ordering::Relaxed)),
            format_num(self.read_block_from_disk.load(Ordering::Relaxed)),
        )
    }
}

impl AccessStat {
    pub fn read_key(&self) -> u64 {
        self.read_key.load(Ordering::Relaxed)
    }

    pub fn read_block(&self) -> u64 {
        self.read_block.load(Ordering::Relaxed)
    }

    pub fn read_block_from_cache(&self) -> u64 {
        self.read_block_from_cache.load(Ordering::Relaxed)
    }

    pub fn read_block_from_disk(&self) -> u64 {
        self.read_block_from_disk.load(Ordering::Relaxed)
    }

    pub fn hit_block(&self, from_cache: bool) {
        self.read_block.fetch_add(1, Ordering::Relaxed);

        if from_cache {
            self.read_block_from_cache.fetch_add(1, Ordering::Relaxed);
        } else {
            self.read_block_from_disk.fetch_add(1, Ordering::Relaxed);
        }
    }
}
