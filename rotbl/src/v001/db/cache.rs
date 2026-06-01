use std::sync::Arc;

use moka::sync::Cache;

use crate::v001::block::id::BlockId;
use crate::v001::block::Block;
use crate::v001::config::BlockCacheConfig;

/// A concurrent, weight-bounded block cache backed by [`moka::sync::Cache`].
///
/// Keyed by [`BlockId`], values are `Arc<Block>`. Each entry is weighed by
/// `Block::data_encoded_size()`, so [`BlockCacheConfig::capacity`] caps total
/// bytes rather than entry count.
///
/// Concurrency properties:
/// - `get` is lock-free and scales with the number of cores.
/// - `try_get_with` coalesces concurrent misses for the same key: only the first caller runs the
///   initializer (disk load), the rest block-wait and then receive a clone of the loaded
///   `Arc<Block>`. This eliminates the "thundering herd" where N tasks simultaneously read the same
///   block.
pub type BlockCache = Cache<BlockId, Arc<Block>>;

/// Build a new [`BlockCache`] from a [`BlockCacheConfig`].
///
/// The resulting cache enforces a total weighted capacity equal to
/// `cfg.capacity()` bytes. Each block is charged its `data_encoded_size()`,
/// with a floor of 1 so zero-sized blocks still occupy a slot.
pub fn new_block_cache(cfg: &BlockCacheConfig) -> BlockCache {
    Cache::builder()
        .max_capacity(cfg.capacity() as u64)
        .weigher(|_k: &BlockId, v: &Arc<Block>| v.data_encoded_size().max(1) as u32)
        .build()
}
