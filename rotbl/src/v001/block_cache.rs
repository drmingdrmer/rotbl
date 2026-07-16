use std::sync::Arc;

use moka::sync::Cache;

use crate::v001::block::CachedBlock;
use crate::v001::block_id::BlockId;
use crate::v001::config::BlockCacheConfig;

/// A cache key for either a whole legacy block, a V003 directory, or one V003
/// compressed row group.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) enum BlockCacheKey {
    Block(BlockId),
    RowGroup(RowGroupId),
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct RowGroupId {
    block_id: BlockId,
    group_index: u32,
}

impl RowGroupId {
    pub(crate) fn new(block_id: BlockId, group_index: usize) -> Option<Self> {
        Some(Self {
            block_id,
            group_index: u32::try_from(group_index).ok()?,
        })
    }
}

/// A cache value. V003 retains its small directory and compressed groups as
/// separate entries so a cold point read does not allocate a decoded block.
#[derive(Clone, Debug)]
pub(crate) enum BlockCacheValue {
    Block(Arc<CachedBlock>),
    RowGroup(Arc<[u8]>),
}

impl BlockCacheValue {
    fn cache_size(&self) -> usize {
        match self {
            Self::Block(block) => match block.as_ref() {
                CachedBlock::Decoded(block) => {
                    usize::try_from(block.data_encoded_size()).unwrap_or(usize::MAX)
                }
                CachedBlock::RowGroup(directory) => directory.cache_size(),
            },
            Self::RowGroup(group) => group.len(),
        }
    }
}

/// A concurrent, weight-bounded cache of decoded legacy blocks and compressed
/// V003 row-group data.
pub(crate) type BlockCache = Cache<BlockCacheKey, BlockCacheValue>;

/// Build a cache whose capacity applies to both directories and compressed row
/// groups, keeping their combined residency within the configured budget.
pub(crate) fn new_block_cache(cfg: &BlockCacheConfig) -> BlockCache {
    Cache::builder()
        .max_capacity(cfg.capacity() as u64)
        .weigher(|_key: &BlockCacheKey, value: &BlockCacheValue| {
            u32::try_from(value.cache_size().max(1)).unwrap_or(u32::MAX)
        })
        .build()
}
