use std::borrow::Borrow;
use std::sync::Arc;

use lru_cache::hashbrown::hash_map::DefaultHashBuilder;
use lru_cache::meter::Meter;
use lru_cache::LruCache;

use crate::v001::block::Block;
use crate::v001::block_id::BlockId;

pub struct BlockMeter;

impl<K> Meter<K, Arc<Block>> for BlockMeter {
    type Measure = usize;

    fn measure<Q: ?Sized>(&self, _: &Q, v: &Arc<Block>) -> usize
    where K: Borrow<Q> {
        v.data_encoded_size() as usize
    }
}

pub type BlockCache = LruCache<BlockId, Arc<Block>, DefaultHashBuilder, BlockMeter>;
