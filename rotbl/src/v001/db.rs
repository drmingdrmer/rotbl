use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use lru_cache_map::LruCache;

use crate::v001::block_cache::BlockCache;
use crate::v001::block_cache::BlockMeter;
use crate::v001::config::Config;

pub struct DB {
    #[allow(dead_code)]
    pub(crate) config: Config,

    #[allow(dead_code)]
    pub(crate) block_cache: Arc<Mutex<BlockCache>>,
}

impl DB {
    pub fn open(mut config: Config) -> Result<Arc<Self>, io::Error> {
        config.fill_default_values();

        let block_cache = Self::new_cache(config.clone());

        let db = Self {
            config,
            block_cache,
        };

        Ok(Arc::new(db))
    }

    pub fn config(&self) -> Config {
        self.config.clone()
    }

    pub fn new_cache(mut config: Config) -> Arc<Mutex<BlockCache>> {
        config.fill_default_values();

        let bc = &config.block_cache;
        let block_cache =
            LruCache::with_meter(bc.max_items.unwrap(), bc.capacity.unwrap(), BlockMeter);
        Arc::new(Mutex::new(block_cache))
    }
}
