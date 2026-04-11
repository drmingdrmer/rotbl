use std::io;
use std::sync::Arc;

use crate::v001::block_cache::new_block_cache;
use crate::v001::block_cache::BlockCache;
use crate::v001::config::Config;

pub struct DB {
    #[allow(dead_code)]
    pub(crate) config: Config,

    #[allow(dead_code)]
    pub(crate) block_cache: BlockCache,
}

impl DB {
    pub fn open(config: Config) -> Result<Arc<Self>, io::Error> {
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

    pub fn new_cache(config: Config) -> BlockCache {
        new_block_cache(&config.block_cache)
    }
}
