#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct BlockCacheConfig {
    /// Max blocks to cache
    pub(crate) max_items: Option<usize>,

    /// Max bytes to cache
    pub(crate) capacity: Option<usize>,
}

#[allow(clippy::identity_op)]
impl BlockCacheConfig {
    const DEFAULT_MAX_ITEM: usize = 1024;
    const DEFAULT_CAPACITY: usize = 1 * 1024 * 1024 * 1024;

    pub fn fill_default_values(&mut self) {
        self.max_items = self.max_items.or(Some(Self::DEFAULT_MAX_ITEM));
        self.capacity = self.capacity.or(Some(Self::DEFAULT_CAPACITY));
    }

    pub fn with_max_items(mut self, max_items: usize) -> Self {
        self.max_items = Some(max_items);
        self
    }

    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = Some(capacity);
        self
    }
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct BlockConfig {
    /// Max item per block
    pub(crate) max_items: Option<usize>,
}

impl BlockConfig {
    const DEFAULT_MAX_ITEM: usize = 8 * 1024;

    pub fn with_max_items(mut self, max_items: usize) -> Self {
        self.max_items = Some(max_items);
        self
    }

    pub fn max_items(&self) -> usize {
        self.max_items.unwrap_or(Self::DEFAULT_MAX_ITEM)
    }

    pub fn fill_default_values(&mut self) {
        self.max_items = self.max_items.or(Some(Self::DEFAULT_MAX_ITEM));
    }
}

#[derive(Debug)]
#[derive(Clone)]
pub struct Config {
    pub root_path: String,
    pub block_config: BlockConfig,
    pub block_cache: BlockCacheConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            root_path: "./.rotbl/".to_string(),
            block_config: Default::default(),
            block_cache: Default::default(),
        }
    }
}

impl Config {
    #[allow(dead_code)]
    pub fn new(root_path: String) -> Self {
        Self {
            root_path,
            ..Default::default()
        }
    }

    pub fn with_root_path(mut self, root_path: impl ToString) -> Self {
        self.root_path = root_path.to_string();
        self
    }

    pub fn with_block_config(mut self, block_config: BlockConfig) -> Self {
        self.block_config = block_config;
        self
    }

    pub fn with_block_cache_config(mut self, block_cache_config: BlockCacheConfig) -> Self {
        self.block_cache = block_cache_config;
        self
    }

    pub fn disable_cache(&mut self) {
        self.block_cache.max_items = Some(0);
        self.block_cache.capacity = Some(0);
    }

    pub fn block_cache_mut(&mut self) -> &mut BlockCacheConfig {
        &mut self.block_cache
    }

    pub fn fill_default_values(&mut self) {
        self.block_config.fill_default_values();
        self.block_cache.fill_default_values();
    }
}
