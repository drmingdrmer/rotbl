pub(crate) mod cache;
pub(crate) mod cache_stat;

use std::io;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::storage::Storage;
use crate::v001::config::Config;
use crate::v001::db::cache::new_block_cache;
use crate::v001::db::cache::BlockCache;
use crate::v001::Builder;
use crate::v001::CacheStat;
use crate::v001::Rotbl;
use crate::v001::RotblMeta;
use crate::v001::SeqMarked;

/// A set of tables sharing one block cache.
///
/// Every table opened or created through a `DB` borrows the DB's single
/// `BlockCache`, so the cache space is shared: hot blocks—from any
/// table—compete for it, and total cache memory is bounded by one budget
/// ([`BlockCacheConfig::capacity`](crate::v001::BlockCacheConfig)) regardless of
/// table count.
pub struct DB {
    config: Config,
    block_cache: BlockCache,

    /// Source of process-unique cache namespaces for tables in this DB.
    next_table_id: AtomicU32,
}

impl DB {
    pub fn open(config: Config) -> Result<Arc<Self>, io::Error> {
        let block_cache = new_block_cache(&config.block_cache);

        let db = Self {
            config,
            block_cache,
            next_table_id: AtomicU32::new(0),
        };

        Ok(Arc::new(db))
    }

    pub fn config(&self) -> Config {
        self.config.clone()
    }

    /// Allocate a cache namespace for a table opened in this DB.
    ///
    /// The id is unique within this DB instance and namespaces the shared cache
    /// so blocks from different tables never collide. It lives only in memory;
    /// it is never persisted (the on-disk table id stays 0).
    fn alloc_table_id(&self) -> u32 {
        self.next_table_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Build a new table in this DB, sharing the DB's block cache.
    pub fn create_table<S: Storage>(
        &self,
        storage: S,
        rel_path: &str,
        meta: RotblMeta,
        kvs: impl IntoIterator<Item = (String, SeqMarked)>,
    ) -> Result<Rotbl, io::Error> {
        let mut builder = Builder::new_in_db(
            storage,
            self.config.clone(),
            rel_path,
            self.alloc_table_id(),
            self.block_cache.clone(),
        )?;
        for (k, v) in kvs {
            builder.append_kv(k, v)?;
        }
        builder.commit(meta)
    }

    /// Open an existing table into this DB, sharing the DB's block cache.
    pub fn open_table<S: Storage>(&self, storage: S, rel_path: &str) -> Result<Rotbl, io::Error> {
        Rotbl::open_in_db(
            storage,
            rel_path,
            self.alloc_table_id(),
            self.block_cache.clone(),
        )
    }

    /// Stats of the DB-wide shared block cache.
    pub fn cache_stat(&self) -> CacheStat {
        // Flush moka's pending admissions/evictions so the counters are current.
        self.block_cache.run_pending_tasks();
        CacheStat::new(
            self.block_cache.entry_count(),
            self.block_cache.weighted_size(),
        )
    }
}
