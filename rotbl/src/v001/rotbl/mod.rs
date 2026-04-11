pub mod access_stat;
pub mod builder;
pub mod dump;
pub mod stat;

use std::io;
use std::sync::Arc;
use std::time::Instant;

use codeq::Decode;
use codeq::FixedSize;
use futures::stream::BoxStream;
use log::debug;

use crate::buf::new_uninitialized;
use crate::io_util;
use crate::storage::BoxReaderAt;
use crate::storage::Storage;
use crate::typ::Type;
use crate::v001::block::Block;
use crate::v001::block_cache::BlockCache;
use crate::v001::block_id::BlockId;
use crate::v001::block_index::BlockIndex;
use crate::v001::db::DB;
use crate::v001::footer::Footer;
use crate::v001::header::Header;
use crate::v001::range::RangeArg;
use crate::v001::rotbl::access_stat::AccessStat;
use crate::v001::rotbl_meta::RotblMeta;
use crate::v001::types::WithChecksum;
use crate::v001::CacheStat;
use crate::v001::Config;
use crate::v001::SeqMarked;
use crate::version::Version;

/// A readonly table.
///
/// The table is organized as follows, and every part has its own checksum embedded:
///
/// ```text
/// | Header
/// | TableId with checksum
/// | Meta
/// | Block 0
/// | Block 1
/// | ...
/// | BlockIndex
/// | Stat
/// | Footer
/// ```
#[derive(Debug)]
pub struct Rotbl {
    /// A concurrent, weight-bounded cache of decoded blocks.
    ///
    /// Backed by [`moka::sync::Cache`] — `get` is lock-free and concurrent
    /// cache misses for the same block are coalesced by `try_get_with`, so
    /// a thundering herd of readers only triggers one disk read per block.
    block_cache: BlockCache,

    /// Positional reader for on-miss block loads.
    ///
    /// Uses `pread(2)`-style I/O under the hood (see
    /// [`crate::storage::ReaderAt`]), so concurrent misses on different
    /// blocks can issue parallel reads against the OS page cache / disk
    /// without any userspace lock.
    file: BoxReaderAt,

    /// On disk file size in bytes
    file_size: u64,

    header: Header,

    // not used yet.
    pub(crate) table_id: u32,

    meta: RotblMeta,

    pub(crate) block_index: BlockIndex,

    stat: stat::RotblStat,

    access_stat: AccessStat,

    #[allow(dead_code)]
    footer: Footer,
}

impl Rotbl {
    /// Create a new table from a series of key-value pairs
    pub fn create_table<S: Storage>(
        storage: S,
        config: Config,
        path: &str,
        meta: RotblMeta,
        kvs: impl IntoIterator<Item = (String, SeqMarked)>,
    ) -> Result<Rotbl, io::Error> {
        let mut builder = builder::Builder::new(storage, config, path)?;
        for (k, v) in kvs {
            builder.append_kv(k, v)?;
        }
        let t = builder.commit(meta)?;

        Ok(t)
    }

    pub fn open<S: Storage>(
        mut storage: S,
        config: Config,
        rel_path: &str,
    ) -> Result<Self, io::Error> {
        // Single positional reader handles both the one-shot metadata parse
        // and the concurrent block-load hot path.
        let file: BoxReaderAt = storage.reader_at(rel_path)?;
        let file_size = file.len()?;

        // Header + table_id live at offset 0, each with a statically known
        // fixed size. Read the whole prefix in one syscall and decode from
        // an in-memory slice (which implements `Read`).
        let prefix_size = Header::encoded_size() + <WithChecksum<u32>>::encoded_size();
        let mut prefix = new_uninitialized(prefix_size);
        file.read_exact_at(&mut prefix, 0)?;
        let mut prefix_slice = prefix.as_slice();
        let header = Header::decode(&mut prefix_slice)?;
        assert_eq!(header, Header::new(Type::Rotbl, Version::V001));
        let table_id = WithChecksum::<u32>::decode(&mut prefix_slice)?.into_inner();

        // Footer sits at the tail of the file at a fixed offset.
        let footer_offset = file_size - Footer::encoded_size() as u64;
        let mut footer_buf = new_uninitialized(Footer::encoded_size());
        file.read_exact_at(&mut footer_buf, footer_offset)?;
        let footer = Footer::decode(&mut footer_buf.as_slice())?;

        let block_index = {
            let buf = io_util::read_segment(&*file, footer.block_index_segment)?;
            BlockIndex::decode(&mut buf.as_slice())?
        };

        let meta = {
            let buf = io_util::read_segment(&*file, footer.meta_segment)?;
            RotblMeta::decode(&mut buf.as_slice())?
        };

        let stat = {
            let buf = io_util::read_segment(&*file, footer.stat_segment)?;
            stat::RotblStat::decode(&mut buf.as_slice())?
        };

        let cache = DB::new_cache(config.clone());

        let r = Self {
            block_cache: cache,
            table_id,
            header,
            file,
            file_size,
            meta,
            block_index,
            stat,
            access_stat: Default::default(),
            footer,
        };

        Ok(r)
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn meta(&self) -> &RotblMeta {
        &self.meta
    }

    pub fn block_index(&self) -> &BlockIndex {
        &self.block_index
    }

    pub fn stat(&self) -> &stat::RotblStat {
        &self.stat
    }

    pub fn footer(&self) -> &Footer {
        &self.footer
    }

    pub fn access_stat(&self) -> &AccessStat {
        &self.access_stat
    }

    pub fn cache_stat(&self) -> CacheStat {
        // Flush moka's pending admissions/evictions so the counters reflect
        // the current cache state — moka updates entry_count/weighted_size
        // via a background maintenance queue and the fresh values only
        // become visible after pending tasks have been drained.
        self.block_cache.run_pending_tasks();
        CacheStat::new(
            self.block_cache.entry_count(),
            self.block_cache.weighted_size(),
        )
    }

    /// Return the block if it is in the cache.
    pub fn get_block(&self, block_num: u32) -> Option<Arc<Block>> {
        let block_id = BlockId::new(self.table_id, block_num);

        let b = self.block_cache.get(&block_id);

        if b.is_some() {
            self.access_stat.hit_block(true);
        }

        b
    }

    /// Load a block from disk and fill it into the cache.
    ///
    /// If the block is already in the cache, it is returned immediately.
    /// Otherwise the block is loaded from disk, inserted into the cache,
    /// and returned. Concurrent calls for the same `block_num` are coalesced
    /// by the underlying moka cache: only one caller performs the disk read,
    /// while the others block-wait and receive a clone of the loaded `Arc<Block>`.
    pub fn load_block(&self, block_num: u32) -> Result<Arc<Block>, io::Error> {
        // Fast path: record a cache hit and return immediately.
        if let Some(b) = self.get_block(block_num) {
            return Ok(b);
        }

        // Slow path: delegate to moka's singleflight `try_get_with` so that
        // at most one caller per `block_id` actually runs the disk loader.
        let block_id = BlockId::new(self.table_id, block_num);
        self.block_cache
            .try_get_with(block_id, || self.load_block_nocache(block_num))
            .map_err(|e: Arc<io::Error>| io::Error::new(e.kind(), e.to_string()))
    }

    pub async fn load_block_async(
        self: &Arc<Self>,
        block_num: u32,
    ) -> Result<Arc<Block>, io::Error> {
        debug!("load_block_async start: {}", block_num);

        if let Some(b) = self.get_block(block_num) {
            debug!("load_block_async cache: {}", block_num);
            return Ok(b);
        }

        let s = self.clone();
        let join_handle = tokio::task::spawn_blocking(move || s.load_block(block_num));

        let load_block_res =
            join_handle.await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let block = load_block_res?;

        debug!("load_block_async   end: {}", block_num);
        Ok(block)
    }

    /// Load a block directly from disk, bypassing the cache.
    ///
    /// Invoked as the `try_get_with` initializer on a cache miss. The read
    /// is issued via [`crate::storage::ReaderAt::read_exact_at`], so
    /// concurrent initializers for different blocks do not serialize on any
    /// userspace lock — the kernel's `pread` handles parallel positioned
    /// reads directly against the page cache / disk.
    pub(crate) fn load_block_nocache(&self, block_num: u32) -> Result<Arc<Block>, io::Error> {
        let start = Instant::now();
        debug!("load_block start: {}", block_num);

        let block_meta = self.block_index.get_index_entry_by_num(block_num).unwrap();

        let mut buf = new_uninitialized(block_meta.size as usize);
        self.file.read_exact_at(&mut buf, block_meta.offset)?;

        let block = Block::decode(&mut buf.as_slice())?;
        let block = Arc::new(block);

        self.access_stat.hit_block(false);

        debug!(
            "load_block   end: {}; elapsed: {:?}",
            block_num,
            start.elapsed()
        );

        Ok(block)
    }

    /// Dump the table to human-readable lines in an iterator.
    pub fn dump(self: &Arc<Self>) -> impl Iterator<Item = Result<String, io::Error>> {
        dump::Dump::new(self.clone()).dump()
    }

    /// Return the value of the specified key.
    pub async fn get(self: &Arc<Self>, key: &str) -> Result<Option<SeqMarked>, io::Error> {
        let block_num = self.block_index.lookup(key).map(|x| x.block_num);

        let Some(block_num) = block_num else {
            return Ok(None);
        };

        let block = self.load_block_async(block_num).await?;
        let v = block.get(key).cloned();
        Ok(v)
    }

    /// Return a `'static` `Stream` that iterating kvs in the specified range.
    pub fn range(
        self: &Arc<Self>,
        range: impl RangeArg,
    ) -> BoxStream<'static, Result<(String, SeqMarked), io::Error>> {
        self.clone().do_range(range)
    }

    #[futures_async_stream::try_stream(boxed, ok = (String, SeqMarked), error = io::Error)]
    async fn do_range(self: Arc<Self>, range: impl RangeArg) {
        let block_metas = self.block_index.lookup_range(range.clone()).to_vec();

        for m in block_metas {
            let block = self.load_block_async(m.block_num).await?;
            let it = block.range(range.clone());
            for (k, v) in it {
                yield (k.clone(), v.clone());
            }
        }
    }
}
