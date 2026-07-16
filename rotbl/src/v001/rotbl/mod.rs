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
use crate::v001::block::CachedBlock;
use crate::v001::block_cache::BlockCache;
use crate::v001::block_cache::BlockCacheKey;
use crate::v001::block_cache::BlockCacheValue;
use crate::v001::block_cache::RowGroupId;
use crate::v001::block_id::BlockId;
use crate::v001::block_index::BlockIndex;
use crate::v001::block_v003::read_v003_directory;
use crate::v001::block_v003::v003_fixed_prefix_size;
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
    /// A concurrent, weight-bounded cache of legacy blocks plus V003
    /// directories and compressed row groups.
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

    /// Return a fully decoded block when all of its contents are cached.
    ///
    /// V003 point reads cache only the directory and accessed compressed groups,
    /// so this returns `None` until every group has been loaded.
    pub fn get_block(&self, block_num: u32) -> Option<Arc<Block>> {
        let cached = self.get_cached_block(block_num)?;
        self.materialize_cached_block(block_num, &cached)
    }

    /// Load and fully decode a block.
    ///
    /// V003 retains only its directory and compressed groups in the cache; this
    /// explicit whole-block API inflates every group before returning.
    pub fn load_block(&self, block_num: u32) -> Result<Arc<Block>, io::Error> {
        let cached = self.load_cached_block(block_num)?;
        self.materialize_block(block_num, &cached)
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

    fn get_cached_block(&self, block_num: u32) -> Option<Arc<CachedBlock>> {
        let block_id = BlockId::new(self.table_id, block_num);
        let BlockCacheValue::Block(block) =
            self.block_cache.get(&BlockCacheKey::Block(block_id))?
        else {
            return None;
        };

        self.access_stat.hit_block(true);
        Some(block)
    }

    fn load_cached_block(&self, block_num: u32) -> Result<Arc<CachedBlock>, io::Error> {
        if let Some(block) = self.get_cached_block(block_num) {
            return Ok(block);
        }

        let block_id = BlockId::new(self.table_id, block_num);
        let value = self
            .block_cache
            .try_get_with(BlockCacheKey::Block(block_id), || {
                self.load_cached_block_nocache(block_num)
            })
            .map_err(cache_error)?;
        cached_block_value(value)
    }

    async fn load_cached_block_async(
        self: &Arc<Self>,
        block_num: u32,
    ) -> Result<Arc<CachedBlock>, io::Error> {
        if let Some(block) = self.get_cached_block(block_num) {
            return Ok(block);
        }

        let table = self.clone();
        tokio::task::spawn_blocking(move || table.load_cached_block(block_num))
            .await
            .map_err(join_error)?
    }

    /// Load a block directory directly from disk, bypassing the cache.
    fn load_cached_block_nocache(&self, block_num: u32) -> Result<BlockCacheValue, io::Error> {
        let start = Instant::now();
        debug!("load_block start: {}", block_num);

        let block_meta = self
            .block_index
            .get_index_entry_by_num(block_num)
            .ok_or_else(|| invalid_block_num(block_num))?;
        let fixed_size = v003_fixed_prefix_size();
        if block_meta.size < fixed_size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "block is shorter than the V003 fixed prefix",
            ));
        }

        let mut fixed = new_uninitialized(fixed_size);
        self.file.read_exact_at(&mut fixed, block_meta.offset)?;
        let mut header_input = fixed.as_slice();
        let header = Header::decode(&mut header_input)?;

        let block = if header == Header::new(Type::Block, Version::V003) {
            let directory =
                read_v003_directory(&*self.file, block_meta.offset, block_meta.size, &fixed)?;
            CachedBlock::RowGroup(Arc::new(directory))
        } else {
            let mut buf = new_uninitialized(block_meta.size as usize);
            self.file.read_exact_at(&mut buf, block_meta.offset)?;
            CachedBlock::Decoded(Arc::new(Block::decode(&mut buf.as_slice())?))
        };

        self.access_stat.hit_block(false);

        debug!(
            "load_block   end: {}; elapsed: {:?}",
            block_num,
            start.elapsed()
        );

        Ok(BlockCacheValue::Block(Arc::new(block)))
    }

    fn materialize_block(
        &self,
        block_num: u32,
        cached: &CachedBlock,
    ) -> Result<Arc<Block>, io::Error> {
        match cached {
            CachedBlock::Decoded(block) => Ok(block.clone()),
            CachedBlock::RowGroup(directory) => {
                let block = directory.to_block(|group_index| {
                    let encoded = self.load_row_group(block_num, directory, group_index)?;
                    directory.decode_group(group_index, &encoded)
                })?;
                Ok(Arc::new(block))
            }
        }
    }

    fn materialize_cached_block(&self, block_num: u32, cached: &CachedBlock) -> Option<Arc<Block>> {
        match cached {
            CachedBlock::Decoded(block) => Some(block.clone()),
            CachedBlock::RowGroup(directory) => {
                let block_id = BlockId::new(self.table_id, block_num);
                let groups = (0..directory.group_count())
                    .map(|group_index| self.get_cached_row_group(block_id, group_index))
                    .collect::<Option<Vec<_>>>()?;
                let block = directory
                    .to_block(|group_index| {
                        directory.decode_group(group_index, &groups[group_index])
                    })
                    .expect("cached V003 row groups are validated before insertion");
                Some(Arc::new(block))
            }
        }
    }

    fn get_cached_row_group(&self, block_id: BlockId, group_index: usize) -> Option<Arc<[u8]>> {
        let group_id = RowGroupId::new(block_id, group_index)?;
        let BlockCacheValue::RowGroup(group) =
            self.block_cache.get(&BlockCacheKey::RowGroup(group_id))?
        else {
            return None;
        };
        Some(group)
    }

    fn load_row_group(
        &self,
        block_num: u32,
        directory: &crate::v001::block_v003::RowGroupDirectory,
        group_index: usize,
    ) -> Result<Arc<[u8]>, io::Error> {
        let block_id = BlockId::new(self.table_id, block_num);
        if let Some(group) = self.get_cached_row_group(block_id, group_index) {
            return Ok(group);
        }

        let group_id = RowGroupId::new(block_id, group_index).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "row-group index exceeds u32")
        })?;
        let value = self
            .block_cache
            .try_get_with(BlockCacheKey::RowGroup(group_id), || {
                self.load_row_group_nocache(block_num, directory, group_index)
            })
            .map_err(cache_error)?;
        cached_row_group_value(value)
    }

    async fn load_row_group_async(
        self: &Arc<Self>,
        block_num: u32,
        directory: Arc<crate::v001::block_v003::RowGroupDirectory>,
        group_index: usize,
    ) -> Result<Arc<[u8]>, io::Error> {
        let block_id = BlockId::new(self.table_id, block_num);
        if let Some(group) = self.get_cached_row_group(block_id, group_index) {
            return Ok(group);
        }

        let table = self.clone();
        tokio::task::spawn_blocking(move || {
            table.load_row_group(block_num, &directory, group_index)
        })
        .await
        .map_err(join_error)?
    }

    fn load_row_group_nocache(
        &self,
        block_num: u32,
        directory: &crate::v001::block_v003::RowGroupDirectory,
        group_index: usize,
    ) -> Result<BlockCacheValue, io::Error> {
        let block_meta = self
            .block_index
            .get_index_entry_by_num(block_num)
            .ok_or_else(|| invalid_block_num(block_num))?;
        let offset = directory.group_file_offset(block_meta.offset, group_index)?;
        let size = directory.group_size(group_index)?;
        let mut encoded = new_uninitialized(size);
        self.file.read_exact_at(&mut encoded, offset)?;
        directory.validate_group_bytes(group_index, &encoded)?;
        Ok(BlockCacheValue::RowGroup(Arc::from(encoded)))
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

        let cached = self.load_cached_block_async(block_num).await?;
        match cached.as_ref() {
            CachedBlock::Decoded(block) => Ok(block.get(key).cloned()),
            CachedBlock::RowGroup(directory) => {
                let Some(group_index) = directory.group_index(key) else {
                    return Ok(None);
                };
                let encoded =
                    self.load_row_group_async(block_num, directory.clone(), group_index).await?;
                let rows = directory.decode_group(group_index, &encoded)?;
                Ok(directory.group_value(key, &rows))
            }
        }
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
            let cached = self.load_cached_block_async(m.block_num).await?;
            match cached.as_ref() {
                CachedBlock::Decoded(block) => {
                    for (key, value) in block.range(range.clone()) {
                        yield (key.to_string(), value.clone());
                    }
                }
                CachedBlock::RowGroup(directory) => {
                    for group_index in directory.group_range(&range) {
                        let encoded = self
                            .load_row_group_async(m.block_num, directory.clone(), group_index)
                            .await?;
                        let rows = directory.decode_group(group_index, &encoded)?;
                        for (key, value) in directory.group_rows_in_range(rows, &range) {
                            yield (key, value);
                        }
                    }
                }
            }
        }
    }
}

fn cached_block_value(value: BlockCacheValue) -> Result<Arc<CachedBlock>, io::Error> {
    match value {
        BlockCacheValue::Block(block) => Ok(block),
        BlockCacheValue::RowGroup(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "block cache key contains a row group",
        )),
    }
}

fn cached_row_group_value(value: BlockCacheValue) -> Result<Arc<[u8]>, io::Error> {
    match value {
        BlockCacheValue::Block(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "row-group cache key contains a block",
        )),
        BlockCacheValue::RowGroup(group) => Ok(group),
    }
}

fn cache_error(error: Arc<io::Error>) -> io::Error {
    io::Error::new(error.kind(), error.to_string())
}

fn join_error(error: tokio::task::JoinError) -> io::Error {
    io::Error::new(io::ErrorKind::Other, error)
}

fn invalid_block_num(block_num: u32) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("block number {block_num} does not exist"),
    )
}
