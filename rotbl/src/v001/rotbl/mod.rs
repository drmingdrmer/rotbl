pub mod access_stat;
pub mod builder;
pub mod dump;
pub mod stat;

#[cfg(test)]
mod tests;

use std::io;
use std::io::Read;
use std::io::Seek;
use std::sync::Arc;
use std::sync::Mutex;

use codeq::Decode;
use codeq::FixedSize;
use futures::stream::BoxStream;
use log::debug;

use crate::buf::new_uninitialized;
use crate::io_util;
use crate::storage::BoxReader;
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
    /// The db this table belongs
    block_cache: Arc<Mutex<BlockCache>>,

    file: Arc<Mutex<BoxReader>>,

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

    pub fn open<S: Storage>(mut storage: S, config: Config, path: &str) -> Result<Self, io::Error> {
        let mut f = storage.reader(path)?;

        let header = {
            let header = Header::decode(&mut f)?;
            assert_eq!(header, Header::new(Type::Rotbl, Version::V001));
            header
        };

        let table_id = WithChecksum::<u32>::decode(&mut f)?.into_inner();

        let footer_offset = f.seek(io::SeekFrom::End(-(Footer::encoded_size() as i64)))?;
        let footer = Footer::decode(&mut f)?;

        let block_index = {
            let buf = io_util::read_segment(&mut f, footer.block_index_segment)?;
            BlockIndex::decode(&mut buf.as_slice())?
        };

        let meta = {
            let buf = io_util::read_segment(&mut f, footer.meta_segment)?;
            RotblMeta::decode(&mut buf.as_slice())?
        };

        let stat = {
            let buf = io_util::read_segment(&mut f, footer.stat_segment)?;
            stat::RotblStat::decode(&mut buf.as_slice())?
        };

        let cache = DB::new_cache(config.clone());

        let r = Self {
            block_cache: cache,
            table_id,
            header,
            file: Arc::new(Mutex::new(f)),
            file_size: footer_offset + Footer::encoded_size() as u64,
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

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn meta(&self) -> &RotblMeta {
        &self.meta
    }

    pub fn stat(&self) -> &stat::RotblStat {
        &self.stat
    }

    pub fn access_stat(&self) -> &AccessStat {
        &self.access_stat
    }

    pub fn cache_stat(&self) -> CacheStat {
        let c = self.block_cache.lock().unwrap();
        CacheStat::new(c.len() as u64, c.size() as u64)
    }

    /// Return the block if it is in the cache.
    pub fn get_block(&self, block_num: u32) -> Option<Arc<Block>> {
        let block_id = BlockId::new(self.table_id, block_num);

        let b = {
            let mut c = self.block_cache.lock().unwrap();
            c.get(&block_id).cloned()
        };

        if b.is_some() {
            self.access_stat.hit_block(true);
        }

        b
    }

    /// Load a block from disk and fill it into the cache.
    ///
    /// If the block is already in the cache, it will be returned immediately.
    ///
    /// If hold a lock to the cache while loading the block.
    pub fn load_block(&self, block_num: u32) -> Result<Arc<Block>, io::Error> {
        debug!("load_block start: {}", block_num);
        let block_id = BlockId::new(self.table_id, block_num);

        // Hold the lock until the block is loaded.
        let mut cache = self.block_cache.lock().unwrap();
        if let Some(b) = cache.get(&block_id).cloned() {
            self.access_stat.hit_block(true);
            return Ok(b);
        }

        let block = self.load_block_nocache(block_num)?;

        cache.insert(block_id, block.clone());

        debug!("load_block   end: {}", block_num);

        Ok(block)
    }

    pub async fn load_block_async(&self, block_num: u32) -> Result<Arc<Block>, io::Error> {
        debug!("load_block_async start: {}", block_num);
        let join_handle = tokio::task::block_in_place(move || self.load_block(block_num));
        let block = join_handle?;
        debug!("load_block_async   end: {}", block_num);
        Ok(block)
    }

    /// Load block from disk without accessing cache.
    pub(crate) fn load_block_nocache(&self, block_num: u32) -> Result<Arc<Block>, io::Error> {
        let block_meta = self.block_index.get_index_entry_by_num(block_num).unwrap();

        let mut buf = new_uninitialized(block_meta.size as usize);

        {
            let mut f = self.file.lock().unwrap();
            f.seek(io::SeekFrom::Start(block_meta.offset))?;
            f.read_exact(&mut buf)?;
        }

        let block = Block::decode(&mut buf.as_slice())?;
        let block = Arc::new(block);

        self.access_stat.hit_block(false);

        Ok(block)
    }

    /// Dump the table to human-readable lines in an iterator.
    pub fn dump(self: &Arc<Self>) -> impl Iterator<Item = Result<String, io::Error>> {
        dump::Dump::new(self.clone()).dump()
    }

    /// Return the value of the specified key.
    pub async fn get(&self, key: &str) -> Result<Option<SeqMarked>, io::Error> {
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
