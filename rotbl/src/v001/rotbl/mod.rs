#[cfg(test)]
mod rotbl_async_test;
#[cfg(test)]
mod rotbl_test;

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use futures::stream::BoxStream;
use itertools::Itertools;

use crate::buf::new_uninitialized;
use crate::codec::Codec;
use crate::typ::Type;
use crate::v001::block::Block;
use crate::v001::block_cache::BlockCache;
use crate::v001::block_id::BlockId;
use crate::v001::block_index::BlockIndex;
use crate::v001::block_index::BlockMeta;
use crate::v001::db::DB;
use crate::v001::footer::Footer;
use crate::v001::header::Header;
use crate::v001::range::RangeArg;
use crate::v001::rotbl_io::IODriver;
use crate::v001::rotbl_io::IOPort;
use crate::v001::rotbl_meta::RotblMeta;
use crate::v001::with_checksum::WithChecksum;
use crate::v001::SeqMarked;
use crate::version::Version;

#[derive(Debug)]
pub struct Rotbl {
    /// The db this table belongs
    // db: Arc<DB>,
    block_cache: Arc<Mutex<BlockCache>>,

    file: Arc<Mutex<fs::File>>,

    #[allow(dead_code)]
    header: Header,

    pub(crate) table_id: u32,

    meta: RotblMeta,

    pub(crate) block_index: BlockIndex,

    #[allow(dead_code)]
    footer: Footer,
}

impl Rotbl {
    /// Create a new table from a series of key-value pairs
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
    /// | Footer
    /// ```
    pub fn create_table<P: AsRef<Path>>(
        db: &DB,
        path: P,
        table_id: u32,
        seq: u64,
        user_data: impl ToString,
        kvs: impl IntoIterator<Item = (String, SeqMarked)>,
    ) -> Result<Rotbl, io::Error> {
        let mut n = 0;

        let mut f = fs::OpenOptions::new().create(true).create_new(true).read(true).write(true).open(&path)?;

        // Write header

        let header = Header::new(Type::Rotbl, Version::V001);
        n += header.encode(&mut f)?;

        // Write table id

        let tid = WithChecksum::new(table_id);
        n += tid.encode(&mut f)?;

        // Write RotblMeta
        let rotbl_meta = RotblMeta::new(seq, user_data);
        n += rotbl_meta.encode(&mut f)?;

        // Writ blocks

        let mut index = Vec::new();

        let chunk = db.config.block.max_items.unwrap();

        let kv_chunks = kvs.into_iter().chunks(chunk);
        for (block_num, chunk_entries) in kv_chunks.into_iter().enumerate() {
            let bt: BTreeMap<_, _> = BTreeMap::from_iter(chunk_entries.into_iter());

            let first_key: String = bt.first_key_value().unwrap().0.clone();
            let last_key: String = bt.last_key_value().unwrap().0.clone();

            let block = Block::new(block_num as u32, bt);
            let offset = n as u64;
            let block_size = block.encode(&mut f)?;
            n += block_size;

            let index_entry = BlockMeta {
                block_num: block_num as u32,
                offset,
                size: block_size as u64,
                first_key,
                last_key,
            };

            index.push(index_entry);
        }

        let block_index_offset = n;

        // Write block index

        let block_index = BlockIndex::new(index);
        n += block_index.encode(&mut f)?;

        // Write footer

        let footer = Footer::new(block_index_offset as u64);
        n += footer.encode(&mut f)?;

        let _ = n;

        let r = Rotbl {
            block_cache: db.block_cache.clone(),
            file: Arc::new(Mutex::new(f)),
            header,
            table_id,
            meta: rotbl_meta,
            footer,
            block_index,
        };

        Ok(r)
    }

    pub fn open<P: AsRef<Path>>(db: &DB, path: P) -> Result<Self, io::Error> {
        let mut f = fs::OpenOptions::new().create(false).create_new(false).read(true).open(&path)?;

        // Header

        let header = Header::decode(&mut f)?;
        assert_eq!(header, Header::new(Type::Rotbl, Version::V001));

        // TableId

        let table_id = WithChecksum::<u32>::decode(&mut f)?.into_inner();

        // Meta
        let meta = RotblMeta::decode(&mut f)?;

        // Footer

        f.seek(io::SeekFrom::End(-(Footer::ENCODED_SIZE as i64)))?;
        let footer = Footer::decode(&mut f)?;

        // block index

        let index_offset = footer.block_index_offset;
        let index_size = f.metadata()?.len() - Footer::ENCODED_SIZE - index_offset;

        f.seek(io::SeekFrom::Start(index_offset))?;

        let mut index_buf = new_uninitialized(index_size as usize);
        f.read_exact(&mut index_buf)?;

        let block_index = BlockIndex::decode(&mut index_buf.as_slice())?;

        //

        let r = Self {
            // db,
            block_cache: db.block_cache.clone(),
            header,
            file: Arc::new(Mutex::new(f)),
            footer,
            block_index,
            table_id,
            meta,
        };

        Ok(r)
    }

    pub fn meta(&self) -> &RotblMeta {
        &self.meta
    }

    /// Return the block if it is in the cache.
    pub fn get_block(&self, block_num: u32) -> Option<Arc<Block>> {
        let block_id = BlockId::new(self.table_id, block_num);

        let mut c = self.block_cache.lock().unwrap();
        c.get(&block_id).cloned()
    }

    /// Load a block from disk and fill it into the cache.
    ///
    /// If the block is already in the cache, it will be returned immediately.
    ///
    /// If hold a lock to the cache while loading the block.
    pub fn load_block(&self, block_num: u32) -> Result<Arc<Block>, io::Error> {
        let block_id = BlockId::new(self.table_id, block_num);

        // Hold the lock until the block is loaded.
        let mut cache = self.block_cache.lock().unwrap();
        if let Some(b) = cache.get(&block_id).cloned() {
            return Ok(b);
        }

        let block = self.do_load_block(block_num)?;

        cache.insert(block_id, block.clone());

        Ok(block)
    }

    pub async fn load_block_async(&self, block_num: u32) -> Result<Arc<Block>, io::Error> {
        let join_handle = tokio::task::block_in_place(move || self.load_block(block_num));
        let block = join_handle?;
        Ok(block)
    }

    /// Load block from disk without accessing cache.
    pub(crate) fn do_load_block(&self, block_num: u32) -> Result<Arc<Block>, io::Error> {
        let block_meta = self.block_index.get_index_entry_by_num(block_num).unwrap();

        let mut buf = new_uninitialized(block_meta.size as usize);

        {
            let mut f = self.file.lock().unwrap();
            f.seek(io::SeekFrom::Start(block_meta.offset))?;
            f.read_exact(&mut buf)?;
        }

        let block = Block::decode(&mut buf.as_slice())?;
        let block = Arc::new(block);
        Ok(block)
    }

    pub fn io_driver(&self) -> IODriver {
        IODriver {
            rotbl: self,
            io: IOPort::new(),
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<SeqMarked>, io::Error> {
        let block_num = self.block_index.lookup(key).map(|x| x.block_num);

        let Some(block_num) = block_num else {
            return Ok(None);
        };

        let block = self.load_block_async(block_num).await?;
        let v = block.get(key).cloned();
        Ok(v)
    }

    /// Return a static `Stream` that iterating kvs in the specified range.
    pub fn range(
        self: &Arc<Self>,
        range: impl RangeArg<String>,
    ) -> BoxStream<'static, Result<(String, SeqMarked), io::Error>> {
        self.clone().do_range(range)
    }

    #[futures_async_stream::try_stream(boxed, ok = (String, SeqMarked), error = io::Error)]
    async fn do_range(self: Arc<Self>, range: impl RangeArg<String>) {
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
