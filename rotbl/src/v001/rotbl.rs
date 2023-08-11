use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

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
use crate::v001::rotbl_io::IODriver;
use crate::v001::rotbl_io::IOPort;
use crate::v001::tseq::TSeqValue;
use crate::v001::with_checksum::WithChecksum;
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
        kvs: impl IntoIterator<Item = (String, TSeqValue)>,
    ) -> Result<Rotbl, io::Error> {
        let mut n = 0;

        let mut f = fs::OpenOptions::new().create(true).create_new(true).read(true).write(true).open(&path)?;

        // Write header

        let header = Header::new(Type::Rotbl, Version::V001);
        n += header.encode(&mut f)?;

        // Write table id

        let tid = WithChecksum::new(table_id);
        n += tid.encode(&mut f)?;

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
        };

        Ok(r)
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

        let mut cache = self.block_cache.lock().unwrap();
        if let Some(b) = cache.get(&block_id).cloned() {
            return Ok(b);
        }

        let block_index_entry = self.block_index.get_index_entry_by_num(block_num).unwrap();

        let mut buf = new_uninitialized(block_index_entry.size as usize);

        {
            let mut f = self.file.lock().unwrap();
            f.seek(io::SeekFrom::Start(block_index_entry.offset))?;
            f.read_exact(&mut buf)?;
        }

        let block = Block::decode(&mut buf.as_slice())?;
        let block = Arc::new(block);

        cache.insert(block_id, block.clone());

        Ok(block)
    }

    pub fn io_driver(&self) -> IODriver {
        IODriver {
            rotbl: self,
            io: IOPort::new(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::redundant_clone)]
#[allow(clippy::vec_init_then_push)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use futures::StreamExt;
    use tempfile::TempDir;

    use crate::typ::Type;
    use crate::v001::block_index::BlockIndex;
    use crate::v001::block_index::BlockMeta;
    use crate::v001::config::Config;
    use crate::v001::db::DB;
    use crate::v001::footer::Footer;
    use crate::v001::header::Header;
    use crate::v001::rotbl::Rotbl;
    use crate::v001::testing::bb;
    use crate::v001::testing::ss;
    use crate::v001::tseq::TSeqValue;
    use crate::version::Version;

    #[test]
    fn test_create_table() -> anyhow::Result<()> {
        let ctx = TestContext::new()?;
        let p = ctx.db_path();

        let (t, index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

        println!("{:?}", t);

        assert_eq!(t.header, Header::new(Type::Rotbl, Version::V001));
        assert_eq!(t.table_id, 12);
        assert_eq!(t.block_index, BlockIndex {
            header: Header::new(Type::BlockIndex, Version::V001),
            // It is created, has not encoded size
            data_encoded_size: 0,
            data: index_data.clone(),
        });

        assert_eq!(t.footer, Footer::new(258));

        Ok(())
    }

    #[test]
    fn test_open_table() -> anyhow::Result<()> {
        let ctx = TestContext::new()?;
        let p = ctx.db_path();

        let (t, index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

        println!("{:?}", t);

        let t = Rotbl::open(ctx.db(), p.join("foo.rot"))?;

        assert_eq!(t.header, Header::new(Type::Rotbl, Version::V001));
        assert_eq!(t.table_id, 12);
        assert_eq!(t.block_index, BlockIndex {
            header: Header::new(Type::BlockIndex, Version::V001),
            // It is set when decode()
            data_encoded_size: 141,
            data: index_data.clone(),
        });

        assert_eq!(t.footer, Footer::new(258));

        Ok(())
    }

    #[test]
    fn test_rotbl_load_block() -> anyhow::Result<()> {
        let ctx = TestContext::new()?;
        let p = ctx.db_path();

        let (t, _index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

        {
            let b = t.load_block(0)?;
            let keys = b.range::<String, _>(..).map(|(k, _)| k.clone()).collect::<Vec<_>>();
            assert_eq!(keys, vec!["a", "b", "c"]);
        }

        {
            let b = t.load_block(1)?;
            let keys = b.range::<String, _>(..).map(|(k, _)| k.clone()).collect::<Vec<_>>();
            assert_eq!(keys, vec!["d"]);
        }

        Ok(())
    }

    #[test]
    fn test_rotbl_get_block() -> anyhow::Result<()> {
        let ctx = TestContext::new()?;
        let p = ctx.db_path();

        let (t, _index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

        assert!(t.get_block(0).is_none());
        t.load_block(0)?;

        {
            // Block is filled into the cache.
            let b = t.get_block(0).unwrap();
            let keys = b.range::<String, _>(..).map(|(k, _)| k.clone()).collect::<Vec<_>>();
            assert_eq!(keys, vec!["a", "b", "c"]);
        }

        Ok(())
    }

    #[test]
    fn test_rotbl_get() -> anyhow::Result<()> {
        let ctx = TestContext::new()?;
        let p = ctx.db_path();

        let (_t, _index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

        let t = Rotbl::open(ctx.db(), p.join("foo.rot"))?;

        let drv = t.io_driver();

        // Get from non-existent block

        let fu = drv.get("e");
        let got = drv.block_on(fu)?.map(|v| v.data_ref().clone());

        assert_eq!(None, got);

        // Get non-existent from existent block

        let fu = drv.get("a1");
        let got = drv.block_on(fu)?.map(|v| v.data_ref().clone());

        assert_eq!(None, got);

        // Get from non-cached block

        let fu = drv.get("a");
        let got = drv.block_on(fu)?.map(|v| v.data_ref().clone());

        assert_eq!(Some(bb("A")), got);

        // Get from cached block

        let fu = drv.get("a");
        let got = drv.block_on(fu)?.map(|v| v.data_ref().clone());

        assert_eq!(Some(bb("A")), got);

        Ok(())
    }

    #[test]
    fn test_rotbl_range() -> anyhow::Result<()> {
        let ctx = TestContext::new()?;
        let p = ctx.db_path();

        let (_t, _index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

        let t = Rotbl::open(ctx.db(), p.join("foo.rot"))?;

        let drv = t.io_driver();

        // Full range

        let r = drv.range(..);
        let fu = r.map(|(k, _v)| k.clone()).collect::<Vec<_>>();
        let got_keys = drv.block_on(fu)?;

        assert_eq!(vec![ss("a"), ss("b"), ss("c"), ss("d")], got_keys);

        // Sub range in block 0

        let r = drv.range(ss("a1")..=ss("c"));
        let fu = r.map(|(k, _v)| k.clone()).collect::<Vec<_>>();
        let got_keys = drv.block_on(fu)?;

        assert_eq!(vec![ss("b"), ss("c")], got_keys);

        Ok(())
    }

    #[test]
    fn test_rotbl_io_driver_run_range() -> anyhow::Result<()> {
        let ctx = TestContext::new()?;
        let p = ctx.db_path();

        let (_t, _index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

        let t = Rotbl::open(ctx.db(), p.join("foo.rot"))?;

        let drv = t.io_driver();

        let got_keys = drv.run(|drv| {
            let f = async move {
                let r = drv.range(..);
                r.map(|(k, _v)| k).collect::<Vec<_>>().await
            };
            Box::pin(f)
        })?;

        assert_eq!(vec![ss("a"), ss("b"), ss("c"), ss("d")], got_keys);

        Ok(())
    }

    #[test]
    fn test_rotbl_no_cache() -> anyhow::Result<()> {
        let mut config = Config::default();
        config.block.max_items = Some(3);
        config.disable_cache();

        let ctx = TestContext::with_config(config)?;
        let p = ctx.db_path();

        let (_t, _index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

        let t = Rotbl::open(ctx.db(), p.join("foo.rot"))?;

        let drv = t.io_driver();

        let got_keys = drv.run(|drv| {
            let f = async move {
                let r = drv.range(..);
                let mut v1 = r.map(|(k, _v)| k).collect::<Vec<_>>().await;

                let r = drv.range(..);
                let v2 = r.map(|(k, _v)| k).collect::<Vec<_>>().await;
                v1.extend(v2);
                v1
            };
            Box::pin(f)
        })?;

        assert_eq!(
            vec![ss("a"), ss("b"), ss("c"), ss("d"), ss("a"), ss("b"), ss("c"), ss("d"),],
            got_keys
        );

        Ok(())
    }

    pub(crate) struct TestContext {
        #[allow(dead_code)]
        config: Config,
        db: Arc<DB>,

        temp_dir: TempDir,
    }

    impl TestContext {
        pub(crate) fn new() -> anyhow::Result<TestContext> {
            let mut config = Config::default();
            config.block.max_items = Some(3);

            Self::with_config(config)
        }

        pub(crate) fn with_config(config: Config) -> anyhow::Result<TestContext> {
            let db = DB::open(config.clone())?;
            let temp_dir = tempfile::tempdir()?;

            Ok(TestContext { config, db, temp_dir })
        }

        pub(crate) fn db(&self) -> &Arc<DB> {
            &self.db
        }

        pub(crate) fn db_path(&self) -> &Path {
            self.temp_dir.path()
        }
    }

    /// Create a temp table and return the rotbl and expected block index
    ///
    /// Table data:
    /// ```text
    /// ---
    /// a: 1, false, A,
    /// b: 2, true, B,
    /// c: 2, true, C,
    /// ---
    /// d: 2, true, D,
    /// ---
    /// ```
    fn create_tmp_table<P: AsRef<Path>>(db: &DB, path: P) -> anyhow::Result<(Rotbl, Vec<BlockMeta>)> {
        let kvs = maplit::btreemap! {
            ss("a") => TSeqValue::new(1,false, bb("A")),
            ss("b") => TSeqValue::new(2,true, bb("B")),
            ss("c") => TSeqValue::new(2,true, bb("C")),
            ss("d") => TSeqValue::new(2,true, bb("D")),
        };

        let t = Rotbl::create_table(db, path, 12, kvs)?;

        let mut index_data = Vec::new();
        index_data.push(BlockMeta {
            block_num: 0,
            offset: 36,
            size: 138,
            first_key: ss("a"),
            last_key: ss("c"),
        });
        index_data.push(BlockMeta {
            block_num: 1,
            offset: 174,
            size: 84,
            first_key: ss("d"),
            last_key: ss("d"),
        });

        Ok((t, index_data))
    }
}
