#![allow(clippy::redundant_clone)]
#![allow(clippy::vec_init_then_push)]

use std::path::Path;
use std::sync::Arc;

use futures::StreamExt;
use tempfile::TempDir;

use crate::typ::Type;
use crate::v001::block_index::BlockIndex;
use crate::v001::block_index::BlockIndexEntry;
use crate::v001::config::Config;
use crate::v001::db::DB;
use crate::v001::footer::Footer;
use crate::v001::header::Header;
use crate::v001::rotbl::stat::RotblStat;
use crate::v001::rotbl::Rotbl;
use crate::v001::segment::Segment;
use crate::v001::testing::bb;
use crate::v001::testing::ss;
use crate::v001::RotblMeta;
use crate::v001::SeqMarked;
use crate::version::Version;

#[test]
fn test_create_table() -> anyhow::Result<()> {
    let ctx = TestContext::new()?;
    let p = ctx.db_path();

    let (t, index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

    println!("{:?}", t);

    assert_eq!(t.header, Header::new(Type::Rotbl, Version::V001));
    // assert_eq!(t.table_id, 12);
    assert_eq!(t.table_id, 0, "table_id is disabled in this version");
    assert_eq!(t.meta.user_data(), "hello");
    assert_eq!(t.meta.seq(), 5);
    assert_eq!(t.block_index, BlockIndex {
        header: Header::new(Type::BlockIndex, Version::V001),
        // It is created, has not encoded size
        data_encoded_size: 0,
        data: index_data.clone(),
    });

    assert_eq!(t.stat, RotblStat {
        block_num: 2,
        key_num: 4,
        data_size: 136,
        index_size: 188,
    });

    assert_eq!(
        t.footer,
        Footer::new(
            Segment::new(172, 188),
            Segment::new(360, 77),
            Segment::new(437, 84)
        )
    );

    assert_eq!(593, t.file_size);

    Ok(())
}

#[test]
fn test_open_table() -> anyhow::Result<()> {
    let ctx = TestContext::new()?;
    let p = ctx.db_path();

    let (t, index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

    println!("{:?}", t);

    let t = Rotbl::open(ctx.config(), p.join("foo.rot"))?;

    assert_eq!(t.header, Header::new(Type::Rotbl, Version::V001));
    // assert_eq!(t.table_id, 12);
    assert_eq!(t.table_id, 0, "table_id is disabled in this version");
    assert_eq!(t.meta.user_data(), "hello");
    assert_eq!(t.meta.seq(), 5);
    assert_eq!(t.block_index, BlockIndex {
        header: Header::new(Type::BlockIndex, Version::V001),
        // It is set when decode()
        data_encoded_size: 140,
        data: index_data.clone(),
    });

    assert_eq!(t.stat, RotblStat {
        block_num: 2,
        key_num: 4,
        data_size: 136,
        index_size: 188,
    });

    assert_eq!(
        t.footer,
        Footer::new(
            Segment::new(172, 188),
            Segment::new(360, 77),
            Segment::new(437, 84)
        )
    );
    assert_eq!(593, t.file_size);

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
fn test_rotbl_io_driver_get() -> anyhow::Result<()> {
    let ctx = TestContext::new()?;
    let p = ctx.db_path();

    let (_t, _index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

    let t = Rotbl::open(ctx.config(), p.join("foo.rot"))?;

    let drv = t.io_driver();

    // Get from non-existent block

    let fu = drv.get("e");
    let got = drv.block_on(fu)?;

    assert_eq!(None, got);

    // Get non-existent from existent block

    let fu = drv.get("a1");
    let got = drv.block_on(fu)?;

    assert_eq!(None, got);

    // Get from non-cached block

    let fu = drv.get("a");
    let got = drv.block_on(fu)?;

    assert_eq!(Some(SeqMarked::new_tombstone(1)), got);

    // Get from cached block

    let fu = drv.get("a");
    let got = drv.block_on(fu)?;

    assert_eq!(Some(SeqMarked::new_tombstone(1)), got);

    Ok(())
}

#[test]
fn test_rotbl_io_driver_range() -> anyhow::Result<()> {
    let ctx = TestContext::new()?;
    let p = ctx.db_path();

    let (_t, _index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

    let t = Rotbl::open(ctx.config(), p.join("foo.rot"))?;

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

    let t = Rotbl::open(ctx.config(), p.join("foo.rot"))?;

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
fn test_rotbl_io_driver_get_without_cache() -> anyhow::Result<()> {
    let mut config = Config::default();
    config.block_config.max_items = Some(3);
    config.disable_cache();

    let ctx = TestContext::with_config(config)?;
    let p = ctx.db_path();

    let (_t, _index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

    let t = Rotbl::open(ctx.config(), p.join("foo.rot"))?;

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
        vec![
            ss("a"),
            ss("b"),
            ss("c"),
            ss("d"),
            ss("a"),
            ss("b"),
            ss("c"),
            ss("d"),
        ],
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
        config.block_config.max_items = Some(3);

        Self::with_config(config)
    }

    pub(crate) fn with_config(config: Config) -> anyhow::Result<TestContext> {
        let db = DB::open(config.clone())?;
        let temp_dir = tempfile::tempdir()?;

        Ok(TestContext {
            config,
            db,
            temp_dir,
        })
    }

    pub(crate) fn db(&self) -> &Arc<DB> {
        &self.db
    }

    pub(crate) fn config(&self) -> Config {
        self.config.clone()
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
pub(crate) fn create_tmp_table<P: AsRef<Path>>(
    db: &DB,
    path: P,
) -> anyhow::Result<(Rotbl, Vec<BlockIndexEntry>)> {
    let kvs = maplit::btreemap! {
        ss("a") => SeqMarked::new_tombstone(1),
        ss("b") => SeqMarked::new_normal(2, bb("B")),
        ss("c") => SeqMarked::new_normal(2, bb("C")),
        ss("d") => SeqMarked::new_normal(2, bb("D")),
    };

    let rotbl_meta = RotblMeta::new(5, "hello");
    let t = Rotbl::create_table(db.config(), path, rotbl_meta, kvs)?;

    let mut index_data = Vec::new();
    index_data.push(BlockIndexEntry {
        block_num: 0,
        offset: 36,
        size: 73,
        first_key: ss("a"),
        last_key: ss("c"),
    });
    index_data.push(BlockIndexEntry {
        block_num: 1,
        offset: 109,
        size: 63,
        first_key: ss("d"),
        last_key: ss("d"),
    });

    Ok((t, index_data))
}
