use std::sync::Arc;

use futures::TryStreamExt;
use rotbl::storage::impls::fs::FsStorage;
use rotbl::v001::Rotbl;
use rotbl::v001::SeqMarked;

use crate::context::TestContext;
use crate::temp_table::create_tmp_table;
use crate::utils::bb;
use crate::utils::ss;

#[tokio::test(flavor = "multi_thread")]
async fn test_rotbl_async_get() -> anyhow::Result<()> {
    let ctx = TestContext::new()?;

    let storage = FsStorage::new(ctx.base_dir().to_path_buf());

    let (_t, _index_data) = create_tmp_table(storage.clone(), ctx.db(), "foo.rot")?;

    let t = Rotbl::open(storage, ctx.db().config(), "foo.rot")?;

    // Get from non-existent block

    let got = t.get("e").await?;
    assert_eq!(None, got);

    // Get non-existent from existent block

    let got = t.get("a1").await?;
    assert_eq!(None, got);

    // Get from non-cached block

    let got = t.get("a").await?;
    assert_eq!(Some(SeqMarked::new_tombstone(1)), got);

    // Get from cached block

    let got = t.get("a").await?;
    assert_eq!(Some(SeqMarked::new_tombstone(1)), got);

    let got = t.get("b").await?;
    assert_eq!(Some(SeqMarked::new_normal(2, bb("B"))), got);

    let got = t.get("d").await?;
    assert_eq!(Some(SeqMarked::new_normal(2, bb("D"))), got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rotbl_async_range() -> anyhow::Result<()> {
    let ctx = TestContext::new()?;

    let storage = FsStorage::new(ctx.base_dir().to_path_buf());

    let (_t, _index_data) = create_tmp_table(storage.clone(), ctx.db(), "foo.rot")?;

    let t = Rotbl::open(storage, ctx.db().config(), "foo.rot")?;
    let t = Arc::new(t);

    // Full range

    let r = t.range(..);
    let got_keys = r.map_ok(|(k, _v)| k).try_collect::<Vec<_>>().await?;

    assert_eq!(vec![ss("a"), ss("b"), ss("c"), ss("d")], got_keys);

    // Sub range in block 0

    let r = t.range(ss("a1")..=ss("c"));
    let got_keys = r.map_ok(|(k, _v)| k).try_collect::<Vec<_>>().await?;

    assert_eq!(vec![ss("b"), ss("c")], got_keys);

    Ok(())
}
