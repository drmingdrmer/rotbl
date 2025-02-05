#![allow(clippy::redundant_clone)]
#![allow(clippy::vec_init_then_push)]

use std::sync::Arc;

use futures::TryStreamExt;

use crate::v001::rotbl::tests::rotbl_test::create_tmp_table;
use crate::v001::rotbl::tests::rotbl_test::TestContext;
use crate::v001::rotbl::Rotbl;
use crate::v001::testing::bb;
use crate::v001::testing::ss;
use crate::v001::SeqMarked;

#[tokio::test(flavor = "multi_thread")]
async fn test_rotbl_async_get() -> anyhow::Result<()> {
    let ctx = TestContext::new()?;
    let p = ctx.db_path();

    let (_t, _index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

    let t = Rotbl::open(ctx.db().config.clone(), p.join("foo.rot"))?;

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
    let p = ctx.db_path();

    let (_t, _index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;

    let t = Rotbl::open(ctx.db().config.clone(), p.join("foo.rot"))?;
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
