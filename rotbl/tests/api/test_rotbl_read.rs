use std::sync::Arc;

use futures::TryStreamExt;
use libtest_mimic::Trial;
use rotbl::storage::Storage;
use rotbl::v001::Rotbl;
use rotbl::v001::SeqMarked;

use crate::async_trials;
use crate::context::TestContext;
use crate::temp_table::create_tmp_table;
use crate::utils::bb;
use crate::utils::ss;
use crate::utils::NewContext;

pub fn tests<S: Storage>(new_ctx: impl NewContext<S>, trials: &mut Vec<Trial>) {
    trials.extend(async_trials!(
        new_ctx,
        test_rotbl_async_get,
        test_rotbl_async_range
    ));
}

async fn test_rotbl_async_get<S: Storage>(ctx: TestContext<S>) -> anyhow::Result<()> {
    let (_t, _index_data) = create_tmp_table(ctx.storage(), ctx.new_db()?.as_ref(), "foo.rot")?;

    let t = Rotbl::open(ctx.storage(), ctx.config(), "foo.rot")?;

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

async fn test_rotbl_async_range<S: Storage>(ctx: TestContext<S>) -> anyhow::Result<()> {
    let (_t, _index_data) = create_tmp_table(ctx.storage(), ctx.new_db()?.as_ref(), "foo.rot")?;

    let t = Rotbl::open(ctx.storage(), ctx.config(), "foo.rot")?;
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
