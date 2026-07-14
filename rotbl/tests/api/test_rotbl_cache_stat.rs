use std::sync::Arc;

use libtest_mimic::Trial;
use rotbl::storage::Storage;
use rotbl::v001::CacheStat;
use rotbl::v001::RotblMeta;
use rotbl::v001::SeqMarked;

use crate::async_trials;
use crate::context::TestContext;
use crate::temp_table;
use crate::utils::bb;
use crate::utils::ss;
use crate::utils::NewContext;

pub fn tests<S: Storage>(new_ctx: impl NewContext<S>, trials: &mut Vec<Trial>) {
    trials.extend(async_trials!(
        new_ctx,
        test_rotbl_cache_cap_limit,
        test_db_shared_block_cache
    ));
}

async fn test_rotbl_cache_cap_limit<S: Storage>(mut ctx: TestContext<S>) -> anyhow::Result<()> {
    let config = ctx.config_mut();
    config.block_config.max_items = Some(1);
    config.block_cache.capacity = Some(40);

    let (t, _index_data) =
        temp_table::create_tmp_table(ctx.storage(), ctx.new_db()?.as_ref(), "foo.rot")?;

    let t = Arc::new(t);

    let stat = t.stat();
    let _ = stat;
    // println!("{}", stat);

    let cache_stat = t.cache_stat();
    let _ = cache_stat;
    // println!("{:?}", cache_stat);

    // Cache weights are encoded block sizes, tracking the pinned libzstd (zstd-sys, Cargo.toml).
    t.get("a").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(1, 16));
    t.get("b").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(2, 34));
    t.get("c").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(1, 18));
    t.get("d").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(2, 36));
    t.get("a").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(2, 36));
    t.get("b").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(2, 36));

    Ok(())
}

/// Tables opened in one DB share that DB's block cache, namespaced by a unique
/// per-table id so blocks from different tables never alias.
async fn test_db_shared_block_cache<S: Storage>(ctx: TestContext<S>) -> anyhow::Result<()> {
    let db = ctx.new_db()?;

    let t1 = db.create_table(
        ctx.storage(),
        "t1.rot",
        RotblMeta::new(1, "t1"),
        maplit::btreemap! {
            ss("a") => SeqMarked::new_normal(1, bb("A")),
            ss("b") => SeqMarked::new_normal(2, bb("B")),
        },
    )?;
    let t2 = db.create_table(
        ctx.storage(),
        "t2.rot",
        RotblMeta::new(2, "t2"),
        maplit::btreemap! {
            ss("x") => SeqMarked::new_normal(3, bb("X")),
            ss("y") => SeqMarked::new_normal(4, bb("Y")),
        },
    )?;

    let b1 = t1.load_block(0)?;
    let b2 = t2.load_block(0)?;

    // Each table's block 0 holds only its own keys — no cross-table aliasing.
    assert_eq!(b1.get("a"), Some(&SeqMarked::new_normal(1, bb("A"))));
    assert_eq!(b1.get("x"), None);
    assert_eq!(b2.get("x"), Some(&SeqMarked::new_normal(3, bb("X"))));
    assert_eq!(b2.get("a"), None);

    // Both blocks live in the single DB cache, keyed by distinct table_ids.
    assert_eq!(db.cache_stat(), CacheStat::new(2, 48));

    Ok(())
}
