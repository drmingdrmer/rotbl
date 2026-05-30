use std::sync::Arc;

use libtest_mimic::Trial;
use rotbl::storage::Storage;
use rotbl::v001::CacheStat;

use crate::async_trials;
use crate::context::TestContext;
use crate::temp_table;
use crate::utils::NewContext;

pub fn tests<S: Storage>(new_ctx: impl NewContext<S>, trials: &mut Vec<Trial>) {
    trials.extend(async_trials!(new_ctx, test_rotbl_cache_cap_limit));
}

async fn test_rotbl_cache_cap_limit<S: Storage>(mut ctx: TestContext<S>) -> anyhow::Result<()> {
    let config = ctx.config_mut();
    config.block_config.max_items = Some(1);
    config.block_cache.capacity = Some(20);

    let (t, _index_data) =
        temp_table::create_tmp_table(ctx.storage(), ctx.new_db()?.as_ref(), "foo.rot")?;

    let t = Arc::new(t);

    let stat = t.stat();
    let _ = stat;
    // println!("{}", stat);

    let cache_stat = t.cache_stat();
    let _ = cache_stat;
    // println!("{:?}", cache_stat);

    t.get("a").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(1, 6));
    t.get("b").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(2, 14));
    t.get("c").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(1, 8));
    t.get("d").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(2, 16));
    t.get("a").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(2, 16));
    t.get("b").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(2, 16));

    Ok(())
}
