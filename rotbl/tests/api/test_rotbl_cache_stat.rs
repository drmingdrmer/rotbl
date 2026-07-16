use std::sync::Arc;

use libtest_mimic::Trial;
use rotbl::storage::Storage;
use rotbl::v001::CacheStat;

use crate::async_trials;
use crate::context::TestContext;
use crate::temp_table;
use crate::utils::NewContext;

pub fn tests<S: Storage>(new_ctx: impl NewContext<S>, trials: &mut Vec<Trial>) {
    trials.extend(async_trials!(new_ctx, test_rotbl_cache_row_groups));
}

async fn test_rotbl_cache_row_groups<S: Storage>(mut ctx: TestContext<S>) -> anyhow::Result<()> {
    let config = ctx.config_mut();
    config.block_config.max_items = Some(4);
    config.block_config.row_group_max_items = Some(1);
    config.block_cache.capacity = Some(4 * 1024);

    let (t, _index_data) =
        temp_table::create_tmp_table(ctx.storage(), ctx.new_db()?.as_ref(), "foo.rot")?;

    let t = Arc::new(t);

    // V003 caches a directory plus each accessed compressed row group.
    t.get("a").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(2, 398));
    t.get("b").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(3, 422));
    t.get("c").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(4, 446));
    t.get("d").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(5, 470));
    t.get("a").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(5, 470));
    t.get("b").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(5, 470));

    Ok(())
}
