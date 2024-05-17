use crate::v001::rotbl::tests::rotbl_test;
use crate::v001::rotbl::tests::rotbl_test::TestContext;
use crate::v001::CacheStat;
use crate::v001::Config;

#[tokio::test(flavor = "multi_thread")]
async fn test_rotbl_cache_cap_limit() -> anyhow::Result<()> {
    let mut config = Config::default();
    config.block_config.max_items = Some(1);
    config.block_cache.capacity = Some(20);
    config.block_cache.max_items = Some(100);

    let ctx = TestContext::with_config(config)?;
    let p = ctx.db_path();

    let (t, _index_data) = rotbl_test::create_tmp_table(ctx.db(), p.join("foo.rot"))?;

    let stat = t.stat();
    println!("{}", stat);

    let cache_stat = t.cache_stat();
    println!("{:?}", cache_stat);

    t.get("a").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(1, 5));
    t.get("b").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(2, 12));
    t.get("c").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(3, 19));
    t.get("d").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(2, 14));
    t.get("a").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(3, 19));
    t.get("b").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(3, 19));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rotbl_cache_item_limit() -> anyhow::Result<()> {
    let mut config = Config::default();
    config.block_config.max_items = Some(1);
    config.block_cache.capacity = Some(100);
    config.block_cache.max_items = Some(3);

    let ctx = TestContext::with_config(config)?;
    let p = ctx.db_path();

    let (t, _index_data) = rotbl_test::create_tmp_table(ctx.db(), p.join("foo.rot"))?;

    let stat = t.stat();
    println!("{}", stat);

    let cache_stat = t.cache_stat();
    println!("{:?}", cache_stat);

    t.get("a").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(1, 5));
    t.get("b").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(2, 12));
    t.get("c").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(3, 19));
    t.get("d").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(3, 21));
    t.get("a").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(3, 19));
    t.get("b").await?;
    assert_eq!(t.cache_stat(), CacheStat::new(3, 19));

    Ok(())
}
