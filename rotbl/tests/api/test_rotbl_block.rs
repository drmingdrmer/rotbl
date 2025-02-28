use crate::context::TestContext;
use crate::temp_table;

#[test]
fn test_rotbl_get_block() -> anyhow::Result<()> {
    let ctx = TestContext::new()?;

    let (t, _index_data) = temp_table::create_tmp_table(ctx.storage(), ctx.db(), "foo.rot")?;

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
fn test_rotbl_load_block() -> anyhow::Result<()> {
    let ctx = TestContext::new()?;

    let (t, _index_data) = temp_table::create_tmp_table(ctx.storage(), ctx.db(), "foo.rot")?;

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
