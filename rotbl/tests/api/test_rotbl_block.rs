use libtest_mimic::Trial;
use rotbl::storage::Storage;

use crate::context::TestContext;
use crate::temp_table;
use crate::trials;
use crate::utils::NewContext;

pub(crate) fn tests<S: Storage>(new_ctx: impl NewContext<S>, trials: &mut Vec<Trial>) {
    trials.extend(trials!(
        new_ctx,
        test_rotbl_get_block,
        test_rotbl_load_block
    ));
}

fn test_rotbl_get_block<S: Storage>(ctx: TestContext<S>) -> anyhow::Result<()> {
    let (t, _index_data) =
        temp_table::create_tmp_table(ctx.storage(), ctx.new_db()?.as_ref(), "foo.rot")?;

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

fn test_rotbl_load_block<S: Storage>(ctx: TestContext<S>) -> anyhow::Result<()> {
    let (t, _index_data) =
        temp_table::create_tmp_table(ctx.storage(), ctx.new_db()?.as_ref(), "foo.rot")?;

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
