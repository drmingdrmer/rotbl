use libtest_mimic::Trial;
use rotbl::storage::Storage;
use rotbl::typ::Type;
use rotbl::v001::stat::RotblStat;
use rotbl::v001::BlockIndex;
use rotbl::v001::Footer;
use rotbl::v001::Header;
use rotbl::v001::Rotbl;
use rotbl::v001::Segment;
use rotbl::version::Version;
use temp_table::create_tmp_table;

use crate::context::TestContext;
use crate::temp_table;
use crate::trials;
use crate::utils::NewContext;

pub(crate) fn tests<S: Storage>(new_ctx: impl NewContext<S>, trials: &mut Vec<Trial>) {
    trials.extend(trials!(new_ctx, test_create_table, test_open_table));
}

fn test_create_table<S: Storage>(ctx: TestContext<S>) -> anyhow::Result<()> {
    let (t, index_data) = create_tmp_table(ctx.storage(), ctx.new_db()?.as_ref(), "foo.rot")?;

    // println!("{:?}", t);

    assert_eq!(t.header(), &Header::new(Type::Rotbl, Version::V001));
    // assert_eq!(t.table_id, 12);
    assert_eq!(t.table_id(), 0, "table_id is disabled in this version");
    assert_eq!(t.meta().user_data(), "hello");
    assert_eq!(t.meta().seq(), 5);
    assert_eq!(t.block_index(), &BlockIndex::new(index_data.clone()));

    assert_eq!(t.stat(), &RotblStat {
        block_num: 2,
        key_num: 4,
        data_size: 136,
        index_size: 188,
    });

    assert_eq!(
        t.footer(),
        &Footer::new(
            Segment::new(172, 188),
            Segment::new(360, 77),
            Segment::new(437, 84)
        )
    );

    assert_eq!(593, t.file_size());

    Ok(())
}

fn test_open_table<S: Storage>(ctx: TestContext<S>) -> anyhow::Result<()> {
    let (t, index_data) = create_tmp_table(ctx.storage(), ctx.new_db()?.as_ref(), "foo.rot")?;

    let _ = t;
    // println!("{:?}", t);

    let t = Rotbl::open(ctx.storage(), ctx.config(), "foo.rot")?;

    assert_eq!(t.header(), &Header::new(Type::Rotbl, Version::V001));
    // assert_eq!(t.table_id, 12);
    assert_eq!(t.table_id(), 0, "table_id is disabled in this version");
    assert_eq!(t.meta().user_data(), "hello");
    assert_eq!(t.meta().seq(), 5);
    assert_eq!(
        t.block_index(),
        &BlockIndex::new(index_data.clone()).with_encoded_size(140)
    );

    assert_eq!(t.stat(), &RotblStat {
        block_num: 2,
        key_num: 4,
        data_size: 136,
        index_size: 188,
    });

    assert_eq!(
        t.footer(),
        &Footer::new(
            Segment::new(172, 188),
            Segment::new(360, 77),
            Segment::new(437, 84)
        )
    );
    assert_eq!(593, t.file_size());

    Ok(())
}
