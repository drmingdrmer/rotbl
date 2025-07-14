use std::io;
use std::sync::Arc;

use libtest_mimic::Trial;
use rotbl::storage::Storage;
use rotbl::v001::Dump;

use crate::context::TestContext;
use crate::temp_table::create_tmp_table;
use crate::trials;
use crate::utils::NewContext;

pub(crate) fn tests<S: Storage>(new_ctx: impl NewContext<S>, trials: &mut Vec<Trial>) {
    trials.extend(trials!(new_ctx, test_dump));
}

fn test_dump<S: Storage>(ctx: TestContext<S>) -> anyhow::Result<()> {
    let (t, index_data) = create_tmp_table(ctx.storage(), ctx.new_db()?.as_ref(), "foo.rot")?;
    let _ = index_data;

    let d = Dump::new(Arc::new(t));
    let got = d.dump().collect::<Result<Vec<_>, io::Error>>()?;

    let want = vec![
        r#"Rotbl:"#,
        r#"    header: {typ: Rotbl, version: V001}"#,
        r#"    file_size: 593"#,
        r#"    meta: {header: {typ: RotblMeta, version: V001}, payload: {seq: 5, user_data: hello}}"#,
        r#"    stat: 4 keys in 2 blocks: data(136 B), index(188 B), avg block size(68 B)"#,
        r#"    access_stat: AccessStat { read_key: 0, read_block: 0, read_block_from_cache: 0, read_block_from_disk: 0 }"#,
        r#"BlockIndex: n: 2"#,
        r#"    index: { block_num: 0000, position: 36+73, key_range: ["a", "c"] }"#,
        r#"    index: { block_num: 0001, position: 109+63, key_range: ["d", "d"] }"#,
        r#"Block-0000: a: {seq: 1, TOMBSTONE}"#,
        r#"Block-0000: b: {seq: 2, ([66])}"#,
        r#"Block-0000: c: {seq: 2, ([67])}"#,
        r#"Block-0001: d: {seq: 2, ([68])}"#,
    ];

    assert_eq!(want, got);

    Ok(())
}
