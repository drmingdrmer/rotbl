//! Export rotbl data

use std::io;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;
use std::sync::Arc;

use crate::v001::Rotbl;

pub struct Dump {
    rotbl: Arc<Rotbl>,
}

pub struct DumpIter {
    coro: Pin<Box<dyn Coroutine<Yield = String, Return = Result<(), io::Error>>>>,
}

impl Iterator for DumpIter {
    type Item = Result<String, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.coro.as_mut().resume(()) {
            CoroutineState::Yielded(s) => Some(Ok(s)),
            CoroutineState::Complete(res) => match res {
                Ok(()) => None,
                Err(e) => Some(Err(e)),
            },
        }
    }
}

impl Dump {
    pub fn new(rotbl: Arc<Rotbl>) -> Self {
        Dump { rotbl }
    }

    /// Dump rotbl information to human readable lines in an iterator.
    pub fn dump(self) -> impl Iterator<Item = Result<String, io::Error>> {
        let c = self.dump_coro();
        DumpIter { coro: Box::pin(c) }
    }

    /// Dump rotbl information to human readable lines. Return a coroutine.
    pub fn dump_coro(self) -> impl Coroutine<Yield = String, Return = Result<(), io::Error>> {
        #[coroutine]
        move || {
            yield "Rotbl:".to_string();
            yield format!("    header: {}", self.rotbl.header());
            yield format!("    file_size: {}", self.rotbl.file_size());
            yield format!("    meta: {}", self.rotbl.meta());
            yield format!("    stat: {}", self.rotbl.stat());
            yield format!("    access_stat: {:?}", self.rotbl.access_stat());

            // Block index

            let bi = self.rotbl.block_index.iter_index_entries().cloned().collect::<Vec<_>>();
            yield format!("BlockIndex: n: {}", bi.len());
            for ent in bi.into_iter() {
                yield format!("    index: {}", ent);
            }

            // Block data

            for block_num in 0..self.rotbl.stat.block_num {
                let block = self.rotbl.load_block(block_num)?;
                let kvs = block
                    .range::<String, _>(..)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Vec<_>>();

                for (k, v) in kvs {
                    yield format!("Block-{:>04}: {}: {}", block_num, k, v.display_with_debug());
                }
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;

    use crate::v001::rotbl::dump::Dump;
    use crate::v001::rotbl::tests::rotbl_test::create_tmp_table;
    use crate::v001::rotbl::tests::rotbl_test::TestContext;

    #[test]
    fn test_dump() -> anyhow::Result<()> {
        let ctx = TestContext::new()?;
        let p = ctx.db_path();

        let (t, index_data) = create_tmp_table(ctx.db(), p.join("foo.rot"))?;
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
            r#"Block-0000: a: seq: 1, tombstone"#,
            r#"Block-0000: b: seq: 2, data: [66]"#,
            r#"Block-0000: c: seq: 2, data: [67]"#,
            r#"Block-0001: d: seq: 2, data: [68]"#,
        ];

        assert_eq!(want, got);

        Ok(())
    }
}
