use codeq::Segment;
use rotbl::storage::Storage;
use rotbl::v001::BlockIndexEntry;
use rotbl::v001::Rotbl;
use rotbl::v001::RotblMeta;
use rotbl::v001::SeqMarked;
use rotbl::v001::DB;

use crate::utils::bb;
use crate::utils::ss;

/// Create a temp table and return the rotbl and expected block index
///
/// Table data:
/// ```text
/// ---
/// a: 1, false, A,
/// b: 2, true, B,
/// c: 2, true, C,
/// ---
/// d: 2, true, D,
/// ---
/// ```
pub fn create_tmp_table<S>(
    storage: S,
    db: &DB,
    path: &str,
) -> anyhow::Result<(Rotbl, Vec<BlockIndexEntry>)>
where
    S: Storage,
{
    let kvs = maplit::btreemap! {
        ss("a") => SeqMarked::new_tombstone(1),
        ss("b") => SeqMarked::new_normal(2, bb("B")),
        ss("c") => SeqMarked::new_normal(2, bb("C")),
        ss("d") => SeqMarked::new_normal(2, bb("D")),
    };

    let rotbl_meta = RotblMeta::new(5, "hello");
    let t = Rotbl::create_table(storage, db.config(), path, rotbl_meta, kvs)?;

    let mut index_data = Vec::new();
    index_data.push(BlockIndexEntry::new(
        0,
        Segment::new(36, 73),
        ss("a"),
        ss("c"),
    ));
    index_data.push(BlockIndexEntry::new(
        1,
        Segment::new(109, 63),
        ss("d"),
        ss("d"),
    ));

    Ok((t, index_data))
}
