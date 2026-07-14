#![allow(dead_code)]

use std::sync::Arc;

use crate::v001::SeqMarked;
use crate::v001::TableInfo;
use crate::v001::TableRecord;

/// Create a string
pub(crate) fn ss(x: impl ToString) -> String {
    x.to_string()
}

/// Build a [`TableInfo`] with the given id, level and inclusive `[smallest,
/// largest]` extent. Shared by the manifest submodule unit tests.
pub(crate) fn table_info(table_id: u32, level: u32, smallest: &str, largest: &str) -> TableInfo {
    TableInfo::new(
        level,
        Arc::new(TableRecord::new(table_id, smallest, largest)),
    )
}

/// Create a String vector from multiple strings
pub(crate) fn ss_vec(x: impl IntoIterator<Item = impl ToString>) -> Vec<String> {
    let r = x.into_iter().map(|x| x.to_string());
    r.collect()
}

/// Create a byte vector
pub(crate) fn bb(x: impl ToString) -> Vec<u8> {
    x.to_string().into_bytes()
}

/// Create a byte vector from multiple strings
pub(crate) fn bbs(x: impl IntoIterator<Item = impl ToString>) -> Vec<u8> {
    let r = x.into_iter().map(|x| x.to_string().into_bytes());
    vec_chain(r)
}

/// Concat multiple Vec into one.
pub(crate) fn vec_chain<T>(vectors: impl IntoIterator<Item = Vec<T>>) -> Vec<T> {
    let mut r = vec![];
    for v in vectors {
        r.extend(v);
    }
    r
}

/// Create a `SeqMarked::Normal`.
pub(crate) fn norm<D>(seq: u64, d: D) -> SeqMarked<D> {
    SeqMarked::new_normal(seq, d)
}

/// Create a `SeqMarked::TombStone`.
pub(crate) fn ts<D>(seq: u64) -> SeqMarked<D> {
    SeqMarked::new_tombstone(seq)
}
