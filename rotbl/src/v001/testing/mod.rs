#![allow(dead_code)]

use crate::v001::SeqMarked;

/// Create a string
pub(crate) fn ss(x: impl ToString) -> String {
    x.to_string()
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
