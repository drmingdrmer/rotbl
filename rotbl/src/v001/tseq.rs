/// Sequence number with embedded tombstone flag.
///
/// The even number seq is normal data, and the odd number seq is tombstone.
/// To mark a record as tombstone: `seq = seq | 1` .
///
/// So that a tombstone record is always greater than the corresponding normal data and will
/// override it.
#[derive(Debug)]
#[derive(Clone, Copy)]
#[derive(PartialEq, Eq)]
#[derive(PartialOrd, Ord)]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct TSeq(u64);

impl TSeq {
    pub fn new(seq: u64, tombstone: bool) -> Self {
        debug_assert!(seq < u64::MAX / 2);
        let s = seq * 2 + if tombstone { 1 } else { 0 };
        Self(s)
    }

    pub fn is_tombstone(&self) -> bool {
        self.0 & 1 == 1
    }
}

/// Some data with a sequence number.
#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct TSeqValue<D = Vec<u8>> {
    tseq: TSeq,
    data: D,
}

impl<D> TSeqValue<D> {
    pub fn new(seq: u64, tombstone: bool, data: D) -> Self {
        Self {
            tseq: TSeq::new(seq, tombstone),
            data,
        }
    }
    pub fn data_ref(&self) -> &D {
        &self.data
    }
}
