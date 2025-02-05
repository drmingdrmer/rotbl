use std::fmt;

/// Data that can be marked as tombstone.
///
/// ## `PartialOrd` implementation
///
/// A tombstone will always be greater than normal marked value.
/// This is because for a single record, tombstone can only be created through a deletion operation,
/// and a deletion is always after creating the normal record.
#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
#[derive(PartialOrd, Ord)]
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(bincode::Encode, bincode::Decode)]
pub enum Marked<D> {
    // Keep `Normal` as the first variant so that `TombStone` is greater than `Normal`.
    Normal(D),
    TombStone,
}

/// Marked data with a sequence number.
///
/// ## `PartialOrd` implementation
///
/// If they share the same seq number, a tombstone will always be greater than normal marked value.
/// This is because for a single record, tombstone can only be created through a deletion operation,
/// and a deletion is always after creating the normal record.
#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
#[derive(PartialOrd, Ord)]
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(bincode::Encode, bincode::Decode)]
pub struct SeqMarked<D = Vec<u8>> {
    // Keep the `seq` as the first field so that it can be compared first.
    seq: u64,
    marked: Marked<D>,
}

impl<D> SeqMarked<D> {
    pub fn new_normal(seq: u64, data: D) -> Self {
        Self {
            seq,
            marked: Marked::Normal(data),
        }
    }

    pub fn new_tombstone(seq: u64) -> Self {
        Self {
            seq,
            marked: Marked::TombStone,
        }
    }

    pub fn is_normal(&self) -> bool {
        !self.is_tombstone()
    }

    pub fn is_tombstone(&self) -> bool {
        match self.marked {
            Marked::Normal(_) => false,
            Marked::TombStone => true,
        }
    }

    /// Convert the marked data to a new marked data with a different data type.
    pub fn map<U>(self, f: impl FnOnce(D) -> U) -> SeqMarked<U> {
        SeqMarked {
            seq: self.seq,
            marked: match self.marked {
                Marked::Normal(data) => Marked::<U>::Normal(f(data)),
                Marked::TombStone => Marked::<U>::TombStone,
            },
        }
    }

    /// Convert the marked data to a ref.
    pub fn as_ref(&self) -> SeqMarked<&D> {
        SeqMarked {
            seq: self.seq,
            marked: match &self.marked {
                Marked::Normal(data) => Marked::Normal(data),
                Marked::TombStone => Marked::TombStone,
            },
        }
    }

    /// Return a key to determine which one of the values of the same key are the last inserted.
    pub fn order_key(&self) -> SeqMarked<()> {
        SeqMarked {
            seq: self.seq,
            marked: match &self.marked {
                Marked::Normal(_) => Marked::Normal(()),
                Marked::TombStone => Marked::TombStone,
            },
        }
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn data_ref(&self) -> Option<&D> {
        match self.marked {
            Marked::Normal(ref d) => Some(d),
            Marked::TombStone => None,
        }
    }

    pub fn into_data(self) -> Option<D> {
        match self.marked {
            Marked::Normal(data) => Some(data),
            Marked::TombStone => None,
        }
    }

    /// Return a struct that implements `fmt::Display` for SeqMarked.
    pub fn display(&self) -> impl fmt::Display + '_
    where D: fmt::Debug {
        struct DisplaySeqMarked<'a, D>(&'a SeqMarked<D>);

        impl<'a, D> fmt::Display for DisplaySeqMarked<'a, D>
        where D: fmt::Debug
        {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "seq: {}, ", self.0.seq)?;
                match &self.0.marked {
                    Marked::Normal(data) => {
                        write!(f, "data: {:?}", data)
                    }
                    Marked::TombStone => write!(f, "tombstone"),
                }
            }
        }

        DisplaySeqMarked(self)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use Ordering::Equal;
    use Ordering::Greater;
    use Ordering::Less;

    use crate::v001::testing::norm;
    use crate::v001::testing::ts;
    use crate::v001::SeqMarked;

    #[test]
    fn test_map() -> anyhow::Result<()> {
        let a = norm(1, 1u64);
        assert_eq!(norm(1, 2u32), a.map(|x| (x * 2) as u32));

        let a = ts::<u64>(1);
        assert_eq!(ts::<u32>(1), a.map(|x| (x * 2) as u32));

        Ok(())
    }

    #[test]
    fn test_as_ref() -> anyhow::Result<()> {
        let a = norm(1, 1u64);
        assert_eq!(norm(1, &1u64), a.as_ref());

        let a = ts::<u64>(1);
        assert_eq!(ts::<&u64>(1), a.as_ref());

        Ok(())
    }

    #[test]
    fn test_order_key() -> anyhow::Result<()> {
        assert!(norm(1, 1u64).order_key() == norm(1, 1u64).order_key());
        assert!(norm(1, 2u64).order_key() == norm(1, 1u64).order_key());
        assert!(norm(2, 2u64).order_key() > norm(1, 1u64).order_key());

        assert!(ts::<u64>(1).order_key() > norm(1, 1u64).order_key());
        assert!(ts::<u64>(2).order_key() > norm(1, 1u64).order_key());

        assert!(ts::<u64>(2).order_key() > ts::<u64>(1).order_key());
        assert!(ts::<u64>(1).order_key() == ts::<u64>(1).order_key());

        Ok(())
    }

    #[test]
    fn test_partial_ord() -> anyhow::Result<()> {
        fn pcmp<D: PartialOrd>(a: &SeqMarked<D>, b: &SeqMarked<D>) -> Option<Ordering> {
            PartialOrd::partial_cmp(a, b)
        }

        // normal vs normal, with the same data

        assert_eq!(Some(Greater), pcmp(&norm(2, 2u64), &norm(1, 2u64)));
        assert_eq!(Some(Equal), pcmp(&norm(2, 2u64), &norm(2, 2u64)));
        assert_eq!(Some(Less), pcmp(&norm(2, 2u64), &norm(3, 2u64)));

        // normal vs normal, same seq, different value

        assert_eq!(Some(Greater), pcmp(&norm(2, 2u64), &norm(2, 1u64)));
        assert_eq!(Some(Equal), pcmp(&norm(2, 2u64), &norm(2, 2u64)));
        assert_eq!(Some(Less), pcmp(&norm(2, 2u64), &norm(2, 3u64)));

        // normal vs tombstone

        assert_eq!(Some(Greater), pcmp(&norm(2, 2u64), &ts(1)));
        assert_eq!(
            Some(Less),
            pcmp(&norm(2, 2u64), &ts(2)),
            "tombstone is greater than a normal with the same seq"
        );
        assert_eq!(Some(Less), pcmp(&norm(2, 2u64), &ts(3)));

        // tombstone vs normal

        assert_eq!(Some(Less), pcmp(&ts(1), &norm(2, 2u64)));
        assert_eq!(
            Some(Greater),
            pcmp(&ts(2), &norm(2, 2u64)),
            "tombstone is greater than a normal with the same seq"
        );
        assert_eq!(Some(Greater), pcmp(&ts(3), &norm(2, 2u64)));

        // tombstone vs tombstone

        assert_eq!(Some(Greater), pcmp(&ts::<()>(2), &ts(1)));
        assert_eq!(Some(Equal), pcmp(&ts::<()>(2), &ts(2)));
        assert_eq!(Some(Less), pcmp(&ts::<()>(2), &ts(3)));
        Ok(())
    }

    #[test]
    fn test_ord_operator() -> anyhow::Result<()> {
        // normal vs normal, with the same data

        assert!(norm(2, 2u64) > norm(1, 2u64));
        assert!(norm(2, 2u64) >= norm(1, 2u64));
        assert!(norm(2, 2u64) == norm(2, 2u64));
        assert!(norm(2, 2u64) <= norm(3, 2u64));
        assert!(norm(2, 2u64) < norm(3, 2u64));

        // normal vs normal, same seq, different value

        assert!(norm(2, 2u64) > norm(2, 1u64));
        assert!(norm(2, 2u64) >= norm(2, 1u64));
        assert!(norm(2, 2u64) == norm(2, 2u64));
        assert!(norm(2, 2u64) <= norm(2, 3u64));
        assert!(norm(2, 2u64) < norm(2, 3u64));

        // normal vs tombstone

        assert!(norm(2, 2u64) > ts(1));
        assert!(norm(2, 2u64) >= ts(1));
        assert!(
            norm(2, 2u64) < ts(2),
            "tombstone is greater than a normal with the same seq"
        );
        assert!(
            norm(2, 2u64) <= ts(2),
            "tombstone is greater than a normal with the same seq"
        );
        assert!(norm(2, 2u64) < ts(3));
        assert!(norm(2, 2u64) <= ts(3));

        // tombstone vs normal

        assert!(ts(1) < norm(2, 2u64));
        assert!(ts(1) <= norm(2, 2u64));
        assert!(
            ts(2) > norm(2, 2u64),
            "tombstone is greater than a normal with the same seq"
        );
        assert!(
            ts(2) >= norm(2, 2u64),
            "tombstone is greater than a normal with the same seq"
        );
        assert!(ts(3) > norm(2, 2u64));
        assert!(ts(3) >= norm(2, 2u64));

        // tombstone vs tombstone

        assert!(ts::<()>(2) > ts(1));
        assert!(ts::<()>(2) >= ts(1));
        assert!(ts::<()>(2) >= ts(2));
        assert!(ts::<()>(2) == ts(2));
        assert!(ts::<()>(2) <= ts(2));
        assert!(ts::<()>(2) <= ts(3));
        assert!(ts::<()>(2) < ts(3));

        Ok(())
    }
}
