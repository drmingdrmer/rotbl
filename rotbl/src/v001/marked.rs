/// Data that can be marked as tombstone.
#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
#[derive(PartialOrd, Ord)]
#[derive(serde::Serialize, serde::Deserialize)]
pub enum Marked<D> {
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
pub struct SeqMarked<D = Vec<u8>> {
    seq: u64,
    t: Marked<D>,
}

impl<D> SeqMarked<D> {
    pub fn new_normal(seq: u64, data: D) -> Self {
        Self {
            seq,
            t: Marked::Normal(data),
        }
    }

    pub fn new_tombstone(seq: u64) -> Self {
        Self {
            seq,
            t: Marked::TombStone,
        }
    }

    pub fn is_normal(&self) -> bool {
        !self.is_tombstone()
    }

    pub fn is_tombstone(&self) -> bool {
        match self.t {
            Marked::Normal(_) => false,
            Marked::TombStone => true,
        }
    }

    pub fn data_ref(&self) -> Option<&D> {
        match self.t {
            Marked::Normal(ref d) => Some(d),
            Marked::TombStone => None,
        }
    }

    pub fn into_data(self) -> Option<D> {
        match self.t {
            Marked::Normal(data) => Some(data),
            Marked::TombStone => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use Ordering::Equal;
    use Ordering::Greater;
    use Ordering::Less;

    use crate::v001::SeqMarked;

    fn norm<D>(seq: u64, d: D) -> SeqMarked<D> {
        SeqMarked::new_normal(seq, d)
    }

    fn ts<D>(seq: u64) -> SeqMarked<D> {
        SeqMarked::new_tombstone(seq)
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

        Ok(())
    }
}
