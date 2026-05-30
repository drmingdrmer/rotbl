use std::collections::BTreeMap;
use std::ops::Bound;
use std::ops::RangeBounds;

use crate::v001::SegmentedKey;
use crate::v001::SeqMarked;

/// Which end of a range a bound belongs to.
enum Side {
    Start,
    End,
}

/// The common key prefix shared by every entry in a block.
///
/// A block stores each key by suffix; `Prefix` owns the shared head plus all the
/// arithmetic for stripping it from full keys, translating query bounds into
/// suffix space, and re-attaching it as a [`SegmentedKey`]. Keeping this in one
/// place means the "keys are stored without their common head" invariant lives in
/// a single type instead of being spread across the block's methods.
#[derive(Debug)]
#[derive(Clone, Default)]
#[derive(PartialEq, Eq)]
pub(crate) struct Prefix {
    inner: String,
}

impl Prefix {
    pub fn new(inner: String) -> Self {
        Self { inner }
    }

    /// Extract the common prefix from a full-key map, returning it together with
    /// the same entries re-keyed by suffix.
    ///
    /// For a sorted map the longest common prefix of all keys equals the common
    /// prefix of the first and last key, so this is O(prefix length), not O(keys).
    pub fn extract(data: BTreeMap<String, SeqMarked>) -> (Self, BTreeMap<String, SeqMarked>) {
        let prefix = match (data.keys().next(), data.keys().next_back()) {
            (Some(first), Some(last)) => {
                Self::new(first[..common_prefix_len(first, last)].to_string())
            }
            _ => Self::default(),
        };

        let plen = prefix.as_str().len();
        if plen == 0 {
            return (prefix, data);
        }

        let data = data.into_iter().map(|(k, v)| (k[plen..].to_string(), v)).collect();
        (prefix, data)
    }

    pub fn as_str(&self) -> &str {
        &self.inner
    }

    /// Strip the prefix from a full key, returning its suffix, or `None` if the
    /// key does not start with the prefix (and so cannot be stored in the block).
    pub fn strip<'a>(&self, key: &'a str) -> Option<&'a str> {
        key.strip_prefix(self.inner.as_str())
    }

    /// View a stored suffix as a full key without materializing it.
    pub fn segment<'a>(&'a self, suffix: &'a str) -> SegmentedKey<'a> {
        SegmentedKey::new(&self.inner, suffix)
    }

    /// Translate a full-key range into this block's suffix space, ready to index
    /// the suffix-keyed map directly.
    ///
    /// Returns `None` when the range cannot overlap this block at all — its start
    /// sorts after every key, or its end before every key — so the caller can
    /// skip the block instead of querying it.
    pub fn suffix_range<R>(&self, range: &R) -> Option<(Bound<String>, Bound<String>)>
    where R: RangeBounds<String> {
        let start = self.suffix_bound(range.start_bound(), Side::Start)?;
        let end = self.suffix_bound(range.end_bound(), Side::End)?;
        Some((start, end))
    }

    /// Map a single full-key range bound into the block's suffix space.
    ///
    /// Returns `None` when the bound alone forces the range empty — e.g. a start
    /// bound that sorts after every key, or an end bound that sorts before every
    /// key, given that all keys share the prefix.
    fn suffix_bound(&self, bound: Bound<&String>, side: Side) -> Option<Bound<String>> {
        let (key, inclusive) = match bound {
            Bound::Unbounded => return Some(Bound::Unbounded),
            Bound::Included(k) => (k.as_str(), true),
            Bound::Excluded(k) => (k.as_str(), false),
        };

        if let Some(suffix) = self.strip(key) {
            let suffix = suffix.to_string();
            return Some(if inclusive {
                Bound::Included(suffix)
            } else {
                Bound::Excluded(suffix)
            });
        }

        // `key` does not share the prefix, so it bounds either all keys or none,
        // depending on which side of the prefix it falls and which end it is.
        match (key < self.inner.as_str(), side) {
            (true, Side::Start) => Some(Bound::Unbounded), // before all keys
            (true, Side::End) => None,                     // ends before all keys
            (false, Side::Start) => None,                  // starts after all keys
            (false, Side::End) => Some(Bound::Unbounded),  // after all keys
        }
    }
}

/// Longest common prefix length of `a` and `b` in bytes, floored to a char
/// boundary so slicing at the returned length always yields valid UTF-8.
///
/// The return is a byte length (what `str` slicing needs), but the comparison is
/// per-`char`, so it never stops inside a multibyte character.
fn common_prefix_len(a: &str, b: &str) -> usize {
    let mut n = 0;
    for ((idx, ca), cb) in a.char_indices().zip(b.chars()) {
        if ca == cb {
            n = idx + ca.len_utf8();
        } else {
            break;
        }
    }
    n
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ops::Bound;

    use pretty_assertions::assert_eq;

    use super::common_prefix_len;
    use super::Prefix;
    use super::Side;
    use crate::v001::testing::bb;
    use crate::v001::testing::norm;
    use crate::v001::testing::ss;
    use crate::v001::testing::ts;
    use crate::v001::SeqMarked;

    /// Call `suffix_bound` with an owned key, so the borrowed `Bound` it takes
    /// stays alive for the duration of the call.
    fn sb(prefix: &Prefix, key: &str, inclusive: bool, side: Side) -> Option<Bound<String>> {
        let k = key.to_string();
        let bound = if inclusive {
            Bound::Included(&k)
        } else {
            Bound::Excluded(&k)
        };
        prefix.suffix_bound(bound, side)
    }

    #[test]
    fn test_new_and_as_str() {
        assert_eq!(Prefix::new(ss("exp-/")).as_str(), "exp-/");
        assert_eq!(Prefix::default().as_str(), "");
    }

    #[test]
    fn test_extract_common_prefix() {
        let data = maplit::btreemap! {
            ss("exp-/0001") => norm(1, bb("A")),
            ss("exp-/0005") => norm(5, bb("E")),
            ss("exp-/0009") => norm(9, bb("I")),
        };
        let (prefix, suffixed) = Prefix::extract(data);
        assert_eq!(prefix.as_str(), "exp-/000");
        assert_eq!(suffixed, maplit::btreemap! {
            ss("1") => norm(1, bb("A")),
            ss("5") => norm(5, bb("E")),
            ss("9") => norm(9, bb("I")),
        });
    }

    #[test]
    fn test_extract_no_common_prefix_returns_map_unchanged() {
        let data = maplit::btreemap! {
            ss("a") => ts(1),
            ss("b") => norm(2, bb("B")),
            ss("c") => norm(3, bb("C")),
        };
        let (prefix, suffixed) = Prefix::extract(data.clone());
        assert_eq!(prefix.as_str(), "");
        assert_eq!(suffixed, data);
    }

    #[test]
    fn test_extract_empty_map() {
        let (prefix, suffixed) = Prefix::extract(BTreeMap::new());
        assert_eq!(prefix.as_str(), "");
        assert_eq!(suffixed, BTreeMap::<String, SeqMarked>::new());
    }

    #[test]
    fn test_extract_single_key_becomes_whole_prefix() {
        // first == last, so the entire key is the common prefix and the stored
        // suffix is empty.
        let data = maplit::btreemap! {
            ss("hello") => norm(1, bb("V")),
        };
        let (prefix, suffixed) = Prefix::extract(data);
        assert_eq!(prefix.as_str(), "hello");
        assert_eq!(suffixed, maplit::btreemap! {
            ss("") => norm(1, bb("V")),
        });
    }

    #[test]
    fn test_extract_floors_prefix_to_char_boundary() {
        // The shared head is "ké" (k=1B, é=2B); the keys differ at the byte after,
        // so the prefix is 3 bytes and slicing never lands inside "é".
        let data = maplit::btreemap! {
            ss("ké1") => norm(1, bb("A")),
            ss("ké2") => norm(2, bb("B")),
        };
        let (prefix, suffixed) = Prefix::extract(data);
        assert_eq!(prefix.as_str(), "ké");
        assert_eq!(suffixed, maplit::btreemap! {
            ss("1") => norm(1, bb("A")),
            ss("2") => norm(2, bb("B")),
        });
    }

    #[test]
    fn test_extract_no_prefix_when_only_a_byte_is_shared() {
        // "è"(C3 A8) and "é"(C3 A9) share their first byte but no whole char, so
        // extract must report no prefix rather than slice mid-character.
        let data = maplit::btreemap! {
            ss("è") => norm(1, bb("A")),
            ss("é") => norm(2, bb("B")),
        };
        let (prefix, suffixed) = Prefix::extract(data.clone());
        assert_eq!(prefix.as_str(), "");
        assert_eq!(suffixed, data);
    }

    #[test]
    fn test_strip() {
        let prefix = Prefix::new(ss("exp-/"));
        assert_eq!(prefix.strip("exp-/0001"), Some("0001"));
        assert_eq!(prefix.strip("exp-/"), Some("")); // key equals the prefix
        assert_eq!(prefix.strip("exp"), None); // shorter than the prefix
        assert_eq!(prefix.strip("abc"), None); // sorts before the prefix
        assert_eq!(prefix.strip("zzz"), None); // sorts after the prefix
    }

    #[test]
    fn test_strip_empty_prefix_matches_everything() {
        let prefix = Prefix::default();
        assert_eq!(prefix.strip("anything"), Some("anything"));
        assert_eq!(prefix.strip(""), Some(""));
    }

    #[test]
    fn test_segment_reattaches_prefix() {
        let prefix = Prefix::new(ss("exp-/000"));
        let key = prefix.segment("5");
        assert_eq!(key.prefix(), "exp-/000");
        assert_eq!(key.suffix(), "5");
        assert_eq!(key.to_string(), "exp-/0005");
    }

    #[test]
    fn test_suffix_bound_unbounded_passes_through() {
        let prefix = Prefix::new(ss("exp-/000"));
        assert_eq!(
            prefix.suffix_bound(Bound::Unbounded, Side::Start),
            Some(Bound::Unbounded)
        );
        assert_eq!(
            prefix.suffix_bound(Bound::Unbounded, Side::End),
            Some(Bound::Unbounded)
        );
    }

    #[test]
    fn test_suffix_bound_key_within_prefix_maps_to_suffix() {
        let prefix = Prefix::new(ss("exp-/000"));
        // The inclusive/exclusive flag is preserved across the translation.
        assert_eq!(
            sb(&prefix, "exp-/0005", true, Side::Start),
            Some(Bound::Included(ss("5")))
        );
        assert_eq!(
            sb(&prefix, "exp-/0005", false, Side::End),
            Some(Bound::Excluded(ss("5")))
        );
        // A key equal to the prefix strips to an empty suffix.
        assert_eq!(
            sb(&prefix, "exp-/000", true, Side::Start),
            Some(Bound::Included(ss("")))
        );
    }

    #[test]
    fn test_suffix_bound_key_below_prefix() {
        let prefix = Prefix::new(ss("exp-/000"));
        // A start below every key keeps the range open; an end below every key
        // empties it.
        assert_eq!(
            sb(&prefix, "aaa", true, Side::Start),
            Some(Bound::Unbounded)
        );
        assert_eq!(sb(&prefix, "aaa", true, Side::End), None);
    }

    #[test]
    fn test_suffix_bound_key_above_prefix() {
        let prefix = Prefix::new(ss("exp-/000"));
        // A start above every key empties the range; an end above every key keeps
        // it open. The inclusive flag is irrelevant outside the prefix.
        assert_eq!(sb(&prefix, "zzz", true, Side::Start), None);
        assert_eq!(sb(&prefix, "zzz", false, Side::End), Some(Bound::Unbounded));
    }

    #[test]
    fn test_suffix_bound_empty_prefix_passes_keys_through() {
        let prefix = Prefix::default();
        assert_eq!(
            sb(&prefix, "abc", true, Side::Start),
            Some(Bound::Included(ss("abc")))
        );
        assert_eq!(
            sb(&prefix, "abc", false, Side::End),
            Some(Bound::Excluded(ss("abc")))
        );
    }

    #[test]
    fn test_suffix_range_within_prefix() {
        let prefix = Prefix::new(ss("exp-/000"));
        let r = ss("exp-/0003")..ss("exp-/0007");
        assert_eq!(
            prefix.suffix_range(&r),
            Some((Bound::Included(ss("3")), Bound::Excluded(ss("7"))))
        );
    }

    #[test]
    fn test_suffix_range_unbounded() {
        let prefix = Prefix::new(ss("exp-/000"));
        assert_eq!(
            prefix.suffix_range(&(..)),
            Some((Bound::Unbounded, Bound::Unbounded))
        );
    }

    #[test]
    fn test_suffix_range_straddling_bounds_open_to_full_block() {
        let prefix = Prefix::new(ss("exp-/000"));
        // start below every key and end above every key → the whole block.
        let r = ss("aaa")..ss("zzz");
        assert_eq!(
            prefix.suffix_range(&r),
            Some((Bound::Unbounded, Bound::Unbounded))
        );
    }

    #[test]
    fn test_suffix_range_empty_when_start_above_prefix() {
        let prefix = Prefix::new(ss("exp-/000"));
        let r = ss("zzz")..; // start sorts after every key
        assert_eq!(prefix.suffix_range(&r), None);
    }

    #[test]
    fn test_suffix_range_empty_when_end_below_prefix() {
        let prefix = Prefix::new(ss("exp-/000"));
        let r = ..ss("aaa"); // end sorts before every key
        assert_eq!(prefix.suffix_range(&r), None);
    }

    #[test]
    fn test_common_prefix_len_ascii() {
        assert_eq!(common_prefix_len("exp-/0001", "exp-/0009"), 8); // "exp-/000"
        assert_eq!(common_prefix_len("abc", "abc"), 3);
        assert_eq!(common_prefix_len("a", "b"), 0);
        assert_eq!(common_prefix_len("", "anything"), 0);
    }

    #[test]
    fn test_common_prefix_len_respects_char_boundaries() {
        // "é"(C3 A9) and "è"(C3 A8) share the first byte but not the first char.
        // A byte-wise loop would return 1 — a non-char-boundary that panics on
        // slicing. The char-wise loop returns 0.
        assert_eq!(common_prefix_len("é", "è"), 0);
        // A fully shared multibyte char counts its whole byte length.
        assert_eq!(common_prefix_len("aé1", "aé2"), 3); // "a"(1) + "é"(2)
    }
}
