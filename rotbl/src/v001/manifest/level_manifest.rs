use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::ops::Bound;
use std::sync::Arc;

use codeq::Decode;
use codeq::Encode;

use super::invalid_data;
use super::TableInfo;
use super::TableRecord;
use crate::typ::Type;
use crate::v001::header::Header;
use crate::version::Version;

/// All tables in one manifest level, keyed by each table's smallest key.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LevelManifest {
    /// Keyed by `Arc::clone(&record.smallest)`, so the key and the record's
    /// `smallest` field share one allocation.
    tables: BTreeMap<Arc<str>, Arc<TableRecord>>,
}

impl LevelManifest {
    pub fn version(&self) -> Version {
        Version::V001
    }

    /// The file header this level encodes at the beginning of its frame.
    pub fn header(&self) -> Header {
        Header::new(Type::LevelManifest, self.version())
    }

    pub fn tables(&self) -> &BTreeMap<Arc<str>, Arc<TableRecord>> {
        &self.tables
    }

    pub fn table_by_smallest(&self, level: u32, smallest: &str) -> Option<TableInfo> {
        let table = self.tables.get(smallest)?;
        Some(TableInfo::new(level, Arc::clone(table)))
    }

    /// The table in this level whose `[smallest, largest]` range covers `key`.
    ///
    /// At most one table matches, since ranges within a level are disjoint. This
    /// only routes a read to the single table that could hold `key`; the key is
    /// not necessarily stored there.
    pub fn table_for_key(&self, level: u32, key: &str) -> Option<TableInfo> {
        // Borrowed bound: probe with `key` itself, no owned copy per lookup.
        let below = (Bound::Unbounded, Bound::Included(key));
        let (_, table) = self.tables.range::<str, _>(below).next_back()?;
        if table.largest() < key {
            return None;
        }
        Some(TableInfo::new(level, Arc::clone(table)))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Insert one table, rejecting any range overlap with its neighbors.
    ///
    /// The checks are local (two `BTreeMap` lookups), so callers do not need a
    /// full manifest validation per mutation.
    pub(super) fn insert(&mut self, table: TableInfo) -> Result<(), io::Error> {
        let (level, record) = table.into_level_entry();
        let smallest = record.smallest();
        if smallest > record.largest() {
            return Err(invalid_data(format!(
                "table smallest key {} is greater than largest key {}",
                smallest,
                record.largest()
            )));
        }

        // The previous table must end before this one starts; this covers the
        // duplicate-smallest case as well.
        let below = (Bound::Unbounded, Bound::Included(smallest));
        if let Some((prev_smallest, prev)) = self.tables.range::<str, _>(below).next_back() {
            if prev.largest() >= smallest {
                return Err(overlapping(
                    level,
                    prev_smallest,
                    prev.largest(),
                    smallest,
                    record.largest(),
                ));
            }
        }
        // This table must end before the next one starts.
        let above = (Bound::Included(smallest), Bound::Unbounded);
        if let Some((next_smallest, next)) = self.tables.range::<str, _>(above).next() {
            if record.largest() >= next_smallest.as_ref() {
                return Err(overlapping(
                    level,
                    smallest,
                    record.largest(),
                    next_smallest,
                    next.largest(),
                ));
            }
        }
        let key = Arc::clone(record.smallest_arc());
        self.tables.insert(key, record);
        Ok(())
    }

    pub(super) fn table_by_id(&self, level: u32, table_id: u32) -> Option<TableInfo> {
        let (_, table) = self.tables.iter().find(|(_, table)| table.table_id() == table_id)?;
        Some(TableInfo::new(level, Arc::clone(table)))
    }

    pub(super) fn remove_table(&mut self, level: u32, table_id: u32) -> Option<TableInfo> {
        let key = self
            .tables
            .iter()
            .find(|(_, table)| table.table_id() == table_id)
            .map(|(smallest, _)| Arc::clone(smallest))?;
        let record = self.tables.remove(&key)?;
        Some(TableInfo::new(level, record))
    }

    pub(super) fn validate(
        &self,
        level: u32,
        next_table_id: u32,
        table_ids: &mut BTreeSet<u32>,
    ) -> Result<(), io::Error> {
        if self.tables.is_empty() {
            return Err(invalid_data(format!("level {} has no tables", level)));
        }

        let mut previous = None;
        for (smallest, table) in &self.tables {
            let smallest: &str = smallest;
            if smallest > table.largest() {
                return Err(invalid_data(format!(
                    "table smallest key {} is greater than largest key {}",
                    smallest,
                    table.largest()
                )));
            }
            if table.table_id() >= next_table_id {
                return Err(invalid_data(format!(
                    "table_id {} reaches next_table_id {}",
                    table.table_id(),
                    next_table_id
                )));
            }
            if !table_ids.insert(table.table_id()) {
                return Err(invalid_data(format!(
                    "duplicate table_id {}",
                    table.table_id()
                )));
            }
            if let Some((prev_smallest, prev_largest)) = previous {
                if prev_largest >= smallest {
                    return Err(invalid_data(format!(
                        "overlapping table ranges at level {}: [{}, {}] and [{}, {}]",
                        level,
                        prev_smallest,
                        prev_largest,
                        smallest,
                        table.largest()
                    )));
                }
            }
            previous = Some((smallest, table.largest()));
        }

        Ok(())
    }

    fn encode_payload<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        let mut n = 0;
        n += (self.tables.len() as u64).encode(&mut w)?;
        for table in self.tables.values() {
            n += table.smallest().encode(&mut w)?;
            n += table.largest().encode(&mut w)?;
            n += table.table_id().encode(&mut w)?;
        }
        Ok(n)
    }

    fn decode_payload<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
        let len = u64::decode(&mut r)?;
        let mut tables = BTreeMap::new();
        for _ in 0..len {
            let smallest = String::decode(&mut r)?;
            let largest = String::decode(&mut r)?;
            let table_id = u32::decode(&mut r)?;
            let record = Arc::new(TableRecord::new(table_id, smallest, largest));
            let key = Arc::clone(record.smallest_arc());
            if tables.insert(Arc::clone(&key), record).is_some() {
                return Err(invalid_data(format!(
                    "duplicate table smallest key {}",
                    key
                )));
            }
        }
        Ok(Self { tables })
    }
}

fn overlapping(
    level: u32,
    a_smallest: &str,
    a_largest: &str,
    b_smallest: &str,
    b_largest: &str,
) -> io::Error {
    invalid_data(format!(
        "overlapping table ranges at level {}: [{}, {}] and [{}, {}]",
        level, a_smallest, a_largest, b_smallest, b_largest
    ))
}

impl Encode for LevelManifest {
    /// Frame: `header + payload`. No inner checksum — this frame is nested inside
    /// the [`Levels`](super::Levels) zstd payload, itself inside the checksummed
    /// [`Manifest`](super::Manifest) frame, so any corruption is already caught
    /// by that outer checksum.
    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        let mut n = 0;
        n += self.header().encode(&mut w)?;
        n += self.encode_payload(&mut w)?;
        Ok(n)
    }
}

impl Decode for LevelManifest {
    fn decode<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
        let header = Header::decode(&mut r)?;
        if header != Header::new(Type::LevelManifest, Version::V001) {
            return Err(invalid_data(format!(
                "unsupported level manifest header {}",
                header
            )));
        }

        Self::decode_payload(&mut r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v001::testing::table_info as table;

    #[test]
    fn test_level_manifest_header() {
        assert_eq!(
            LevelManifest::default().header(),
            Header::new(Type::LevelManifest, Version::V001)
        );
    }

    #[test]
    fn test_insert_and_lookups() -> Result<(), io::Error> {
        let mut lm = LevelManifest::default();
        assert!(lm.is_empty());
        lm.insert(table(1, 0, "a", "c"))?;
        lm.insert(table(2, 0, "f", "h"))?;

        assert!(!lm.is_empty());
        assert_eq!(lm.tables().len(), 2);
        assert_eq!(lm.table_by_smallest(0, "a"), Some(table(1, 0, "a", "c")));
        assert_eq!(lm.table_by_smallest(0, "b"), None); // "b" is not a smallest key
        assert_eq!(lm.table_by_id(0, 2), Some(table(2, 0, "f", "h")));
        assert_eq!(lm.table_by_id(0, 99), None);
        Ok(())
    }

    #[test]
    fn test_insert_rejects_smallest_greater_than_largest() {
        let mut lm = LevelManifest::default();
        let err = lm.insert(table(1, 0, "z", "a")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("greater than largest"), "{err}");
    }

    /// Overlap is rejected regardless of insert order: the neighbor below *and*
    /// above the new smallest key are both checked. A shared endpoint counts as
    /// overlap; a clean gap does not.
    #[test]
    fn test_insert_detects_overlap_on_both_sides() -> Result<(), io::Error> {
        let mut lm = LevelManifest::default();
        lm.insert(table(1, 0, "f", "h"))?;

        // Ends at "f"; existing [f,h] is the upper neighbor sharing that endpoint.
        let err = lm.insert(table(2, 0, "a", "f")).unwrap_err();
        assert!(
            err.to_string().contains("overlapping table ranges"),
            "{err}"
        );
        // Starts at "h"; existing [f,h] is the lower neighbor sharing that endpoint.
        let err = lm.insert(table(3, 0, "h", "z")).unwrap_err();
        assert!(
            err.to_string().contains("overlapping table ranges"),
            "{err}"
        );

        // Clean gaps on either side are accepted, in either order.
        lm.insert(table(4, 0, "i", "z"))?; // gap "h" < "i"
        lm.insert(table(5, 0, "a", "e"))?; // gap "e" < "f"
        assert_eq!(lm.tables().len(), 3);
        Ok(())
    }

    /// Read routing: ranges within a level are disjoint, so at most one table
    /// covers a key. Probe inside, on both inclusive boundaries, in the gap,
    /// below the first table, and above the last.
    #[test]
    fn test_table_for_key_routes_within_disjoint_ranges() -> Result<(), io::Error> {
        let mut lm = LevelManifest::default();
        lm.insert(table(1, 0, "a", "c"))?;
        lm.insert(table(2, 0, "f", "h"))?;
        lm.insert(table(3, 0, "m", "p"))?;

        assert_eq!(lm.table_for_key(0, "b"), Some(table(1, 0, "a", "c")));
        assert_eq!(lm.table_for_key(0, "a"), Some(table(1, 0, "a", "c"))); // == smallest
        assert_eq!(lm.table_for_key(0, "c"), Some(table(1, 0, "a", "c"))); // == largest
        assert_eq!(lm.table_for_key(0, "n"), Some(table(3, 0, "m", "p")));

        assert_eq!(lm.table_for_key(0, "d"), None); // gap between "c" and "f"
        assert_eq!(lm.table_for_key(0, "0"), None); // below "a"
        assert_eq!(lm.table_for_key(0, "z"), None); // above "p"
        Ok(())
    }

    #[test]
    fn test_remove_table() -> Result<(), io::Error> {
        let mut lm = LevelManifest::default();
        lm.insert(table(1, 0, "a", "c"))?;
        lm.insert(table(2, 0, "f", "h"))?;

        assert_eq!(lm.remove_table(0, 1), Some(table(1, 0, "a", "c")));
        assert_eq!(lm.tables().len(), 1);
        assert!(!lm.is_empty());
        assert_eq!(lm.remove_table(0, 99), None); // missing id is a no-op
        assert_eq!(lm.remove_table(0, 2), Some(table(2, 0, "f", "h")));
        assert!(lm.is_empty());
        Ok(())
    }

    #[test]
    fn test_validate() -> Result<(), io::Error> {
        let mut lm = LevelManifest::default();
        lm.insert(table(1, 0, "a", "c"))?;
        lm.insert(table(2, 0, "f", "h"))?;

        let mut ids = BTreeSet::new();
        lm.validate(0, 3, &mut ids)?;
        assert_eq!(ids, BTreeSet::from([1, 2]));

        // Every table_id must be below next_table_id.
        let err = lm.validate(0, 2, &mut BTreeSet::new()).unwrap_err();
        assert!(err.to_string().contains("reaches next_table_id"), "{err}");

        // An empty level is invalid.
        let err = LevelManifest::default().validate(0, 1, &mut BTreeSet::new()).unwrap_err();
        assert!(err.to_string().contains("has no tables"), "{err}");
        Ok(())
    }

    /// The shared id set lets validate() catch a table_id already used on
    /// another level, even though ids are unique *within* one level.
    #[test]
    fn test_validate_detects_cross_level_duplicate_id() -> Result<(), io::Error> {
        let mut lm = LevelManifest::default();
        lm.insert(table(1, 0, "a", "c"))?;

        let mut ids = BTreeSet::from([1]); // id 1 already seen on another level
        let err = lm.validate(0, 2, &mut ids).unwrap_err();
        assert!(err.to_string().contains("duplicate table_id"), "{err}");
        Ok(())
    }

    #[test]
    fn test_round_trip() -> Result<(), io::Error> {
        let mut lm = LevelManifest::default();
        lm.insert(table(1, 0, "a", "c"))?;
        lm.insert(table(2, 0, "f", "h"))?;

        let mut encoded = Vec::new();
        lm.encode(&mut encoded)?;
        let decoded = LevelManifest::decode(encoded.as_slice())?;
        assert_eq!(lm, decoded);
        Ok(())
    }

    #[test]
    fn test_decode_rejects_wrong_header() {
        let mut bytes = Vec::new();
        Header::new(Type::Levels, Version::V001).encode(&mut bytes).unwrap();
        let err = LevelManifest::decode(bytes.as_slice()).unwrap_err();
        assert!(
            err.to_string().contains("unsupported level manifest header"),
            "{err}"
        );
    }

    #[test]
    fn test_decode_rejects_duplicate_smallest() -> Result<(), io::Error> {
        // Hand-build a payload with two entries keyed by the same smallest "a".
        let mut framed = Vec::new();
        Header::new(Type::LevelManifest, Version::V001).encode(&mut framed)?;
        2u64.encode(&mut framed)?; // table count
        for table_id in [1u32, 2] {
            "a".encode(&mut framed)?; // smallest (duplicated)
            "z".encode(&mut framed)?; // largest
            table_id.encode(&mut framed)?;
        }
        let err = LevelManifest::decode(framed.as_slice()).unwrap_err();
        assert!(
            err.to_string().contains("duplicate table smallest key"),
            "{err}"
        );
        Ok(())
    }
}
