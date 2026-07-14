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
