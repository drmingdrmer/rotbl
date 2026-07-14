use std::sync::Arc;

use super::TableRecord;

/// Derived table entry used by callers: a level plus the shared [`TableRecord`].
///
/// The record already carries `smallest`, so this holds nothing beyond `level`
/// and the `Arc` — constructing one from a stored record is a pointer clone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    level: u32,
    record: Arc<TableRecord>,
}

impl TableInfo {
    pub fn new(level: u32, record: Arc<TableRecord>) -> Self {
        Self { level, record }
    }

    pub fn level(&self) -> u32 {
        self.level
    }

    pub fn table_id(&self) -> u32 {
        self.record.table_id()
    }

    /// Inclusive `[smallest, largest]` key extent of this table.
    pub fn range(&self) -> (&str, &str) {
        (self.record.smallest(), self.record.largest())
    }

    pub(super) fn into_level_entry(self) -> (u32, Arc<TableRecord>) {
        (self.level, self.record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(table_id: u32, smallest: &str, largest: &str) -> Arc<TableRecord> {
        Arc::new(TableRecord::new(table_id, smallest, largest))
    }

    #[test]
    fn test_table_info_accessors() {
        let info = TableInfo::new(3, record(7, "aaa", "zzz"));
        assert_eq!(info.level(), 3);
        assert_eq!(info.table_id(), 7); // delegated to the record
        assert_eq!(info.range(), ("aaa", "zzz"));
    }

    /// `into_level_entry` hands back the level and the *same* record `Arc`
    /// (a pointer move, not a clone of the record).
    #[test]
    fn test_table_info_into_level_entry() {
        let rec = record(7, "aaa", "zzz");
        let info = TableInfo::new(3, Arc::clone(&rec));
        let (level, out) = info.into_level_entry();
        assert_eq!(level, 3);
        assert!(Arc::ptr_eq(&rec, &out));
    }
}
