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
