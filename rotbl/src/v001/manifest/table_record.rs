use std::sync::Arc;

/// A complete, self-describing table entry: its id and inclusive `[smallest,
/// largest]` key extent.
///
/// `smallest` is an [`Arc<str>`] so the enclosing [`LevelManifest`] can key its
/// map by `Arc::clone(&record.smallest)` — the key and this field then share one
/// allocation instead of storing the smallest key twice.
///
/// [`LevelManifest`]: super::LevelManifest
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRecord {
    table_id: u32,
    smallest: Arc<str>,
    largest: String,
}

impl TableRecord {
    pub fn new(table_id: u32, smallest: impl Into<Arc<str>>, largest: impl Into<String>) -> Self {
        Self {
            table_id,
            smallest: smallest.into(),
            largest: largest.into(),
        }
    }

    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    pub fn smallest(&self) -> &str {
        &self.smallest
    }

    /// The `smallest` key as a cheap-to-clone shared handle, so the level map
    /// key and this field share one allocation.
    pub(super) fn smallest_arc(&self) -> &Arc<str> {
        &self.smallest
    }

    pub(super) fn largest(&self) -> &str {
        &self.largest
    }
}
