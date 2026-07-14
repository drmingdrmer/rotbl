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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_record_accessors() {
        let rec = TableRecord::new(7, "aaa", "zzz");
        assert_eq!(rec.table_id(), 7);
        assert_eq!(rec.smallest(), "aaa");
        assert_eq!(rec.largest(), "zzz");
    }

    /// Passing an existing `Arc<str>` for `smallest` reuses that allocation (the
    /// level map keys off the same `Arc`) instead of re-allocating the string.
    #[test]
    fn test_table_record_reuses_smallest_arc() {
        let smallest: Arc<str> = Arc::from("aaa");
        let rec = TableRecord::new(1, Arc::clone(&smallest), "zzz");
        assert!(Arc::ptr_eq(&smallest, rec.smallest_arc()));
    }
}
