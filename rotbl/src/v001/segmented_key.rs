use std::fmt;

/// A key view assembled from a borrowed common `prefix` and a varying `suffix`.
///
/// When an iterator walks entries that share a common prefix, it can keep the
/// prefix once and yield this lightweight view instead of materializing a
/// contiguous `String` for every entry. The full key is `prefix` immediately
/// followed by `suffix`.
///
/// The view cannot implement `Deref<Target = str>` or `AsRef<str>` because a
/// `str` must be contiguous in memory. Callers that need an owned, contiguous
/// key use [`ToString::to_string`] (via [`Display`](std::fmt::Display)); callers
/// that can consume the segments use [`prefix`](Self::prefix) and
/// [`suffix`](Self::suffix).
#[derive(Clone, Copy)]
#[derive(Debug)]
pub struct SegmentedKey<'a> {
    prefix: &'a str,
    suffix: &'a str,
}

impl<'a> SegmentedKey<'a> {
    pub fn new(prefix: &'a str, suffix: &'a str) -> Self {
        Self { prefix, suffix }
    }

    pub fn prefix(&self) -> &'a str {
        self.prefix
    }

    pub fn suffix(&self) -> &'a str {
        self.suffix
    }

    /// The byte length of the full key (`prefix.len() + suffix.len()`).
    pub fn len(&self) -> usize {
        self.prefix.len() + self.suffix.len()
    }

    pub fn is_empty(&self) -> bool {
        self.prefix.is_empty() && self.suffix.is_empty()
    }
}

impl fmt::Display for SegmentedKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.prefix)?;
        f.write_str(self.suffix)
    }
}

#[cfg(test)]
mod tests {
    use super::SegmentedKey;

    #[test]
    fn test_new_and_accessors() {
        let k = SegmentedKey::new("exp-/0001", "234");
        assert_eq!(k.prefix(), "exp-/0001");
        assert_eq!(k.suffix(), "234");
    }

    #[test]
    fn test_display_concatenates_prefix_and_suffix() {
        let k = SegmentedKey::new("exp-/0001", "234");
        assert_eq!(k.to_string(), "exp-/0001234");
    }

    #[test]
    fn test_len_is_total_of_both_segments() {
        let k = SegmentedKey::new("ab", "cde");
        assert_eq!(k.len(), 5);
        assert!(!k.is_empty());
    }

    #[test]
    fn test_empty_segments() {
        let k = SegmentedKey::new("", "");
        assert_eq!(k.to_string(), "");
        assert_eq!(k.len(), 0);
        assert!(k.is_empty());
    }
}
