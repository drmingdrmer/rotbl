use std::mem::size_of;

/// Trait for types that can be encoded with a known, fixed size.
pub(crate) trait FixedSize {
    /// The size of the encoded data if Self has a fixed encoding size
    fn encoded_size() -> usize;
}

impl FixedSize for u64 {
    fn encoded_size() -> usize {
        size_of::<Self>()
    }
}

impl FixedSize for u32 {
    fn encoded_size() -> usize {
        size_of::<Self>()
    }
}
