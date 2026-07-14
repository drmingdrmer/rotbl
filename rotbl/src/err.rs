//! Crate-wide constructors for [`io::Error`] of a specific [`ErrorKind`].
//!
//! Shared by the block codecs and the manifest so the same one-line
//! `Error::new(kind, ...)` choreography is not re-spelled per module.

use std::io::Error;
use std::io::ErrorKind;

/// Wrap any error as an [`Error`] of [`InvalidData`](ErrorKind::InvalidData) kind.
pub(crate) fn invalid_data<E>(e: E) -> Error
where E: Into<Box<dyn std::error::Error + Send + Sync>> {
    Error::new(ErrorKind::InvalidData, e)
}

/// Wrap any error as an [`Error`] of [`InvalidInput`](ErrorKind::InvalidInput) kind.
pub(crate) fn invalid_input<E>(e: E) -> Error
where E: Into<Box<dyn std::error::Error + Send + Sync>> {
    Error::new(ErrorKind::InvalidInput, e)
}
