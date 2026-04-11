use std::io;

use codeq::Span;

use crate::buf::new_uninitialized;
use crate::storage::ReaderAt;

pub(crate) const DEFAULT_WRITE_BUF_SIZE: usize = 64 * 1024 * 1024;

/// Read a segment of bytes from a positional reader.
pub(crate) fn read_segment(r: &dyn ReaderAt, segment: impl Span) -> Result<Vec<u8>, io::Error> {
    let mut buf = new_uninitialized(segment.size().0 as usize);
    r.read_exact_at(&mut buf, segment.start().0)?;
    Ok(buf)
}
