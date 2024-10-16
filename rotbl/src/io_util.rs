use std::io;

use codeq::Span;

use crate::buf::new_uninitialized;

pub(crate) const DEFAULT_READ_BUF_SIZE: usize = 8 * 1024 * 1024;
pub(crate) const DEFAULT_WRITE_BUF_SIZE: usize = 64 * 1024 * 1024;

/// Read a segment of bytes from a seekable reader.
pub(crate) fn read_segment<R: io::Read + io::Seek>(
    mut r: R,
    segment: impl Span,
) -> Result<Vec<u8>, io::Error> {
    r.seek(io::SeekFrom::Start(segment.start().0))?;

    let mut buf = new_uninitialized(segment.size().0 as usize);
    r.read_exact(&mut buf)?;

    Ok(buf)
}
