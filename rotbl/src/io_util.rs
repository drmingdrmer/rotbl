use std::io;

use crate::buf::new_uninitialized;

pub(crate) const DEFAULT_READ_BUF_SIZE: usize = 8 * 1024 * 1024;
pub(crate) const DEFAULT_WRITE_BUF_SIZE: usize = 64 * 1024 * 1024;

pub(crate) trait Segment {
    fn offset(&self) -> u64;
    fn size(&self) -> u64;
}

impl<T> Segment for &T
where T: Segment
{
    fn offset(&self) -> u64 {
        (*self).offset()
    }

    fn size(&self) -> u64 {
        (*self).size()
    }
}

/// Read a segment of bytes from a seekable reader.
pub(crate) fn read_segment<R: io::Read + io::Seek>(
    mut r: R,
    segment: impl Segment,
) -> Result<Vec<u8>, io::Error> {
    r.seek(io::SeekFrom::Start(segment.offset()))?;

    let mut buf = new_uninitialized(segment.size() as usize);
    r.read_exact(&mut buf)?;

    Ok(buf)
}
