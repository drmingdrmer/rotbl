use std::io::Error;
use std::io::Read;
use std::io::Write;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

use crate::codec::checksum_reader::ChecksumReader;
use crate::codec::checksum_writer::ChecksumWriter;
use crate::codec::fixed_size::FixedSize;
use crate::codec::Codec;
use crate::io_util;

/// Describe a segment with offset and size.
#[derive(Debug, Clone, Copy)]
#[derive(Default)]
#[derive(PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct Segment {
    /// Offset of the segment.
    pub offset: u64,

    /// Size of the segment.
    pub size: u64,
}

impl Segment {
    /// Create a new segment.
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }
}

impl io_util::Segment for Segment {
    fn offset(&self) -> u64 {
        self.offset
    }

    fn size(&self) -> u64 {
        self.size
    }
}

impl FixedSize for Segment {
    fn encoded_size() -> usize {
        // offset, size, checksum
        8 + 8 + 8
    }
}

impl Codec for Segment {
    fn encode<W: Write>(&self, mut w: W) -> Result<usize, Error> {
        let mut n = 0;

        let mut cw = ChecksumWriter::new(&mut w);

        cw.write_u64::<BigEndian>(self.offset)?;
        n += 8;

        cw.write_u64::<BigEndian>(self.size)?;
        n += 8;

        n += cw.write_checksum()?;

        Ok(n)
    }

    fn decode<R: Read>(mut r: R) -> Result<Self, Error> {
        let mut cr = ChecksumReader::new(&mut r);

        let offset = cr.read_u64::<BigEndian>()?;
        let size = cr.read_u64::<BigEndian>()?;

        cr.verify_checksum(|| "Segment::decode()")?;

        Ok(Self { offset, size })
    }
}

#[cfg(test)]
mod tests {
    use crate::v001::segment::Segment;
    use crate::v001::testing::test_codec;

    #[test]
    fn test_segment_codec() -> anyhow::Result<()> {
        let s = Segment {
            offset: 5,
            size: 10,
        };

        let b = vec![
            0, 0, 0, 0, 0, 0, 0, 5, // offset
            0, 0, 0, 0, 0, 0, 0, 10, // size
            0, 0, 0, 0, 70, 249, 231, 4, // checksum
        ];

        test_codec(&b, &s)?;

        Ok(())
    }
}
