use std::io::Error;
use std::io::Read;
use std::io::Write;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

use crate::codec::Codec;
use crate::v001::checksum_reader::ChecksumReader;
use crate::v001::checksum_writer::ChecksumWriter;

#[derive(Debug)]
#[derive(Clone, Copy)]
#[derive(PartialEq, Eq)]
pub struct Footer {
    pub(crate) block_index_offset: u64,
}

impl Footer {
    pub fn new(chunk_index_offset: u64) -> Self {
        Self {
            block_index_offset: chunk_index_offset,
        }
    }
}

impl Codec for Footer {
    const ENCODED_SIZE: u64 = 8 + 8;

    fn encode<W: Write>(&self, mut w: W) -> Result<usize, Error> {
        let mut n = 0;

        let mut cw = ChecksumWriter::new(&mut w);
        cw.write_u64::<BigEndian>(self.block_index_offset)?;
        n += 8;
        n += cw.write_checksum()?;

        Ok(n)
    }

    fn decode<R: Read>(mut r: R) -> Result<Self, Error> {
        let mut cr = ChecksumReader::new(&mut r);
        let chunk_index_offset = cr.read_u64::<BigEndian>()?;
        cr.verify_checksum()?;

        Ok(Self {
            block_index_offset: chunk_index_offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::v001::footer::Footer;
    use crate::v001::testing::test_codec;

    #[test]
    fn test_footer_codec() -> anyhow::Result<()> {
        let f = Footer::new(5);

        let b = vec![
            0, 0, 0, 0, 0, 0, 0, 5, //
            0, 0, 0, 0, 21, 72, 43, 230, // checksum
        ];

        test_codec(b.as_slice(), &f)?;

        Ok(())
    }
}
