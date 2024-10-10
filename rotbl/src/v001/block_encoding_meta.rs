use std::io::Error;
use std::io::Read;
use std::io::Write;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

use crate::codec::Codec;
use crate::v001::checksum_reader::ChecksumReader;
use crate::v001::checksum_writer::ChecksumWriter;

/// The meta data of a encoded block
#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
pub struct BlockEncodingMeta {
    /// The block number in a table, starting from 0.
    block_num: u32,

    /// The size of the encoded `data` part of a block.
    pub(crate) data_encoded_size: u64,
}

impl BlockEncodingMeta {
    pub fn new(block_num: u32, data_encoded_size: u64) -> Self {
        Self {
            block_num,
            data_encoded_size,
        }
    }

    pub fn block_num(&self) -> u32 {
        self.block_num
    }

    pub fn data_encoded_size(&self) -> u64 {
        self.data_encoded_size
    }
}

impl Codec for BlockEncodingMeta {
    fn encode<W: Write>(&self, mut w: W) -> Result<usize, Error> {
        let mut n = 0;
        let mut cw = ChecksumWriter::new(&mut w);

        cw.write_u64::<BigEndian>(self.block_num as u64)?;
        n += 8;
        cw.write_u64::<BigEndian>(self.data_encoded_size)?;
        n += 8;
        n += cw.write_checksum()?;

        Ok(n)
    }

    fn decode<R: Read>(r: R) -> Result<Self, Error> {
        let mut cr = ChecksumReader::new(r);

        let block_num = cr.read_u64::<BigEndian>()? as u32;
        let data_encoded_size = cr.read_u64::<BigEndian>()?;
        cr.verify_checksum(|| "BLockEncodingMeta::decode()")?;

        let meta = Self::new(block_num, data_encoded_size);

        Ok(meta)
    }
}

#[cfg(test)]
mod tests {
    use crate::v001::block_encoding_meta::BlockEncodingMeta;
    use crate::v001::testing::test_codec;

    #[test]
    fn test_block_meta_codec() -> anyhow::Result<()> {
        let meta = BlockEncodingMeta::new(1, 2);

        let encoded = vec![
            0, 0, 0, 0, 0, 0, 0, 1, // block_num
            0, 0, 0, 0, 0, 0, 0, 2, // data_encoded_size
            0, 0, 0, 0, 21, 206, 62, 58, // checksum
        ];

        test_codec(encoded.as_slice(), &meta)?;

        Ok(())
    }
}
