use std::io::Error;
use std::io::Read;
use std::io::Write;

use crate::codec::Codec;
use crate::v001::checksum_reader::ChecksumReader;
use crate::v001::checksum_writer::ChecksumWriter;

/// A encoding helper that appends a checksum to the end of the encoded data.
#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
pub struct WithChecksum<T> {
    pub(crate) data: T,
}

impl<T> WithChecksum<T> {
    pub fn new(data: T) -> Self {
        Self { data }
    }

    pub fn into_inner(self) -> T {
        self.data
    }
}

impl<T> Codec for WithChecksum<T>
where T: Codec
{
    const ENCODED_SIZE: u64 = T::ENCODED_SIZE + 8;

    fn encode<W: Write>(&self, mut w: W) -> Result<usize, Error> {
        let mut n = 0;
        let mut cw = ChecksumWriter::new(&mut w);

        n += self.data.encode(&mut cw)?;
        n += cw.write_checksum()?;

        Ok(n)
    }

    fn decode<R: Read>(r: R) -> Result<Self, Error> {
        let mut cr = ChecksumReader::new(r);

        let data = T::decode(&mut cr)?;
        cr.verify_checksum()?;

        let meta = Self { data };

        Ok(meta)
    }
}

#[cfg(test)]
mod tests {
    use crate::codec::Codec;
    use crate::v001::testing::test_codec;
    use crate::v001::with_checksum::WithChecksum;

    #[test]
    fn test_with_checksum_codec() -> anyhow::Result<()> {
        let wc = WithChecksum::<u64>::new(5);
        let mut b = Vec::new();
        let n = wc.encode(&mut b)?;
        assert_eq!(n, b.len());

        assert_eq!(
            vec![
                0, 0, 0, 0, 0, 0, 0, 5, // data
                0, 0, 0, 0, 21, 72, 43, 230, // checksum
            ],
            b
        );

        test_codec(b.as_slice(), &wc)?;

        Ok(())
    }
}
