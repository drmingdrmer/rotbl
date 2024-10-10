use std::io::Error;
use std::io::Read;
use std::io::Write;

use codeq::Codec;
use codeq::FixedSize;

use crate::v001::segment::Segment;

#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
pub struct Footer {
    /// Offset and size of the block index.
    pub(crate) block_index_segment: Segment,

    /// Offset and size of the RotblMeta.
    pub(crate) meta_segment: Segment,

    /// Offset and size of the stat.
    pub(crate) stat_segment: Segment,
}

impl Footer {
    pub(crate) fn new(block_index: Segment, meta: Segment, stat: Segment) -> Self {
        Self {
            block_index_segment: block_index,
            meta_segment: meta,
            stat_segment: stat,
        }
    }
}

impl FixedSize for Footer {
    fn encoded_size() -> usize {
        // Block index, meta, stat
        Segment::encoded_size() * 3
    }
}

impl Codec for Footer {
    fn encode<W: Write>(&self, mut w: W) -> Result<usize, Error> {
        let mut n = 0;

        n += self.block_index_segment.encode(&mut w)?;
        n += self.meta_segment.encode(&mut w)?;
        n += self.stat_segment.encode(&mut w)?;

        Ok(n)
    }

    fn decode<R: Read>(mut r: R) -> Result<Self, Error> {
        let block_index = Segment::decode(&mut r)?;
        let meta = Segment::decode(&mut r)?;
        let stat = Segment::decode(&mut r)?;

        Ok(Self {
            block_index_segment: block_index,
            meta_segment: meta,
            stat_segment: stat,
        })
    }
}

#[cfg(test)]
mod tests {
    use codeq::testing::test_codec;

    use crate::v001::footer::Footer;
    use crate::v001::segment::Segment;

    #[test]
    fn test_footer_codec() -> anyhow::Result<()> {
        let f = Footer::new(
            Segment::new(5, 10),
            Segment::new(3, 4),
            Segment::new(15, 20),
        );

        let b = vec![
            0, 0, 0, 0, 0, 0, 0, 5, // offset
            0, 0, 0, 0, 0, 0, 0, 10, // size
            0, 0, 0, 0, 70, 249, 231, 4, // checksum
            0, 0, 0, 0, 0, 0, 0, 3, // offset
            0, 0, 0, 0, 0, 0, 0, 4, // size
            0, 0, 0, 0, 210, 91, 179, 137, // checksum
            0, 0, 0, 0, 0, 0, 0, 15, // offset
            0, 0, 0, 0, 0, 0, 0, 20, // size
            0, 0, 0, 0, 41, 216, 80, 249, // checksum
        ];

        test_codec(b.as_slice(), &f)?;

        Ok(())
    }
}
