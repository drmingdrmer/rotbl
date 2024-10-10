use std::borrow::Borrow;
use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::io::Error;
use std::io::Read;
use std::io::Write;
use std::ops::RangeBounds;

use crate::buf;
use crate::codec::checksum_reader::ChecksumReader;
use crate::codec::checksum_writer::ChecksumWriter;
use crate::codec::Codec;
use crate::typ::Type;
use crate::v001::bincode_config::bincode_config;
use crate::v001::block_encoding_meta::BlockEncodingMeta;
use crate::v001::header::Header;
use crate::v001::SeqMarked;
use crate::version::Version;

/// Iterator of key-values inside a block.
pub struct BlockIter<'a> {
    inner: Range<'a, String, SeqMarked>,
}

impl<'a> Iterator for BlockIter<'a> {
    type Item = (&'a String, &'a SeqMarked);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
pub struct Block {
    header: Header,

    meta: BlockEncodingMeta,

    data: BTreeMap<String, SeqMarked>,
}

impl Block {
    pub fn new(block_num: u32, data: BTreeMap<String, SeqMarked>) -> Self {
        let header = Header::new(Type::Block, Version::V001);
        let meta = BlockEncodingMeta::new(block_num, 0);
        Self { header, meta, data }
    }

    pub fn data_encoded_size(&self) -> u64 {
        self.meta.data_encoded_size()
    }

    pub fn get(&self, key: &str) -> Option<&SeqMarked> {
        self.data.get(key)
    }

    pub fn range<Q, R>(&self, range: R) -> BlockIter
    where
        R: RangeBounds<Q>,
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        BlockIter {
            inner: self.data.range(range),
        }
    }
}

impl Codec for Block {
    fn encode<W: Write>(&self, mut w: W) -> Result<usize, Error> {
        let mut n = 0usize;
        let encoded_data = bincode::encode_to_vec(&self.data, bincode_config())
            .map_err(|e| Error::new(std::io::ErrorKind::InvalidData, e))?;
        let encoded_size = encoded_data.len() as u64;

        let mut cw = ChecksumWriter::new(&mut w);

        n += self.header.encode(&mut cw)?;

        // Decide the size of the encoded data part.
        let meta = BlockEncodingMeta::new(self.meta.block_num(), encoded_size);
        n += meta.encode(&mut cw)?;

        cw.write_all(&encoded_data)?;
        n += encoded_size as usize;
        n += cw.write_checksum()?;

        Ok(n)
    }

    fn decode<R: Read>(r: R) -> Result<Self, Error> {
        let mut cr = ChecksumReader::new(r);

        let header = Header::decode(&mut cr)?;
        assert_eq!(header, Header::new(Type::Block, Version::V001));

        let meta = BlockEncodingMeta::decode(&mut cr)?;

        let data_size = meta.data_encoded_size() as usize;

        let mut buf = buf::new_uninitialized(data_size);
        cr.read_exact(&mut buf)?;
        cr.verify_checksum(|| "Block::decode()")?;

        let (data, _size) = bincode::decode_from_slice(&buf, bincode_config())
            .map_err(|e| Error::new(std::io::ErrorKind::InvalidData, e))?;

        let block = Self { header, meta, data };

        Ok(block)
    }
}

#[cfg(test)]
#[allow(clippy::redundant_clone)]
mod tests {

    use pretty_assertions::assert_eq;

    use crate::codec::Codec;
    use crate::v001::bincode_config::bincode_config;
    use crate::v001::block::Block;
    use crate::v001::testing::bb;
    use crate::v001::testing::ss;
    use crate::v001::testing::test_codec;
    use crate::v001::testing::vec_chain;
    use crate::v001::SeqMarked;

    #[test]
    fn test_block_codec() -> anyhow::Result<()> {
        let block_data = maplit::btreemap! {
            ss("a") => SeqMarked::new_normal(1, bb("A")),
            ss("b") => SeqMarked::new_tombstone(2),
        };
        let mut block = Block::new(5, block_data.clone());

        let mut b = Vec::new();
        let n = block.encode(&mut b)?;
        assert_eq!(n, b.len());

        // let encoded_data = serde_json::to_string(&block_data)?;
        let encoded_data = bincode::encode_to_vec(&block_data, bincode_config()).unwrap();
        println!("encoded data: {} {:?}", encoded_data.len(), encoded_data);

        let encoded = vec_chain([
            vec![
                98, 108, 107, 0, 0, 0, 0, 0, // header.type
                0, 0, 0, 0, 0, 0, 0, 1, // header.version
                0, 0, 0, 0, 225, 115, 139, 228, // header checksum
                0, 0, 0, 0, 0, 0, 0, 5, // meta.block_num
                0, 0, 0, 0, 0, 0, 0, 11, // meta.data_encoded_size
                0, 0, 0, 0, //
                49, 254, 215, 146, // meta checksum
            ],
            // block data:
            vec![
                2, // number of entries?
                //
                1, 97, // key1: "a"
                1,  // seq
                0,  // normal
                1, 65, // "A"
                //
                1, 98, // key2: "b"
                2,  // seq
                1,  // tombstone
            ],
            vec![
                0, 0, 0, 0, //
                155, 10, 213, 39, // block checksum
            ],
        ]);
        println!("encoded: {:?}", b);
        assert_eq!(encoded, b);

        // Block does not know about the encoded size when it is created.
        block.meta.data_encoded_size = encoded_data.len() as u64;

        test_codec(&b[..], &block)?;

        Ok(())
    }

    #[test]
    fn test_block_get_range() -> anyhow::Result<()> {
        let block_data = maplit::btreemap! {
            ss("a") => SeqMarked::new_tombstone(1),
            ss("b") => SeqMarked::new_normal(2, bb("B")),
            ss("c") => SeqMarked::new_normal(3, bb("C")),
            ss("d") => SeqMarked::new_normal(4, bb("D")),
        };
        let block = Block::new(5, block_data.clone());

        assert_eq!(None, block.get("z"));
        assert_eq!(Some(&SeqMarked::new_tombstone(1)), block.get("a"));

        let got = block.range(ss("b")..ss("e")).collect::<Vec<_>>();
        assert_eq!(got, vec![
            (&ss("b"), &SeqMarked::new_normal(2, bb("B"))),
            (&ss("c"), &SeqMarked::new_normal(3, bb("C"))),
            (&ss("d"), &SeqMarked::new_normal(4, bb("D"))),
        ]);

        Ok(())
    }
}
