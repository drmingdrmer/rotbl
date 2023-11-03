use std::borrow::Borrow;
use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::io::Error;
use std::io::Read;
use std::io::Write;
use std::ops::RangeBounds;

use crate::buf;
use crate::codec::Codec;
use crate::typ::Type;
use crate::v001::block_encoding_meta::BlockEncodingMeta;
use crate::v001::checksum_reader::ChecksumReader;
use crate::v001::checksum_writer::ChecksumWriter;
use crate::v001::header::Header;
use crate::v001::SeqMarked;
use crate::version::Version;

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
    // Variable sized block.
    const ENCODED_SIZE: u64 = 0;

    fn encode<W: Write>(&self, mut w: W) -> Result<usize, Error> {
        let mut n = 0usize;
        let encoded_data = serde_json::to_vec(&self.data)?;
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
        cr.verify_checksum()?;

        let data = serde_json::from_slice(&buf)?;

        let block = Self { header, meta, data };

        Ok(block)
    }
}

#[cfg(test)]
#[allow(clippy::redundant_clone)]
mod tests {

    use pretty_assertions::assert_eq;

    use crate::codec::Codec;
    use crate::v001::block::Block;
    use crate::v001::testing::bb;
    use crate::v001::testing::bbs;
    use crate::v001::testing::ss;
    use crate::v001::testing::test_codec;
    use crate::v001::testing::vec_chain;
    use crate::v001::SeqMarked;

    #[test]
    fn test_block_codec() -> anyhow::Result<()> {
        let block_data = maplit::btreemap! {
            ss("a") => SeqMarked::new(1,false, bb("A")),
            ss("b") => SeqMarked::new(2,true, bb("B")),
        };
        let mut block = Block::new(5, block_data.clone());

        let mut b = Vec::new();
        let n = block.encode(&mut b)?;
        assert_eq!(n, b.len());

        let encoded_data = serde_json::to_string(&block_data)?;
        println!("encoded data: {} {}", encoded_data.len(), encoded_data);

        let encoded = vec_chain([
            vec![
                98, 108, 107, 0, 0, 0, 0, 0, // header.type
                0, 0, 0, 0, 0, 0, 0, 1, // header.version
                0, 0, 0, 0, 225, 115, 139, 228, // header checksum
                0, 0, 0, 0, 0, 0, 0, 5, // meta.block_num
                0, 0, 0, 0, 0, 0, 0, 65, // meta.data_encoded_size
                0, 0, 0, 0, //
                167, 247, 127, 28, // meta checksum
            ],
            bbs([r#"{"a":{"seq":1,"t":{"Normal":[65]}},"b":{"seq":2,"t":"TombStone"}}"#]), // data
            vec![
                0, 0, 0, 0, //
                44, 83, 156, 157, // block checksum
            ],
        ]);
        assert_eq!(encoded, b);

        // Block does not know about the encoded size when it is created.
        block.meta.data_encoded_size = encoded_data.len() as u64;

        test_codec(&b[..], &block)?;

        Ok(())
    }

    #[test]
    fn test_block_get_range() -> anyhow::Result<()> {
        let block_data = maplit::btreemap! {
            ss("a") => SeqMarked::new(1,false, bb("A")),
            ss("b") => SeqMarked::new(2,true, bb("B")),
            ss("c") => SeqMarked::new(3,true, bb("C")),
            ss("d") => SeqMarked::new(4,true, bb("D")),
        };
        let block = Block::new(5, block_data.clone());

        assert_eq!(None, block.get("z"));
        assert_eq!(Some(&SeqMarked::new(1, false, bb("A"))), block.get("a"));

        let got = block.range(ss("b")..ss("e")).collect::<Vec<_>>();
        assert_eq!(got, vec![
            (&ss("b"), &SeqMarked::new(2, true, bb("B"))),
            (&ss("c"), &SeqMarked::new(3, true, bb("C"))),
            (&ss("d"), &SeqMarked::new(4, true, bb("D"))),
        ]);

        Ok(())
    }
}
