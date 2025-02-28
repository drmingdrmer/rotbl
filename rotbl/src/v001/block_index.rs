use std::fmt;
use std::fmt::Debug;
use std::io;
use std::io::Read;
use std::io::Write;
use std::ops::Bound;
use std::ops::RangeBounds;

use codeq::config::CodeqConfig;
use codeq::Span;

use crate::buf::new_uninitialized;
use crate::typ::Type;
use crate::v001::header::Header;
use crate::v001::types::Checksum;
use crate::v001::types::Segment;
use crate::v001::types::WithChecksum;
use crate::version::Version;

/// The meta data of a block, which is also an index entry in the block index.
#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct BlockIndexEntry {
    pub(crate) block_num: u32,

    /// Offset in the rotbl, starting from 0.
    pub(crate) offset: u64,
    pub(crate) size: u64,

    pub(crate) first_key: String,
    pub(crate) last_key: String,
}

impl BlockIndexEntry {
    /// Create a new block index entry.
    ///
    /// # Arguments
    ///
    /// * `block_num` - The number of the block.
    /// * `segment` - The offset and size of the block in the rotbl.
    /// * `first_key` - The first key in the block.
    /// * `last_key` - The last key in the block.
    pub fn new(block_num: u32, segment: Segment, first_key: String, last_key: String) -> Self {
        let offset = *segment.offset();
        let size = *segment.size();
        Self {
            block_num,
            offset,
            size,
            first_key,
            last_key,
        }
    }
}

impl fmt::Display for BlockIndexEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ block_num: {:>04}, position: {}+{}, key_range: [\"{}\", \"{}\"] }}",
            self.block_num, self.offset, self.size, self.first_key, self.last_key
        )
    }
}

/// The block index is a BTreeMap of key to block index entry.
///
/// Encoded data layout:
/// ```text
/// | Header
/// | Data encoded size
/// | Data
/// | Checksum
/// ```
#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
pub struct BlockIndex {
    pub(crate) header: Header,

    /// The size of the encoded `data` part.
    pub(crate) data_encoded_size: u64,

    pub(crate) data: Vec<BlockIndexEntry>,
}

impl BlockIndex {
    pub fn new(data: Vec<BlockIndexEntry>) -> Self {
        Self {
            header: Header::new(Type::BlockIndex, Version::V001),
            data_encoded_size: 0,
            data,
        }
    }

    pub fn with_encoded_size(mut self, size: u64) -> Self {
        self.data_encoded_size = size;
        self
    }

    pub fn iter_index_entries(&self) -> impl Iterator<Item = &BlockIndexEntry> {
        self.data.iter()
    }

    /// Returns block index entries that overlap with the given range.
    pub fn lookup_range<R>(&self, range: R) -> &[BlockIndexEntry]
    where R: RangeBounds<String> {
        // Just a helper function to make the code below more readable.
        fn contains(range: &(Bound<&String>, Bound<&String>), s: &String) -> bool {
            <(Bound<&String>, Bound<&String>) as RangeBounds<String>>::contains(range, s)
        }

        let left_to_inf = (range.start_bound(), Bound::<&String>::Unbounded);
        let start = self.data.partition_point(|ent| !contains(&left_to_inf, &ent.last_key));

        let inf_to_right = (Bound::<&String>::Unbounded, range.end_bound());
        let end = self.data.partition_point(|ent| contains(&inf_to_right, &ent.first_key));

        &self.data[start..end]
    }

    /// Return a block index entry that contains the given key.
    pub fn lookup(&self, key: &str) -> Option<&BlockIndexEntry> {
        let i = self.data.partition_point(|ent| key > ent.last_key.as_str());
        let ent = self.data.get(i)?;
        if key >= ent.first_key.as_str() {
            Some(ent)
        } else {
            None
        }
    }

    pub fn get_index_entry_by_num(&self, block_num: u32) -> Option<&BlockIndexEntry> {
        self.data.get(block_num as usize)
    }
}

impl codeq::Encode for BlockIndex {
    fn encode<W: Write>(&self, mut w: W) -> Result<usize, io::Error> {
        let mut n = 0usize;

        let encoded_data = serde_json::to_vec(&self.data)?;
        let encoded_size = encoded_data.len() as u64;

        let mut cw = Checksum::new_writer(&mut w);

        n += self.header.encode(&mut cw)?;

        n += Checksum::wrap(encoded_size).encode(&mut cw)?;

        cw.write_all(&encoded_data)?;
        n += encoded_size as usize;

        n += cw.write_checksum()?;

        Ok(n)
    }
}

impl codeq::Decode for BlockIndex {
    fn decode<R: Read>(r: R) -> Result<Self, io::Error> {
        let mut cr = Checksum::new_reader(r);

        let header = Header::decode(&mut cr)?;
        assert_eq!(header, Header::new(Type::BlockIndex, Version::V001));

        let encoded_size = WithChecksum::<u64>::decode(&mut cr)?.into_inner();

        let mut buf = new_uninitialized(encoded_size as usize);
        cr.read_exact(&mut buf)?;

        cr.verify_checksum(|| "BlockIndex::decode()")?;

        let data = serde_json::from_slice(&buf)?;

        let block = Self {
            header,
            data_encoded_size: encoded_size,
            data,
        };

        Ok(block)
    }
}

#[cfg(test)]
#[allow(clippy::redundant_clone)]
mod tests {
    use std::fmt::Debug;
    use std::ops::RangeBounds;

    use codeq::testing::test_codec;
    use codeq::Decode;
    use codeq::Encode;
    use pretty_assertions::assert_eq;

    use crate::v001::block_index::BlockIndex;
    use crate::v001::block_index::BlockIndexEntry;
    use crate::v001::testing::bbs;
    use crate::v001::testing::ss;
    use crate::v001::testing::vec_chain;

    #[test]
    fn test_block_index_get_block_by_num() -> anyhow::Result<()> {
        let block_index = create_testing_block_index();

        assert_eq!(block_index.get_index_entry_by_num(0).unwrap().block_num, 0);
        assert_eq!(block_index.get_index_entry_by_num(1).unwrap().block_num, 1);
        assert_eq!(block_index.get_index_entry_by_num(2), None);

        Ok(())
    }

    #[test]
    fn test_block_index_lookup_range() -> anyhow::Result<()> {
        fn to_block_nums(r: &[BlockIndexEntry]) -> Vec<u32> {
            r.iter().map(|ent| ent.block_num).collect()
        }

        fn lookup_range<R>(idx: &BlockIndex, r: R) -> Vec<u32>
        where R: RangeBounds<String> + Debug {
            dbg!(&r);
            let r = idx.lookup_range(r);
            to_block_nums(r)
        }

        let block_index = create_testing_block_index();

        let empty = Vec::<u32>::new();

        assert_eq!(vec![0, 1], lookup_range(&block_index, ..));

        assert_eq!(vec![0, 1], lookup_range(&block_index, ss("`")..));
        assert_eq!(vec![0, 1], lookup_range(&block_index, ss("a")..));
        assert_eq!(vec![0, 1], lookup_range(&block_index, ss("a1")..));
        assert_eq!(vec![0, 1], lookup_range(&block_index, ss("b")..));
        assert_eq!(vec![0, 1], lookup_range(&block_index, ss("p")..));
        assert_eq!(vec![1], lookup_range(&block_index, ss("p0")..));
        assert_eq!(vec![1], lookup_range(&block_index, ss("p1")..));
        assert_eq!(vec![1], lookup_range(&block_index, ss("p2")..));
        assert_eq!(vec![1], lookup_range(&block_index, ss("y")..));
        assert_eq!(vec![1], lookup_range(&block_index, ss("z")..));
        assert_eq!(empty, lookup_range(&block_index, ss("z1")..));

        assert_eq!(empty, lookup_range(&block_index, ..ss("a")));
        assert_eq!(vec![0], lookup_range(&block_index, ..=ss("a")));
        assert_eq!(vec![0], lookup_range(&block_index, ..ss("b")));
        assert_eq!(vec![0], lookup_range(&block_index, ..ss("b1")));
        assert_eq!(vec![0], lookup_range(&block_index, ..ss("p")));
        assert_eq!(vec![0], lookup_range(&block_index, ..ss("p0")));
        assert_eq!(vec![0], lookup_range(&block_index, ..ss("p1")));
        assert_eq!(vec![0, 1], lookup_range(&block_index, ..=ss("p1")));
        assert_eq!(vec![0, 1], lookup_range(&block_index, ..ss("p2")));
        assert_eq!(vec![0, 1], lookup_range(&block_index, ..ss("y")));
        assert_eq!(vec![0, 1], lookup_range(&block_index, ..ss("z")));
        assert_eq!(vec![0, 1], lookup_range(&block_index, ..ss("z1")));

        assert_eq!(empty, lookup_range(&block_index, ss("a")..ss("a")));
        assert_eq!(vec![0], lookup_range(&block_index, ss("p")..ss("p")));
        assert_eq!(empty, lookup_range(&block_index, ss("p1")..ss("p1")));
        assert_eq!(vec![0], lookup_range(&block_index, ss("p")..=ss("p")));
        assert_eq!(vec![0], lookup_range(&block_index, ss("p")..ss("p1")));
        assert_eq!(vec![0, 1], lookup_range(&block_index, ss("p")..ss("q")));
        assert_eq!(vec![0, 1], lookup_range(&block_index, ss("p")..ss("z2")));

        Ok(())
    }

    #[test]
    fn test_block_index_lookup() -> anyhow::Result<()> {
        fn lookup(idx: &BlockIndex, key: &str) -> Option<u32> {
            dbg!(&key);
            let r = idx.lookup(key);
            r.map(|x| x.block_num)
        }

        let bi = create_testing_block_index();

        assert_eq!(None, lookup(&bi, "`"));
        assert_eq!(Some(0), lookup(&bi, "a"));
        assert_eq!(Some(0), lookup(&bi, "a1"));
        assert_eq!(Some(0), lookup(&bi, "b"));
        assert_eq!(Some(0), lookup(&bi, "p"));
        assert_eq!(None, lookup(&bi, "p0"));
        assert_eq!(Some(1), lookup(&bi, "p1"));
        assert_eq!(Some(1), lookup(&bi, "y"));
        assert_eq!(Some(1), lookup(&bi, "z"));
        assert_eq!(None, lookup(&bi, "z1"));

        Ok(())
    }

    #[test]
    fn test_block_index_codec() -> anyhow::Result<()> {
        let ent1 = BlockIndexEntry {
            block_num: 0,
            offset: 2,
            size: 3,
            first_key: ss("a"),
            last_key: ss("p"),
        };

        let ent2 = BlockIndexEntry {
            block_num: 1,
            offset: 5,
            size: 6,
            first_key: ss("p1"),
            last_key: ss("z"),
        };

        let index_data = vec![ent1.clone(), ent2.clone()];
        let mut block_index = BlockIndex::new(index_data.clone());

        let mut b = Vec::new();
        let n = block_index.encode(&mut b)?;
        assert_eq!(n, b.len());

        println!("{}, {:?}", b.len(), b);

        let encoded_data = serde_json::to_string(&index_data)?;
        println!("encoded data: {} {}", encoded_data.len(), encoded_data);

        let encoded = vec_chain([
            vec![
                98, 108, 107, 95, 105, 100, 120, 0, // header.type
                0, 0, 0, 0, 0, 0, 0, 1, // header.version
                0, 0, 0, 0, 127, 225, 31, 239, // header checksum
                0, 0, 0, 0, 0, 0, 0, 136, // data_encoded_size
                0, 0, 0, 0, 134, 65, 212, 123, // data_encoded_size checksum
            ],
            bbs([
                r#"[{"block_num":0,"offset":2,"size":3,"first_key":"a","last_key":"p"},"#,
                r#"{"block_num":1,"offset":5,"size":6,"first_key":"p1","last_key":"z"}]"#,
            ]), // data
            vec![
                0, 0, 0, 0, 79, 146, 129, 41, // block_index checksum
            ],
        ]);

        assert_eq!(encoded, b);

        // Block does not know about the encoded size when it is created.
        block_index.data_encoded_size = encoded_data.len() as u64;

        let bi2 = BlockIndex::decode(&encoded[..])?;
        assert_eq!(block_index, bi2);

        test_codec(&b[..], &block_index)?;

        Ok(())
    }

    /// Build a index of `[a..=p, p1..=z]`
    fn create_testing_block_index() -> BlockIndex {
        let ent1 = BlockIndexEntry {
            block_num: 0,
            offset: 2,
            size: 3,
            first_key: ss("a"),
            last_key: ss("p"),
        };

        let ent2 = BlockIndexEntry {
            block_num: 1,
            offset: 5,
            size: 6,
            first_key: ss("p1"),
            last_key: ss("z"),
        };

        let index_data = vec![ent1.clone(), ent2.clone()];

        BlockIndex::new(index_data.clone())
    }
}
