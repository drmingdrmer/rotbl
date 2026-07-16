use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::io::Error;
use std::io::Read;
use std::io::Write;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;

use codeq::config::CodeqConfig;
use codeq::Decode;
use codeq::Encode;

use crate::typ::Type;
use crate::v001::block_encoding_meta::BlockEncodingMeta;
use crate::v001::block_v001::block_decode_v001;
use crate::v001::block_v002::block_decode_v002;
use crate::v001::block_v003::block_decode_v003;
use crate::v001::block_v003::block_encode_v003;
use crate::v001::block_v003::RowGroupDirectory;
use crate::v001::block_v003::DEFAULT_ROW_GROUP_MAX_ITEMS;
use crate::v001::header::Header;
use crate::v001::prefix::Prefix;
use crate::v001::types::Checksum;
use crate::v001::SegmentedKey;
use crate::v001::SeqMarked;
use crate::version::Version;

/// Wrap any error as an [`Error`] of [`InvalidData`](std::io::ErrorKind::InvalidData) kind.
///
/// Shared by the block decoders.
pub(crate) fn invalid<E>(e: E) -> Error
where E: Into<Box<dyn std::error::Error + Send + Sync>> {
    Error::new(std::io::ErrorKind::InvalidData, e)
}

/// Iterator of key-values inside a block.
///
/// Yields each key as a [`SegmentedKey`] that borrows the block's common
/// `prefix` plus the entry's stored suffix, so no full key is materialized.
pub struct BlockIter<'a> {
    prefix: &'a Prefix,
    inner: Range<'a, String, SeqMarked>,
}

impl<'a> Iterator for BlockIter<'a> {
    type Item = (SegmentedKey<'a>, &'a SeqMarked);

    fn next(&mut self) -> Option<Self::Item> {
        let (suffix, value) = self.inner.next()?;
        Some((self.prefix.segment(suffix), value))
    }
}

#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
pub struct Block {
    /// On-disk format header. An in-memory `Block` always reports V003: a block
    /// decoded from a V001 or V002 file is canonicalized to V003
    /// because the in-memory form is identical and new blocks are always written
    /// as V003. This field describes the in-memory representation, not necessarily
    /// the version the bytes were read from.
    pub(crate) header: Header,

    pub(crate) meta: BlockEncodingMeta,

    /// The common prefix shared by every key in `data`.
    pub(crate) prefix: Prefix,

    /// Entries keyed by suffix: a full key is `prefix` followed by its suffix.
    pub(crate) data: BTreeMap<String, SeqMarked>,
}

impl Block {
    pub fn new(block_num: u32, data: BTreeMap<String, SeqMarked>) -> Self {
        let header = Header::new(Type::Block, Version::V003);
        let meta = BlockEncodingMeta::new(block_num, 0);
        let (prefix, data) = Prefix::extract(data);
        Self {
            header,
            meta,
            prefix,
            data,
        }
    }

    pub fn data_encoded_size(&self) -> u64 {
        self.meta.data_encoded_size()
    }

    pub(crate) fn encode_with_row_group_max_items<W: Write>(
        &self,
        w: W,
        row_group_max_items: usize,
    ) -> Result<usize, Error> {
        block_encode_v003(self, w, row_group_max_items)
    }

    /// The common prefix shared by every key in this block.
    pub fn prefix(&self) -> &str {
        self.prefix.as_str()
    }

    pub fn get(&self, key: &str) -> Option<&SeqMarked> {
        let suffix = self.prefix.strip(key)?;
        self.data.get(suffix)
    }

    pub fn range<R>(&self, range: R) -> BlockIter
    where R: RangeBounds<String> {
        let inner = match self.prefix.suffix_range(&range) {
            Some((start, end)) => self.data.range((start, end)),
            // The requested range cannot overlap this block's prefix. Use an
            // empty `""..""` range (BTreeMap panics on an empty Excluded..Excluded).
            None => self.data.range((
                Bound::Included(String::new()),
                Bound::Excluded(String::new()),
            )),
        };

        BlockIter {
            prefix: &self.prefix,
            inner,
        }
    }
}

impl Encode for Block {
    fn encode<W: Write>(&self, w: W) -> Result<usize, Error> {
        self.encode_with_row_group_max_items(w, DEFAULT_ROW_GROUP_MAX_ITEMS)
    }
}

impl Decode for Block {
    fn decode<R: Read>(r: R) -> Result<Self, Error> {
        // Parse only the header here, to discover the version. Each version's
        // decoder takes over the rest of the stream (meta, payload, checksum);
        // the header is already folded into `cr`'s running checksum.
        let mut cr = Checksum::new_reader(r);
        let header = Header::decode(&mut cr)?;

        if header == Header::new(Type::Block, Version::V001) {
            block_decode_v001(cr)
        } else if header == Header::new(Type::Block, Version::V002) {
            block_decode_v002(cr)
        } else if header == Header::new(Type::Block, Version::V003) {
            block_decode_v003(cr)
        } else {
            Err(invalid(format!("unsupported block header: {}", header)))
        }
    }
}

#[derive(Debug)]
pub(crate) enum CachedBlock {
    Decoded(Arc<Block>),
    RowGroup(Arc<RowGroupDirectory>),
}

#[cfg(test)]
#[allow(clippy::redundant_clone)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::ops::RangeBounds;

    use codeq::config::CodeqConfig;
    use codeq::testing::test_codec;
    use codeq::Decode;
    use codeq::Encode;
    use pretty_assertions::assert_eq;

    use crate::typ::Type;
    use crate::v001::bincode_config::bincode_config;
    use crate::v001::block::Block;
    use crate::v001::block_encoding_meta::BlockEncodingMeta;
    use crate::v001::header::Header;
    use crate::v001::testing::bb;
    use crate::v001::testing::ss;
    use crate::v001::types::Checksum;
    use crate::v001::SeqMarked;
    use crate::version::Version;

    fn range_keys<R: RangeBounds<String>>(block: &Block, r: R) -> Vec<String> {
        block.range(r).map(|(k, _)| k.to_string()).collect()
    }

    #[test]
    fn test_block_extracts_common_prefix() {
        let data = maplit::btreemap! {
            ss("exp-/0001") => SeqMarked::new_normal(1, bb("A")),
            ss("exp-/0005") => SeqMarked::new_normal(5, bb("E")),
            ss("exp-/0009") => SeqMarked::new_normal(9, bb("I")),
        };
        let block = Block::new(7, data);
        assert_eq!(block.prefix(), "exp-/000");
    }

    #[test]
    fn test_block_get_with_prefix() {
        let data = maplit::btreemap! {
            ss("exp-/0001") => SeqMarked::new_normal(1, bb("A")),
            ss("exp-/0009") => SeqMarked::new_tombstone(9),
        };
        let block = Block::new(7, data);

        assert_eq!(
            block.get("exp-/0001"),
            Some(&SeqMarked::new_normal(1, bb("A")))
        );
        assert_eq!(block.get("exp-/0009"), Some(&SeqMarked::new_tombstone(9)));
        assert_eq!(block.get("exp-/0002"), None); // inside prefix, absent
        assert_eq!(block.get("zzz"), None); // after the prefix
        assert_eq!(block.get("a"), None); // before the prefix
    }

    #[test]
    fn test_block_range_translates_bounds_across_prefix() {
        let data = maplit::btreemap! {
            ss("exp-/0001") => SeqMarked::new_normal(1, bb("A")),
            ss("exp-/0005") => SeqMarked::new_normal(5, bb("E")),
            ss("exp-/0009") => SeqMarked::new_normal(9, bb("I")),
        };
        let block = Block::new(7, data);
        assert_eq!(block.prefix(), "exp-/000");

        let all = vec![ss("exp-/0001"), ss("exp-/0005"), ss("exp-/0009")];
        let empty = Vec::<String>::new();

        assert_eq!(range_keys(&block, ..), all);

        // bounds that start with the prefix map into suffix space
        assert_eq!(range_keys(&block, ss("exp-/0005")..), vec![
            ss("exp-/0005"),
            ss("exp-/0009")
        ]);
        assert_eq!(range_keys(&block, ss("exp-/0005")..ss("exp-/0009")), vec![
            ss("exp-/0005")
        ]);
        assert_eq!(range_keys(&block, ss("exp-/0005")..=ss("exp-/0009")), vec![
            ss("exp-/0005"),
            ss("exp-/0009")
        ]);
        assert_eq!(range_keys(&block, ss("exp-/0003")..ss("exp-/0007")), vec![
            ss("exp-/0005")
        ]);

        // bounds outside the prefix clamp to all-or-nothing
        assert_eq!(range_keys(&block, ss("aaa")..), all); // start below prefix
        assert_eq!(range_keys(&block, ss("zzz")..), empty); // start above prefix
        assert_eq!(range_keys(&block, ..ss("aaa")), empty); // end below prefix
        assert_eq!(range_keys(&block, ..ss("zzz")), all); // end above prefix

        // bounds equal to the prefix itself
        assert_eq!(range_keys(&block, ss("exp-/000")..), all);
        assert_eq!(range_keys(&block, ..=ss("exp-/000")), empty); // no stored key equals the prefix

        // first-key boundary
        assert_eq!(range_keys(&block, ..ss("exp-/0001")), empty);
        assert_eq!(range_keys(&block, ..=ss("exp-/0001")), vec![ss(
            "exp-/0001"
        )]);
    }

    #[test]
    fn test_block_range_empty_common_prefix() {
        // No shared prefix: the block behaves like a plain sorted map.
        let data = maplit::btreemap! {
            ss("a") => SeqMarked::new_tombstone(1),
            ss("b") => SeqMarked::new_normal(2, bb("B")),
            ss("c") => SeqMarked::new_normal(3, bb("C")),
            ss("d") => SeqMarked::new_normal(4, bb("D")),
        };
        let block = Block::new(5, data);
        assert_eq!(block.prefix(), "");

        assert_eq!(None, block.get("z"));
        assert_eq!(Some(&SeqMarked::new_tombstone(1)), block.get("a"));
        assert_eq!(range_keys(&block, ss("b")..ss("e")), vec![
            ss("b"),
            ss("c"),
            ss("d")
        ]);
    }

    #[test]
    fn test_block_codec_roundtrip_with_prefix() -> anyhow::Result<()> {
        let data = maplit::btreemap! {
            ss("exp-/0001") => SeqMarked::new_normal(1, bb("A")),
            ss("exp-/0009") => SeqMarked::new_tombstone(9),
        };
        let block = Block::new(7, data);

        let mut b = Vec::new();
        block.encode(&mut b)?;

        let decoded = Block::decode(&b[..])?;
        assert_eq!(decoded.prefix(), "exp-/000");
        assert_eq!(decoded.get("exp-/0009"), Some(&SeqMarked::new_tombstone(9)));
        Ok(())
    }

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
        assert_eq!(block.header, Header::new(Type::Block, Version::V003));

        // Block::new() does not know the on-disk encoded size; mirror what encode wrote.
        block.meta.data_encoded_size = Block::decode(&b[..])?.data_encoded_size();

        test_codec(&b[..], &block)?;

        Ok(())
    }

    #[test]
    fn test_block_v003_compresses_large_payload() -> anyhow::Result<()> {
        // 500 entries sharing one repeated value: highly compressible.
        let data: BTreeMap<String, SeqMarked> = (0..500u64)
            .map(|i| {
                (
                    ss(format!("key/{i:08}")),
                    SeqMarked::new_normal(i, bb("repeated-value-payload")),
                )
            })
            .collect();
        let block = Block::new(0, data);

        let mut encoded = Vec::new();
        block.encode(&mut encoded)?;

        // The raw, uncompressed bincode of the same payload.
        let raw = bincode::encode_to_vec((block.prefix(), &block.data), bincode_config())?;

        // Compression must shrink the whole framed block below the raw payload,
        // even with the header + meta + checksum overhead.
        assert!(
            encoded.len() < raw.len(),
            "compressed block is {} bytes, expected smaller than raw payload {} bytes",
            encoded.len(),
            raw.len()
        );

        // And it must round-trip back to the same entries.
        let decoded = Block::decode(&encoded[..])?;
        assert_eq!(decoded.prefix(), block.prefix());
        assert_eq!(decoded.data, block.data);

        Ok(())
    }

    /// Frame arbitrary `payload` bytes into a V002 block with a valid checksum,
    /// so a test can drive decode against a malformed-but-intact payload (one the
    /// block checksum would otherwise mask).
    fn frame_v002_block(block_num: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        {
            let mut cw = Checksum::new_writer(&mut out);
            Header::new(Type::Block, Version::V002).encode(&mut cw).unwrap();
            BlockEncodingMeta::new(block_num, payload.len() as u64).encode(&mut cw).unwrap();
            cw.write_all(payload).unwrap();
            cw.write_checksum().unwrap();
        }
        out
    }

    #[test]
    fn test_block_v002_decode_rejects_unknown_compression_tag() {
        // Leading tag 9 is not a known compression type; the frame is otherwise intact.
        let bytes = frame_v002_block(7, &[9, 0, 1, 2, 3]);
        let err = Block::decode(&bytes[..]).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("unknown V002 compression tag: 9"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_block_v002_decode_rejects_empty_payload() {
        // Zero-length payload: not even a compression tag is present.
        let bytes = frame_v002_block(7, &[]);
        let err = Block::decode(&bytes[..]).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("empty V002 block payload"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_block_v002_decode_rejects_garbage_zstd_frame() {
        // Tag 1 claims zstd, but the body is not a valid zstd frame.
        let bytes = frame_v002_block(7, &[1, 0xff, 0xff, 0xff, 0xff]);
        assert!(Block::decode(&bytes[..]).is_err());
    }

    #[test]
    fn test_block_v002_decode_rejects_bitrot() -> anyhow::Result<()> {
        let data = maplit::btreemap! {
            ss("k1") => SeqMarked::new_normal(1, bb("v1")),
            ss("k2") => SeqMarked::new_normal(2, bb("v2")),
        };
        let mut bytes = Vec::new();
        Block::new(7, data).encode(&mut bytes)?;

        // Flip a byte inside the compressed body (just before the trailing checksum).
        // The block checksum, verified before decompression, must reject it.
        let i = bytes.len() - 5;
        bytes[i] ^= 0xff;

        let err = Block::decode(&bytes[..]).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        Ok(())
    }

    #[test]
    fn test_block_v002_decode_rejects_truncation() -> anyhow::Result<()> {
        let data = maplit::btreemap! {
            ss("k1") => SeqMarked::new_normal(1, bb("v1")),
            ss("k2") => SeqMarked::new_normal(2, bb("v2")),
        };
        let mut bytes = Vec::new();
        Block::new(7, data).encode(&mut bytes)?;

        // Bytes are missing relative to the framed length: decode must error, not hang or panic.
        assert!(Block::decode(&bytes[..bytes.len() - 10]).is_err());
        Ok(())
    }
}
