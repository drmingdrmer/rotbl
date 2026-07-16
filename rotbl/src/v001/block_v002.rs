//! V002 block decoder, retained for backward compatibility.
//!
//! A block is framed as `header + meta + payload + checksum`. The V002 payload
//! is `[compression tag][body]`: the tag records the compression algorithm, and
//! the body is that algorithm applied to two independent bincode sections — the
//! common prefix and the suffix-keyed entry map — so a future format change can
//! touch one without the other. V002 blocks use a single zstd frame.

use std::collections::BTreeMap;
use std::io::Error;
use std::io::Read;

use codeq::ChecksumReader;
use codeq::Decode;

use crate::buf;
use crate::typ::Type;
use crate::v001::bincode_config::bincode_config;
use crate::v001::block::invalid;
use crate::v001::block::Block;
use crate::v001::block_encoding_meta::BlockEncodingMeta;
use crate::v001::header::Header;
use crate::v001::prefix::Prefix;
use crate::v001::types::Checksum;
use crate::v001::SeqMarked;
use crate::version::Version;

/// Compression tag: the body is a single zstd frame.
const COMPRESSION_ZSTD: u8 = 1;
/// Decode a V002 block from `cr`, whose header has already been read (and so is
/// already folded into the running checksum). Reads the meta, payload, and
/// trailing checksum, returning the block in canonical in-memory form.
pub(crate) fn block_decode_v002<R: Read>(
    mut cr: ChecksumReader<Checksum, R>,
) -> Result<Block, Error> {
    let meta = BlockEncodingMeta::decode(&mut cr)?;

    let data_size = meta.data_encoded_size() as usize;
    let mut buf = buf::new_uninitialized(data_size);
    cr.read_exact(&mut buf)?;
    cr.verify_checksum(|| "Block::decode()")?;

    // Payload is `[compression tag][body]`; recover the raw bincode body.
    let (tag, body) = buf.split_first().ok_or_else(|| invalid("empty V002 block payload"))?;
    let raw = match *tag {
        COMPRESSION_ZSTD => zstd::decode_all(body)?,
        other => return Err(invalid(format!("unknown V002 compression tag: {other}"))),
    };

    let (prefix, read): (String, usize) =
        bincode::decode_from_slice(&raw, bincode_config()).map_err(invalid)?;
    let (data, _): (BTreeMap<String, SeqMarked>, usize) =
        bincode::decode_from_slice(&raw[read..], bincode_config()).map_err(invalid)?;

    Ok(Block {
        header: Header::new(Type::Block, Version::V003),
        meta,
        prefix: Prefix::new(prefix),
        data,
    })
}
