//! V001 block codec — the original format, kept for reading old blocks.
//!
//! The V001 payload is simply the full-key entry map. Blocks are written as V003
//! now, so V001 is decode-only: [`block_decode_v001`] re-extracts the common
//! prefix to produce the same canonical in-memory form as V003.

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

/// Decode a V001 block from `cr`, whose header has already been read (and so is
/// already folded into the running checksum). Reads the meta, payload, and
/// trailing checksum, then extracts the common prefix so the result matches
/// V003's canonical in-memory form.
pub(crate) fn block_decode_v001<R: Read>(
    mut cr: ChecksumReader<Checksum, R>,
) -> Result<Block, Error> {
    let meta = BlockEncodingMeta::decode(&mut cr)?;

    let data_size = meta.data_encoded_size() as usize;
    let mut buf = buf::new_uninitialized(data_size);
    cr.read_exact(&mut buf)?;
    cr.verify_checksum(|| "Block::decode()")?;

    let (full, _): (BTreeMap<String, SeqMarked>, usize) =
        bincode::decode_from_slice(&buf, bincode_config()).map_err(invalid)?;
    let (prefix, data) = Prefix::extract(full);

    Ok(Block {
        header: Header::new(Type::Block, Version::V003),
        meta,
        prefix,
        data,
    })
}
