//! V002 block codec — the current on-disk format.
//!
//! A block is framed as `header + meta + payload + checksum`. The V002 payload
//! is the common prefix followed by the suffix-keyed entry map, encoded as two
//! independent bincode sections so a future format change can touch one without
//! the other. New blocks are written in this format.

use std::collections::BTreeMap;
use std::io::Error;
use std::io::Read;
use std::io::Write;

use codeq::config::CodeqConfig;
use codeq::ChecksumReader;
use codeq::Decode;
use codeq::Encode;

use crate::buf;
use crate::typ::Type;
use crate::v001::bincode_config::bincode_config;
use crate::v001::block::Block;
use crate::v001::block_encoding_meta::BlockEncodingMeta;
use crate::v001::header::Header;
use crate::v001::prefix::Prefix;
use crate::v001::types::Checksum;
use crate::v001::SeqMarked;
use crate::version::Version;

/// Encode a block in the V002 on-disk layout: `header + meta + payload +
/// checksum`. Returns the number of bytes written.
pub(crate) fn block_encode_v002<W: Write>(block: &Block, mut w: W) -> Result<usize, Error> {
    let invalid = |e| Error::new(std::io::ErrorKind::InvalidData, e);
    let mut n = 0usize;

    // The prefix and entries are two separate bincode sections. Buffer them
    // first: their total size goes into the meta, which precedes the payload.
    let mut payload = Vec::new();
    bincode::encode_into_std_write(block.prefix.as_str(), &mut payload, bincode_config())
        .map_err(invalid)?;
    bincode::encode_into_std_write(&block.data, &mut payload, bincode_config()).map_err(invalid)?;
    let encoded_size = payload.len() as u64;

    let mut cw = Checksum::new_writer(&mut w);

    n += block.header.encode(&mut cw)?;

    let meta = BlockEncodingMeta::new(block.meta.block_num(), encoded_size);
    n += meta.encode(&mut cw)?;

    cw.write_all(&payload)?;
    n += encoded_size as usize;
    n += cw.write_checksum()?;

    Ok(n)
}

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

    let invalid = |e| Error::new(std::io::ErrorKind::InvalidData, e);
    let (prefix, read): (String, usize) =
        bincode::decode_from_slice(&buf, bincode_config()).map_err(invalid)?;
    let (data, _): (BTreeMap<String, SeqMarked>, usize) =
        bincode::decode_from_slice(&buf[read..], bincode_config()).map_err(invalid)?;

    Ok(Block {
        header: Header::new(Type::Block, Version::V002),
        meta,
        prefix: Prefix::new(prefix),
        data,
    })
}
