//! V003 block codec with an uncompressed row-group directory.
//!
//! The directory and each compressed group have independent checksums so a
//! point lookup can validate only the bytes it reads.
//!
//! The payload is `[directory size][directory][compressed row groups]`.

use std::collections::BTreeMap;
use std::io::Error;
use std::io::Read;
use std::io::Write;
use std::ops::Bound;
use std::ops::Range;
use std::ops::RangeBounds;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use codeq::config::CodeqConfig;
use codeq::ChecksumReader;
use codeq::Decode;
use codeq::Encode;
use codeq::FixedSize;

use crate::buf;
use crate::storage::ReaderAt;
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

pub(crate) const DEFAULT_ROW_GROUP_MAX_ITEMS: usize = 64;

const CHECKSUM_SIZE: usize = 8;
const DIRECTORY_SIZE_SIZE: usize = 8;
const ZSTD_LEVEL: i32 = 1;

#[derive(Debug, bincode::Encode, bincode::Decode)]
struct RowGroupMeta {
    first_suffix: String,
    last_suffix: String,
    offset: u64,
    size: u64,
    item_count: u64,
}

/// The V003 directory retained in the block cache.
///
/// It contains only the common prefix and the row-group boundaries. Group bytes
/// remain compressed and are cached independently when they are read.
#[derive(Debug)]
pub(crate) struct RowGroupDirectory {
    meta: BlockEncodingMeta,
    prefix: Prefix,
    groups: Vec<RowGroupMeta>,
    group_data_offset: u64,
    group_data_size: u64,
}

impl RowGroupDirectory {
    pub(crate) fn group_count(&self) -> usize {
        self.groups.len()
    }

    pub(crate) fn cache_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.prefix.as_str().len()
            + self.groups.capacity() * std::mem::size_of::<RowGroupMeta>()
            + self
                .groups
                .iter()
                .map(|group| group.first_suffix.len() + group.last_suffix.len())
                .sum::<usize>()
    }

    pub(crate) fn group_index(&self, key: &str) -> Option<usize> {
        let suffix = self.prefix.strip(key)?;
        let index = self.groups.partition_point(|group| suffix > group.last_suffix.as_str());
        let group = self.groups.get(index)?;

        (suffix >= group.first_suffix.as_str()).then_some(index)
    }

    pub(crate) fn group_range<R>(&self, range: &R) -> Range<usize>
    where R: RangeBounds<String> {
        let Some((start, end)) = self.prefix.suffix_range(range) else {
            return 0..0;
        };

        let first = self.groups.partition_point(|group| ends_before(group, &start));
        let last = first + self.groups[first..].partition_point(|group| starts_before(group, &end));

        first..last
    }

    pub(crate) fn group_file_offset(
        &self,
        block_offset: u64,
        group_index: usize,
    ) -> Result<u64, Error> {
        let group = self.group(group_index)?;
        block_offset
            .checked_add(self.group_data_offset)
            .and_then(|offset| offset.checked_add(group.offset))
            .ok_or_else(|| invalid("V003 row-group offset overflow"))
    }

    pub(crate) fn group_size(&self, group_index: usize) -> Result<usize, Error> {
        let group = self.group(group_index)?;
        usize::try_from(group.size).map_err(invalid)
    }

    pub(crate) fn decode_group(
        &self,
        group_index: usize,
        encoded: &[u8],
    ) -> Result<Vec<(String, SeqMarked)>, Error> {
        self.validate_group_bytes(group_index, encoded)?;
        let group = self.group(group_index)?;
        let compressed = &encoded[..encoded.len() - CHECKSUM_SIZE];
        let raw = zstd::decode_all(compressed)?;
        let (rows, read): (Vec<(String, SeqMarked)>, usize) =
            bincode::decode_from_slice(&raw, bincode_config()).map_err(invalid)?;

        if read != raw.len() {
            return Err(invalid("trailing bytes in V003 row-group"));
        }

        validate_rows(group, &rows)?;
        Ok(rows)
    }

    pub(crate) fn validate_group_bytes(
        &self,
        group_index: usize,
        encoded: &[u8],
    ) -> Result<(), Error> {
        if encoded.len() != self.group_size(group_index)? {
            return Err(invalid("V003 row-group size does not match directory"));
        }
        verify_checksum(encoded, "V003 row-group")?;
        Ok(())
    }

    pub(crate) fn group_value(&self, key: &str, rows: &[(String, SeqMarked)]) -> Option<SeqMarked> {
        let suffix = self.prefix.strip(key)?;
        let row_index =
            rows.binary_search_by(|(row_suffix, _)| row_suffix.as_str().cmp(suffix)).ok()?;
        Some(rows[row_index].1.clone())
    }

    pub(crate) fn group_rows_in_range<R>(
        &self,
        rows: Vec<(String, SeqMarked)>,
        range: &R,
    ) -> Vec<(String, SeqMarked)>
    where
        R: RangeBounds<String>,
    {
        let Some((start, end)) = self.prefix.suffix_range(range) else {
            return Vec::new();
        };

        rows.into_iter()
            .filter(|(suffix, _)| contains_suffix(suffix, &start, &end))
            .map(|(suffix, value)| (format!("{}{}", self.prefix.as_str(), suffix), value))
            .collect()
    }

    pub(crate) fn to_block<F>(&self, mut decode_group: F) -> Result<Block, Error>
    where F: FnMut(usize) -> Result<Vec<(String, SeqMarked)>, Error> {
        let mut data = BTreeMap::new();

        for group_index in 0..self.group_count() {
            for (suffix, value) in decode_group(group_index)? {
                if data.insert(suffix, value).is_some() {
                    return Err(invalid("duplicate V003 row-group key"));
                }
            }
        }

        Ok(Block {
            header: Header::new(Type::Block, Version::V003),
            meta: self.meta.clone(),
            prefix: self.prefix.clone(),
            data,
        })
    }

    fn group(&self, group_index: usize) -> Result<&RowGroupMeta, Error> {
        self.groups.get(group_index).ok_or_else(|| invalid("V003 row-group index out of bounds"))
    }
}

/// Encode a V003 block using groups with at most `row_group_max_items` rows.
pub(crate) fn block_encode_v003<W: Write>(
    block: &Block,
    mut w: W,
    row_group_max_items: usize,
) -> Result<usize, Error> {
    if row_group_max_items == 0 {
        return Err(invalid(
            "BlockConfig.row_group_max_items must be greater than 0",
        ));
    }

    let (groups, group_data) = encode_groups(&block.data, row_group_max_items)?;
    let directory = encode_directory(&block.prefix, &groups)?;
    let payload = encode_payload(&directory, &group_data)?;
    let encoded_size = u64::try_from(payload.len()).map_err(invalid)?;

    let mut n = 0;
    let mut cw = Checksum::new_writer(&mut w);
    n += Header::new(Type::Block, Version::V003).encode(&mut cw)?;
    n += BlockEncodingMeta::new(block.meta.block_num(), encoded_size).encode(&mut cw)?;
    cw.write_all(&payload)?;
    n += payload.len();
    n += cw.write_checksum()?;
    Ok(n)
}

/// Decode a complete V003 block, including every row group.
pub(crate) fn block_decode_v003<R: Read>(
    mut cr: ChecksumReader<Checksum, R>,
) -> Result<Block, Error> {
    let meta = BlockEncodingMeta::decode(&mut cr)?;
    let payload_size = usize::try_from(meta.data_encoded_size()).map_err(invalid)?;
    let mut payload = buf::new_uninitialized(payload_size);
    cr.read_exact(&mut payload)?;
    cr.verify_checksum(|| "Block::decode()")?;

    let (directory, group_data) = decode_payload(meta, &payload)?;
    directory.to_block(|group_index| {
        let encoded = encoded_group(&directory, group_data, group_index)?;
        directory.decode_group(group_index, encoded)
    })
}

/// Read only the V003 header, directory size, and directory from disk.
pub(crate) fn read_v003_directory(
    file: &dyn ReaderAt,
    block_offset: u64,
    block_size: u64,
    fixed: &[u8],
) -> Result<RowGroupDirectory, Error> {
    let fixed_size = v003_fixed_prefix_size();
    if block_size < fixed_size as u64 + CHECKSUM_SIZE as u64 {
        return Err(invalid("V003 block is shorter than its fixed prefix"));
    }
    if fixed.len() != fixed_size {
        return Err(invalid("V003 fixed prefix has an unexpected size"));
    }

    let mut input = fixed;
    let header = Header::decode(&mut input)?;
    if header != Header::new(Type::Block, Version::V003) {
        return Err(invalid(format!("unsupported V003 block header: {header}")));
    }

    let meta = BlockEncodingMeta::decode(&mut input)?;
    let directory_size = input.read_u64::<BigEndian>()?;
    let group_data_size = validate_block_layout(&meta, block_size, directory_size)?;
    let directory_size = usize::try_from(directory_size).map_err(invalid)?;
    let directory_offset = block_offset
        .checked_add(fixed_size as u64)
        .ok_or_else(|| invalid("V003 directory offset overflow"))?;
    let mut directory = buf::new_uninitialized(directory_size);
    file.read_exact_at(&mut directory, directory_offset)?;

    let (prefix, groups) = decode_directory(&directory)?;
    new_directory(
        meta,
        prefix,
        groups,
        u64::try_from(directory_size).map_err(invalid)?,
        group_data_size,
    )
}

fn encode_groups(
    data: &BTreeMap<String, SeqMarked>,
    row_group_max_items: usize,
) -> Result<(Vec<RowGroupMeta>, Vec<u8>), Error> {
    let mut groups = Vec::new();
    let mut group_data = Vec::new();
    let mut rows = Vec::with_capacity(row_group_max_items);

    for (suffix, value) in data {
        rows.push((suffix.clone(), value.clone()));
        if rows.len() == row_group_max_items {
            let group = std::mem::replace(&mut rows, Vec::with_capacity(row_group_max_items));
            append_group(group, &mut groups, &mut group_data)?;
        }
    }

    if !rows.is_empty() {
        append_group(rows, &mut groups, &mut group_data)?;
    }

    Ok((groups, group_data))
}

fn append_group(
    rows: Vec<(String, SeqMarked)>,
    groups: &mut Vec<RowGroupMeta>,
    group_data: &mut Vec<u8>,
) -> Result<(), Error> {
    let first_suffix =
        rows.first().ok_or_else(|| invalid("cannot encode empty V003 row-group"))?.0.clone();
    let last_suffix =
        rows.last().ok_or_else(|| invalid("cannot encode empty V003 row-group"))?.0.clone();
    let raw = bincode::encode_to_vec(&rows, bincode_config()).map_err(invalid)?;
    let mut encoded = zstd::encode_all(raw.as_slice(), ZSTD_LEVEL)?;
    append_checksum(&mut encoded)?;

    let offset = u64::try_from(group_data.len()).map_err(invalid)?;
    let size = u64::try_from(encoded.len()).map_err(invalid)?;
    let item_count = u64::try_from(rows.len()).map_err(invalid)?;
    group_data.extend_from_slice(&encoded);
    groups.push(RowGroupMeta {
        first_suffix,
        last_suffix,
        offset,
        size,
        item_count,
    });
    Ok(())
}

fn encode_directory(prefix: &Prefix, groups: &[RowGroupMeta]) -> Result<Vec<u8>, Error> {
    let mut directory = Vec::new();
    bincode::encode_into_std_write(prefix.as_str(), &mut directory, bincode_config())
        .map_err(invalid)?;
    bincode::encode_into_std_write(groups, &mut directory, bincode_config()).map_err(invalid)?;
    append_checksum(&mut directory)?;
    Ok(directory)
}

fn encode_payload(directory: &[u8], group_data: &[u8]) -> Result<Vec<u8>, Error> {
    let mut payload = Vec::with_capacity(DIRECTORY_SIZE_SIZE + directory.len() + group_data.len());
    payload.write_u64::<BigEndian>(u64::try_from(directory.len()).map_err(invalid)?)?;
    payload.extend_from_slice(directory);
    payload.extend_from_slice(group_data);
    Ok(payload)
}

fn decode_payload(
    meta: BlockEncodingMeta,
    payload: &[u8],
) -> Result<(RowGroupDirectory, &[u8]), Error> {
    if payload.len() != usize::try_from(meta.data_encoded_size()).map_err(invalid)? {
        return Err(invalid("V003 payload size does not match metadata"));
    }

    let mut input = payload;
    let directory_size = input.read_u64::<BigEndian>()?;
    let directory_size = usize::try_from(directory_size).map_err(invalid)?;
    if input.len() < directory_size {
        return Err(invalid("V003 directory exceeds payload"));
    }

    let (directory, group_data) = input.split_at(directory_size);
    let (prefix, groups) = decode_directory(directory)?;
    let directory = new_directory(
        meta,
        prefix,
        groups,
        u64::try_from(directory_size).map_err(invalid)?,
        u64::try_from(group_data.len()).map_err(invalid)?,
    )?;
    Ok((directory, group_data))
}

fn decode_directory(directory: &[u8]) -> Result<(Prefix, Vec<RowGroupMeta>), Error> {
    let body = verify_checksum(directory, "V003 directory")?;
    let (prefix, prefix_size): (String, usize) =
        bincode::decode_from_slice(body, bincode_config()).map_err(invalid)?;
    let (groups, groups_size): (Vec<RowGroupMeta>, usize) =
        bincode::decode_from_slice(&body[prefix_size..], bincode_config()).map_err(invalid)?;
    if prefix_size.checked_add(groups_size) != Some(body.len()) {
        return Err(invalid("trailing bytes in V003 directory"));
    }

    Ok((Prefix::new(prefix), groups))
}

fn new_directory(
    meta: BlockEncodingMeta,
    prefix: Prefix,
    groups: Vec<RowGroupMeta>,
    directory_size: u64,
    group_data_size: u64,
) -> Result<RowGroupDirectory, Error> {
    validate_group_layout(&groups, group_data_size)?;
    let group_data_offset = block_header_meta_size()
        .checked_add(DIRECTORY_SIZE_SIZE as u64)
        .and_then(|offset| offset.checked_add(directory_size))
        .ok_or_else(|| invalid("V003 group data offset overflow"))?;
    Ok(RowGroupDirectory {
        meta,
        prefix,
        groups,
        group_data_offset,
        group_data_size,
    })
}

fn validate_block_layout(
    meta: &BlockEncodingMeta,
    block_size: u64,
    directory_size: u64,
) -> Result<u64, Error> {
    let payload_size = meta.data_encoded_size();
    let total_size = block_header_meta_size()
        .checked_add(payload_size)
        .and_then(|size| size.checked_add(CHECKSUM_SIZE as u64))
        .ok_or_else(|| invalid("V003 block size overflow"))?;
    if total_size != block_size {
        return Err(invalid("V003 block size does not match metadata"));
    }

    let directory_and_size = (DIRECTORY_SIZE_SIZE as u64)
        .checked_add(directory_size)
        .ok_or_else(|| invalid("V003 directory size overflow"))?;
    payload_size
        .checked_sub(directory_and_size)
        .ok_or_else(|| invalid("V003 directory exceeds payload"))
}

fn validate_group_layout(groups: &[RowGroupMeta], group_data_size: u64) -> Result<(), Error> {
    let mut expected_offset = 0;
    let mut previous_last: Option<&str> = None;

    for group in groups {
        if group.item_count == 0 || group.size < CHECKSUM_SIZE as u64 {
            return Err(invalid("invalid V003 row-group metadata"));
        }
        if group.first_suffix > group.last_suffix {
            return Err(invalid("V003 row-group key range is inverted"));
        }
        if previous_last.is_some_and(|last| last >= group.first_suffix.as_str()) {
            return Err(invalid("V003 row-group key ranges overlap"));
        }
        if group.offset != expected_offset {
            return Err(invalid("V003 row-group offsets are not contiguous"));
        }

        expected_offset = expected_offset
            .checked_add(group.size)
            .ok_or_else(|| invalid("V003 row-group size overflow"))?;
        previous_last = Some(group.last_suffix.as_str());
    }

    if expected_offset != group_data_size {
        return Err(invalid("V003 row-group sizes do not match payload"));
    }
    Ok(())
}

fn validate_rows(group: &RowGroupMeta, rows: &[(String, SeqMarked)]) -> Result<(), Error> {
    if u64::try_from(rows.len()).map_err(invalid)? != group.item_count {
        return Err(invalid(
            "V003 row-group item count does not match directory",
        ));
    }

    let first = rows.first().ok_or_else(|| invalid("decoded empty V003 row-group"))?.0.as_str();
    let last = rows.last().ok_or_else(|| invalid("decoded empty V003 row-group"))?.0.as_str();
    if first != group.first_suffix || last != group.last_suffix {
        return Err(invalid("V003 row-group key range does not match directory"));
    }
    if rows.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(invalid("V003 row-group keys are not strictly sorted"));
    }
    Ok(())
}

fn encoded_group<'a>(
    directory: &RowGroupDirectory,
    group_data: &'a [u8],
    group_index: usize,
) -> Result<&'a [u8], Error> {
    if u64::try_from(group_data.len()).map_err(invalid)? != directory.group_data_size {
        return Err(invalid("V003 group data size does not match directory"));
    }

    let group = directory.group(group_index)?;
    let start = usize::try_from(group.offset).map_err(invalid)?;
    let end = start
        .checked_add(usize::try_from(group.size).map_err(invalid)?)
        .ok_or_else(|| invalid("V003 row-group range overflow"))?;
    group_data.get(start..end).ok_or_else(|| invalid("V003 row-group exceeds payload"))
}

fn ends_before(group: &RowGroupMeta, start: &Bound<String>) -> bool {
    match start {
        Bound::Unbounded => false,
        Bound::Included(key) => group.last_suffix < *key,
        Bound::Excluded(key) => group.last_suffix <= *key,
    }
}

fn starts_before(group: &RowGroupMeta, end: &Bound<String>) -> bool {
    match end {
        Bound::Unbounded => true,
        Bound::Included(key) => group.first_suffix <= *key,
        Bound::Excluded(key) => group.first_suffix < *key,
    }
}

fn contains_suffix(suffix: &str, start: &Bound<String>, end: &Bound<String>) -> bool {
    let after_start = match start {
        Bound::Unbounded => true,
        Bound::Included(key) => suffix >= key.as_str(),
        Bound::Excluded(key) => suffix > key.as_str(),
    };
    let before_end = match end {
        Bound::Unbounded => true,
        Bound::Included(key) => suffix <= key.as_str(),
        Bound::Excluded(key) => suffix < key.as_str(),
    };
    after_start && before_end
}

fn append_checksum(bytes: &mut Vec<u8>) -> Result<(), Error> {
    bytes.write_u64::<BigEndian>(Checksum::hash(bytes))?;
    Ok(())
}

fn verify_checksum<'a>(bytes: &'a [u8], context: &str) -> Result<&'a [u8], Error> {
    let body_size = bytes
        .len()
        .checked_sub(CHECKSUM_SIZE)
        .ok_or_else(|| invalid(format!("{context} is missing its checksum")))?;
    let (body, checksum) = bytes.split_at(body_size);
    let mut checksum = checksum;
    let actual = checksum.read_u64::<BigEndian>()?;
    let expected = Checksum::hash(body);
    if actual != expected {
        return Err(invalid(format!(
            "crc32 checksum mismatch: expected {expected:x}, got {actual:x}, while {context}"
        )));
    }
    Ok(body)
}

fn block_header_meta_size() -> u64 {
    (Header::encoded_size() + BlockEncodingMeta::encoded_size()) as u64
}

pub(crate) fn v003_fixed_prefix_size() -> usize {
    Header::encoded_size() + BlockEncodingMeta::encoded_size() + DIRECTORY_SIZE_SIZE
}
