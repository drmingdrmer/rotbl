use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::io::Read;
use std::io::Write;

use codeq::Decode;
use codeq::Encode;

use super::invalid_data;
use super::LevelManifest;
use super::TableInfo;
use super::MANIFEST_MAX_BYTES;
use crate::typ::Type;
use crate::v001::header::Header;
use crate::version::Version;

const LEVELS_V001_ZSTD_LEVEL: i32 = 1;

/// All manifest levels, keyed by level number.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Levels {
    levels: BTreeMap<u32, LevelManifest>,
}

impl Levels {
    pub fn version(&self) -> Version {
        Version::V001
    }

    /// The file header this level set encodes at the beginning of its frame.
    pub fn header(&self) -> Header {
        Header::new(Type::Levels, self.version())
    }

    /// The level map, keyed by level number. Used by startup file GC to test
    /// whether a `table_id` is still referenced.
    pub fn as_map(&self) -> &BTreeMap<u32, LevelManifest> {
        &self.levels
    }

    /// The [`LevelManifest`] for `level`, if the level exists (an `O(log n)` map lookup).
    pub fn level(&self, level: u32) -> Option<&LevelManifest> {
        self.levels.get(&level)
    }

    /// Find the [`TableInfo`] with `table_id`. This is an `O(n)` scan across all
    /// levels, acceptable at the advisory manifest scale.
    pub fn table_by_id(&self, table_id: u32) -> Option<TableInfo> {
        for (level, tables) in &self.levels {
            if let Some(table) = tables.table_by_id(*level, table_id) {
                return Some(table);
            }
        }
        None
    }

    /// Insert one table into its level, creating the level on first use.
    ///
    /// The level entry is materialized only when [`LevelManifest::insert`]
    /// succeeds: a rejected table (e.g. `smallest > largest`) must not leave an
    /// empty level behind, which `validate` would then reject forever.
    pub(super) fn insert(&mut self, table: TableInfo) -> Result<(), io::Error> {
        let level = table.level();
        match self.levels.entry(level) {
            Entry::Occupied(mut e) => e.get_mut().insert(table),
            Entry::Vacant(e) => {
                let mut manifest = LevelManifest::default();
                manifest.insert(table)?;
                e.insert(manifest);
                Ok(())
            }
        }
    }

    /// Remove the table with `table_id`, returning it. Drops the level too if it
    /// becomes empty, so no invalid empty-level intermediate state is exposed.
    pub(super) fn remove_table(&mut self, table_id: u32) -> Option<TableInfo> {
        let mut removed = None;
        for (level_num, level) in &mut self.levels {
            if let Some(table) = level.remove_table(*level_num, table_id) {
                removed = Some((*level_num, table, level.is_empty()));
                break;
            }
        }
        let (level_num, table, is_empty) = removed?;
        if is_empty {
            self.levels.remove(&level_num);
        }
        Some(table)
    }

    pub(super) fn validate(&self, next_level: u32, next_table_id: u32) -> Result<(), io::Error> {
        let mut table_ids = BTreeSet::new();
        for (level, tables) in &self.levels {
            if *level >= next_level {
                return Err(invalid_data(format!(
                    "level {} reaches next_level {}",
                    level, next_level
                )));
            }
            tables.validate(*level, next_table_id, &mut table_ids)?;
        }
        Ok(())
    }

    fn encode_payload<W: Write>(&self, mut w: W) -> Result<usize, io::Error> {
        let mut n = 0;
        n += (self.levels.len() as u64).encode(&mut w)?;
        for (level, manifest) in &self.levels {
            n += level.encode(&mut w)?;
            n += manifest.encode(&mut w)?;
        }
        Ok(n)
    }

    fn decode_payload<R: Read>(mut r: R) -> Result<Self, io::Error> {
        let len = u64::decode(&mut r)?;
        let mut levels = BTreeMap::new();
        for _ in 0..len {
            let level = u32::decode(&mut r)?;
            let manifest = LevelManifest::decode(&mut r)?;
            if levels.insert(level, manifest).is_some() {
                return Err(invalid_data(format!("duplicate manifest level {}", level)));
            }
        }
        Ok(Self { levels })
    }
}

impl Encode for Levels {
    /// Frame: `header + uncompressed_len + zstd(payload)`. No inner checksum —
    /// the whole frame lives inside the [`Manifest`](super::Manifest) frame's
    /// checksum, so a second one here would only re-cover already-covered bytes
    /// (see `manifest-design.md`).
    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        let mut raw = Vec::new();
        self.encode_payload(&mut raw)?;
        let payload = zstd::encode_all(raw.as_slice(), LEVELS_V001_ZSTD_LEVEL)?;
        warn_manifest_max_bytes("levels uncompressed payload", raw.len() as u64);
        warn_manifest_max_bytes("levels compressed payload", payload.len() as u64);

        let mut n = 0;
        n += self.header().encode(&mut w)?;
        n += (raw.len() as u64).encode(&mut w)?;
        n += payload.encode(&mut w)?;
        Ok(n)
    }
}

impl Decode for Levels {
    fn decode<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
        let header = Header::decode(&mut r)?;
        if header != Header::new(Type::Levels, Version::V001) {
            return Err(invalid_data(format!(
                "unsupported levels header {}",
                header
            )));
        }

        let uncompressed_len = u64::decode(&mut r)?;

        // Read the compressed payload without trusting its length prefix: a
        // corrupt length reads only as many bytes as the input actually holds
        // (`take` + `read_to_end` grows incrementally), never pre-allocating one
        // oversized buffer before the outer `Manifest` checksum is verified.
        let compressed_len = u32::decode(&mut r)? as u64;
        let mut payload = Vec::new();
        (&mut r).take(compressed_len).read_to_end(&mut payload)?;
        if payload.len() as u64 != compressed_len {
            return Err(invalid_data(format!(
                "levels compressed payload declares {} bytes but only {} are available",
                compressed_len,
                payload.len()
            )));
        }
        warn_manifest_max_bytes("levels uncompressed payload", uncompressed_len);
        warn_manifest_max_bytes("levels compressed payload", payload.len() as u64);

        // Bound decompression by the declared length so a bad frame can never
        // allocate unbounded memory. Read one byte past the declared length:
        // `take()` truncates silently, so without it a stream longer than
        // declared would still read exactly `uncompressed_len` bytes and pass
        // the length check below.
        let mut raw = Vec::new();
        zstd::Decoder::new(payload.as_slice())?
            .take(uncompressed_len.saturating_add(1))
            .read_to_end(&mut raw)?;
        if raw.len() as u64 != uncompressed_len {
            return Err(invalid_data(format!(
                "levels payload decompressed to {} bytes, expected {}",
                raw.len(),
                uncompressed_len
            )));
        }
        let mut input = raw.as_slice();
        let levels = Self::decode_payload(&mut input)?;
        if !input.is_empty() {
            return Err(invalid_data("trailing bytes in levels payload"));
        }
        Ok(levels)
    }
}

fn warn_manifest_max_bytes(name: &str, bytes: u64) {
    if bytes > MANIFEST_MAX_BYTES as u64 {
        log::warn!(
            "{} is {} bytes, exceeds advisory MANIFEST_MAX_BYTES {}",
            name,
            bytes,
            MANIFEST_MAX_BYTES
        );
    }
}
