use std::io;

use codeq::config::CodeqConfig;
use codeq::Decode;
use codeq::Encode;

use super::invalid_data;
use super::slot_for_manifest_seq;
use super::validate_slot_id;
use super::Levels;
use super::TableInfo;
use crate::typ::Type;
use crate::v001::header::Header;
use crate::v001::types::Checksum;
use crate::version::Version;

/// Authoritative persisted table set and allocation counters for a DB.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Manifest<UserData = ()> {
    pub(super) levels: Levels,
    pub(super) next_table_id: u32,
    pub(super) next_level: u32,
    // Key sequence 0 is reserved as the not-found sentinel.
    pub(super) last_key_seq: u64,
    pub(super) user_data: UserData,
    pub(super) manifest_seq: u64,
}

impl Manifest<()> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<UserData> Manifest<UserData>
where UserData: Encode + Decode
{
    pub fn new_with_user_data(user_data: UserData) -> Self {
        Self {
            user_data,
            levels: Levels::default(),
            next_table_id: 0,
            next_level: 0,
            last_key_seq: 0,
            manifest_seq: 0,
        }
    }

    pub fn levels(&self) -> &Levels {
        &self.levels
    }

    pub fn user_data(&self) -> &UserData {
        &self.user_data
    }

    pub fn user_data_mut(&mut self) -> &mut UserData {
        &mut self.user_data
    }

    pub fn allocate_table_id(&mut self) -> u32 {
        let table_id = self.next_table_id;
        self.next_table_id = self.next_table_id.checked_add(1).expect("next_table_id overflow");
        table_id
    }

    pub fn allocate_level(&mut self) -> u32 {
        let level = self.next_level;
        self.next_level = self.next_level.checked_add(1).expect("next_level overflow");
        level
    }

    /// Allocate the next key sequence number. Panics on `u64` overflow, matching
    /// the other exhausted-counter allocators (an unreachable event that leaves
    /// the DB unusable).
    pub fn allocate_key_seq(&mut self) -> u64 {
        let key_seq = self.last_key_seq.checked_add(1).expect("last_key_seq overflow");
        self.last_key_seq = key_seq;
        key_seq
    }

    /// The file header this manifest encodes at the beginning of its frame.
    pub fn header(&self) -> Header {
        Header::new(Type::ManifestFile, Version::V001)
    }

    pub fn manifest_seq(&self) -> u64 {
        self.manifest_seq
    }

    /// The slot file (`manifest_seq % MANIFEST_SLOT_COUNT`) this manifest is
    /// written to.
    pub fn manifest_slot(&self) -> u8 {
        slot_for_manifest_seq(self.manifest_seq)
    }

    /// Insert one table after cheap local checks.
    ///
    /// Rejects unallocated ids/levels, duplicate table ids and range overlap
    /// before mutating, so no rollback is needed. The O(n) `validate()` runs
    /// once per commit in [`Self::encode_file`], not per mutation.
    pub fn add_table(&mut self, table: TableInfo) -> Result<(), io::Error> {
        if table.level() >= self.next_level {
            return Err(invalid_data(format!(
                "table level {} reaches next_level {}",
                table.level(),
                self.next_level
            )));
        }
        if table.table_id() >= self.next_table_id {
            return Err(invalid_data(format!(
                "table_id {} reaches next_table_id {}",
                table.table_id(),
                self.next_table_id
            )));
        }
        if self.levels.table_by_id(table.table_id()).is_some() {
            return Err(invalid_data(format!(
                "duplicate table_id {}",
                table.table_id()
            )));
        }
        self.levels.insert(table)
    }

    /// Remove one table; removal cannot break level invariants.
    pub fn remove_table(&mut self, table_id: u32) -> Result<TableInfo, io::Error> {
        self.levels
            .remove_table(table_id)
            .ok_or_else(|| invalid_data(format!("remove table_id {} does not exist", table_id)))
    }

    /// Advance the manifest snapshot sequence by exactly one (the commit
    /// boundary). Panics on `u64` overflow, matching the id/level/key-seq
    /// allocators. Validation of the table set happens once at [`Self::encode_file`].
    pub fn advance_manifest_seq(&mut self) -> u64 {
        let manifest_seq = self.manifest_seq.checked_add(1).expect("manifest_seq overflow");
        self.manifest_seq = manifest_seq;
        manifest_seq
    }

    pub fn validate(&self) -> Result<(), io::Error> {
        self.levels.validate(self.next_level, self.next_table_id)
    }

    /// Encode the complete manifest file frame. Runs the full O(n) [`Self::validate`]
    /// once here — the single validation point on the write path.
    pub fn encode_file(&self) -> Result<Vec<u8>, io::Error> {
        let mut out = Vec::new();
        self.encode(&mut out)?;
        Ok(out)
    }

    /// Decode a manifest file and check that `slot` matches its `manifest_seq`.
    pub fn decode_file_from_slot(bytes: &[u8], slot: u8) -> Result<Self, io::Error> {
        let manifest = Self::decode_from_reader(bytes)?;
        super::validate_manifest_slot(slot, manifest.manifest_seq())?;
        Ok(manifest)
    }

    /// Pick the committed manifest with the highest `manifest_seq`.
    ///
    /// Callers pass one entry per slot file that exists in storage; absent
    /// slots are simply not passed. An empty input means a fresh DB and
    /// returns `Ok(None)`. If slot bytes exist but none decodes, this is
    /// storage corruption, not a fresh DB: return an error so startup fails
    /// instead of silently starting empty (which would let startup GC delete
    /// every table file). Invalid slots are logged; after a crash one torn
    /// slot (the oldest seq) is expected.
    pub fn select_latest_valid<'a>(
        slots: impl IntoIterator<Item = (u8, &'a [u8])>,
    ) -> Result<Option<Self>, io::Error> {
        let mut latest = None;
        let mut invalid = Vec::new();
        for (slot, bytes) in slots {
            validate_slot_id(slot)?;
            match Self::decode_file_from_slot(bytes, slot) {
                Ok(manifest) => {
                    let is_newer = latest
                        .as_ref()
                        .is_none_or(|prev: &Self| manifest.manifest_seq > prev.manifest_seq);
                    if is_newer {
                        latest = Some(manifest);
                    }
                }
                Err(e) => {
                    log::warn!("manifest slot {} is invalid: {}", slot, e);
                    invalid.push(slot);
                }
            }
        }
        if let Some(manifest) = &latest {
            log::info!(
                "recovered manifest_seq {}; invalid slots: {:?}",
                manifest.manifest_seq,
                invalid
            );
        } else if !invalid.is_empty() {
            return Err(invalid_data(format!(
                "manifest slots {:?} exist but none is valid; refusing to start empty",
                invalid
            )));
        }
        Ok(latest)
    }

    fn decode_from_reader<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
        let mut bytes = Vec::new();
        r.read_to_end(&mut bytes)?;
        let mut input = bytes.as_slice();
        let mut cr = Checksum::new_reader(&mut input);
        let header = Header::decode(&mut cr)?;
        if header != Header::new(Type::ManifestFile, Version::V001) {
            return Err(invalid_data(format!(
                "unsupported manifest file header {}",
                header
            )));
        }

        let manifest = Self::decode_v001(cr)?;
        if !input.is_empty() {
            return Err(invalid_data("trailing bytes in manifest file"));
        }
        Ok(manifest)
    }
}

impl<UserData> Encode for Manifest<UserData>
where UserData: Encode + Decode
{
    fn encode<W: io::Write>(&self, w: W) -> Result<usize, io::Error> {
        self.encode_v001(w)
    }
}

impl<UserData> Decode for Manifest<UserData>
where UserData: Encode + Decode
{
    fn decode<R: io::Read>(r: R) -> Result<Self, io::Error> {
        Self::decode_from_reader(r)
    }
}
