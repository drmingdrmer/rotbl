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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v001::testing::table_info as table;
    use crate::v001::MANIFEST_MAX_BYTES;
    use crate::v001::MANIFEST_SLOT_COUNT;

    /// Allocate ids/level and add a table whose keys are derived from its id, so
    /// distinct tables never collide.
    fn add_generated_table<UserData>(
        manifest: &mut Manifest<UserData>,
    ) -> Result<TableInfo, io::Error>
    where UserData: Encode + Decode + Clone {
        let table_id = manifest.allocate_table_id();
        let level = manifest.allocate_level();
        let table = table(
            table_id,
            level,
            &format!("k{table_id:03}"),
            &format!("k{table_id:03}z"),
        );
        manifest.add_table(table.clone())?;
        Ok(table)
    }

    #[test]
    fn test_manifest_allocates_cursors_and_adds_table() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        assert_eq!(manifest.allocate_key_seq(), 1);
        add_generated_table(&mut manifest)?;
        assert_eq!(manifest.advance_manifest_seq(), 1);

        let mut cursors = manifest.clone();
        assert_eq!(cursors.allocate_table_id(), 1);
        assert_eq!(cursors.allocate_level(), 1);
        assert_eq!(cursors.allocate_key_seq(), 2);
        assert_eq!(manifest.manifest_seq(), 1);
        assert_eq!(manifest.manifest_slot(), 1);
        assert_eq!(
            manifest.levels().table_by_id(0),
            Some(table(0, 0, "k000", "k000z")),
        );
        assert_eq!(
            manifest.levels().level(0).unwrap().table_by_smallest(0, "k000"),
            Some(table(0, 0, "k000", "k000z")),
        );
        Ok(())
    }

    #[test]
    fn test_manifest_header() {
        assert_eq!(
            Manifest::new().header(),
            Header::new(Type::ManifestFile, Version::V001)
        );
    }

    #[test]
    fn test_manifest_remove_table() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        add_generated_table(&mut manifest)?;

        assert_eq!(manifest.remove_table(0)?, table(0, 0, "k000", "k000z"));
        assert_eq!(manifest.levels().table_by_id(0), None);
        assert_eq!(manifest.levels().level(0), None);
        Ok(())
    }

    #[test]
    fn test_manifest_rejects_overlapping_ranges() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        let level = manifest.allocate_level();
        let table_id = manifest.allocate_table_id();
        manifest.add_table(table(table_id, level, "a", "c"))?;

        let table_id = manifest.allocate_table_id();
        let err = manifest.add_table(table(table_id, level, "b", "d")).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("overlapping table ranges"));
        Ok(())
    }

    #[test]
    fn test_manifest_rejects_duplicate_table_id() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        let table_added = add_generated_table(&mut manifest)?;
        let level = manifest.allocate_level();

        let err = manifest.add_table(table(table_added.table_id(), level, "x", "y")).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("duplicate table_id"));
        Ok(())
    }

    #[test]
    fn test_manifest_rejects_unallocated_ids() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        let err = manifest.add_table(table(0, 0, "a", "b")).unwrap_err();
        assert!(err.to_string().contains("reaches next_level"));

        manifest.allocate_level();
        let err = manifest.add_table(table(0, 0, "a", "b")).unwrap_err();
        assert!(err.to_string().contains("reaches next_table_id"));
        Ok(())
    }

    /// C1 regression: a rejected `add_table` must not leave an empty level that
    /// then makes every later `validate()`/`encode()` fail.
    #[test]
    fn test_add_table_bad_range_does_not_poison_level() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        let level = manifest.allocate_level();
        let table_id = manifest.allocate_table_id();

        // `smallest > largest` is rejected...
        let err = manifest.add_table(table(table_id, level, "z", "a")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        // ...and must not have created an empty level behind it.
        assert_eq!(manifest.levels().level(level), None);
        manifest.validate()?;

        // A good insert on the same level still works.
        manifest.add_table(table(table_id, level, "a", "z"))?;
        manifest.validate()?;
        assert_eq!(
            manifest.levels().table_by_id(table_id),
            Some(table(table_id, level, "a", "z"))
        );
        Ok(())
    }

    #[test]
    fn test_remove_table_missing_id_errors() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        add_generated_table(&mut manifest)?;

        let err = manifest.remove_table(999).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("does not exist"));
        Ok(())
    }

    #[test]
    fn test_manifest_file_round_trip() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        add_generated_table(&mut manifest)?;
        manifest.advance_manifest_seq();

        let encoded = manifest.encode_file()?;
        let decoded = Manifest::decode_file_from_slot(&encoded, manifest.manifest_slot())?;

        assert_eq!(manifest, decoded);
        assert!(encoded.len() < MANIFEST_MAX_BYTES);
        Ok(())
    }

    /// A manifest with several levels and several tables per level survives an
    /// encode/decode round trip with its structure and ordering intact.
    #[test]
    fn test_manifest_multi_level_round_trip() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        let l0 = manifest.allocate_level();
        let l1 = manifest.allocate_level();
        for (level, smallest, largest) in [(l0, "a", "c"), (l0, "f", "h"), (l1, "a", "z")] {
            let id = manifest.allocate_table_id();
            manifest.add_table(table(id, level, smallest, largest))?;
        }
        manifest.advance_manifest_seq();

        let encoded = manifest.encode_file()?;
        let decoded = Manifest::decode_file_from_slot(&encoded, manifest.manifest_slot())?;
        assert_eq!(manifest, decoded);

        // Structure preserved: two levels, two tables on level 0, one on level 1.
        assert_eq!(decoded.levels().as_map().len(), 2);
        assert_eq!(decoded.levels().level(l0).unwrap().tables().len(), 2);
        assert_eq!(decoded.levels().level(l1).unwrap().tables().len(), 1);
        Ok(())
    }

    #[test]
    fn test_manifest_user_data_round_trip() -> Result<(), io::Error> {
        let mut manifest = Manifest::new_with_user_data("metadata".to_string());
        add_generated_table(&mut manifest)?;
        manifest.advance_manifest_seq();

        let encoded = manifest.encode_file()?;
        let decoded =
            Manifest::<String>::decode_file_from_slot(&encoded, manifest.manifest_slot())?;

        assert_eq!(manifest, decoded);
        assert_eq!(decoded.user_data(), "metadata");
        Ok(())
    }

    #[test]
    fn test_manifest_file_rejects_wrong_slot() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        add_generated_table(&mut manifest)?;
        manifest.advance_manifest_seq();

        let encoded = manifest.encode_file()?;
        let err = Manifest::<()>::decode_file_from_slot(&encoded, 0).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("manifest slot mismatch"));
        Ok(())
    }

    #[test]
    fn test_decode_rejects_trailing_bytes() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        add_generated_table(&mut manifest)?;
        manifest.advance_manifest_seq();

        let mut encoded = manifest.encode_file()?;
        encoded.push(0);

        let err =
            Manifest::<()>::decode_file_from_slot(&encoded, manifest.manifest_slot()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("trailing bytes"));
        Ok(())
    }

    /// The inner frames carry no checksum (F1); corruption inside the compressed
    /// levels payload must still be caught by the outer `Manifest` checksum.
    #[test]
    fn test_manifest_payload_corruption_is_detected() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        add_generated_table(&mut manifest)?;
        manifest.advance_manifest_seq();
        let encoded = manifest.encode_file()?;

        // Flip a byte inside the compressed payload, before the 8-byte trailing
        // checksum (so it is not the header and not the checksum itself). Decode
        // must fail — either the zstd stream no longer decodes, or the outer
        // checksum mismatches.
        let mut corrupt = encoded.clone();
        let idx = corrupt.len() - 12;
        corrupt[idx] ^= 0xff;

        assert!(Manifest::<()>::decode_file_from_slot(&corrupt, manifest.manifest_slot()).is_err());
        Ok(())
    }

    #[test]
    fn test_select_latest_valid_manifest() -> Result<(), io::Error> {
        let mut first = Manifest::new();
        add_generated_table(&mut first)?;
        first.advance_manifest_seq();

        let mut second = first.clone();
        add_generated_table(&mut second)?;
        second.advance_manifest_seq();

        let mut invalid = second.encode_file()?;
        invalid[0] ^= 1;
        let first_bytes = first.encode_file()?;
        let second_bytes = second.encode_file()?;

        let latest = Manifest::select_latest_valid([
            (first.manifest_slot(), first_bytes.as_slice()),
            (second.manifest_slot(), invalid.as_slice()),
            (second.manifest_slot(), second_bytes.as_slice()),
        ])?
        .unwrap();

        assert_eq!(latest, second);
        Ok(())
    }

    #[test]
    fn test_select_latest_no_slot_is_fresh_db() -> Result<(), io::Error> {
        assert_eq!(Manifest::<()>::select_latest_valid([])?, None);
        Ok(())
    }

    #[test]
    fn test_select_latest_all_invalid_is_error() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        add_generated_table(&mut manifest)?;
        manifest.advance_manifest_seq();

        let mut corrupt = manifest.encode_file()?;
        corrupt[0] ^= 1;

        let err =
            Manifest::<()>::select_latest_valid([(manifest.manifest_slot(), corrupt.as_slice())])
                .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("refusing to start empty"));
        Ok(())
    }

    #[test]
    fn test_select_latest_valid_rejects_out_of_range_slot() -> Result<(), io::Error> {
        let mut manifest = Manifest::new();
        add_generated_table(&mut manifest)?;
        manifest.advance_manifest_seq();
        let encoded = manifest.encode_file()?;

        let err = Manifest::<()>::select_latest_valid([(MANIFEST_SLOT_COUNT, encoded.as_slice())])
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("out of range"));
        Ok(())
    }

    fn to_hex(bytes: &[u8]) -> String {
        use std::fmt::Write as _;
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            write!(s, "{b:02x}").unwrap();
        }
        s
    }

    fn from_hex(hex: &str) -> Vec<u8> {
        (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect()
    }

    /// Golden: pins the exact V001 manifest encoding so silent format drift is
    /// caught. `zstd` is pinned in `Cargo.toml` to keep these bytes reproducible;
    /// regenerate the constant deliberately if the format is changed on purpose.
    #[test]
    fn test_manifest_encoding_golden() -> Result<(), io::Error> {
        const GOLDEN_HEX: &str = "6d667374000000000000000000000001000000002730a58000000000000000\
            01000000010000000100000000000000016c6576656c73000000000000000000010000\
            00003f8f5a21000000000000003e0000004028b52ffd0048bd0100d40200000100000000\
            6c766c5f6d66737400010000000017064a4d000100000003616161000000037a7a7a0000\
            00000310000319756101000000006afbf7b0";
        let golden: String = GOLDEN_HEX.split_whitespace().collect();

        let mut manifest = Manifest::new();
        let level = manifest.allocate_level();
        let table_id = manifest.allocate_table_id();
        manifest.allocate_key_seq();
        manifest.add_table(table(table_id, level, "aaa", "zzz"))?;
        manifest.advance_manifest_seq();

        let encoded = manifest.encode_file()?;
        assert_eq!(to_hex(&encoded), golden, "manifest V001 encoding drifted");

        // The golden bytes still decode back to the same manifest.
        let decoded =
            Manifest::<()>::decode_file_from_slot(&from_hex(&golden), manifest.manifest_slot())?;
        assert_eq!(manifest, decoded);
        Ok(())
    }
}
