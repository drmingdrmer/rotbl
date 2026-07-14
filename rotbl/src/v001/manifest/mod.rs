use std::io;

mod level_manifest;
mod levels;
// Each file is named after its primary struct; this one holds `Manifest`.
#[allow(clippy::module_inception)]
mod manifest;
mod table_info;
mod table_record;
mod v001;

pub use level_manifest::LevelManifest;
pub use levels::Levels;
pub use manifest::Manifest;
pub use table_info::TableInfo;
pub use table_record::TableRecord;

pub(crate) use crate::err::invalid_data;
pub(crate) use crate::err::invalid_input;

/// Advisory warning threshold for the complete encoded manifest file. Exceeding
/// it logs a warning; it is never a hard failure (see `manifest-design.md`).
pub const MANIFEST_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Number of fixed manifest slot files. A committed manifest with sequence `s`
/// is written to slot `s % MANIFEST_SLOT_COUNT`; four slots keep three intact
/// committed manifests at all times (see `manifest-design.md`).
pub const MANIFEST_SLOT_COUNT: u8 = 4;

/// The slot a manifest with `manifest_seq` is written to.
///
/// Single source of truth for the `manifest_seq % MANIFEST_SLOT_COUNT` mapping,
/// shared by [`Manifest::manifest_slot`] and [`validate_manifest_slot`].
pub(crate) fn slot_for_manifest_seq(manifest_seq: u64) -> u8 {
    (manifest_seq % u64::from(MANIFEST_SLOT_COUNT)) as u8
}

pub(crate) fn validate_manifest_slot(slot: u8, manifest_seq: u64) -> Result<(), io::Error> {
    validate_slot_id(slot)?;
    let expected = slot_for_manifest_seq(manifest_seq);
    if slot != expected {
        return Err(invalid_data(format!(
            "manifest slot mismatch: slot {}, manifest_seq {} implies slot {}",
            slot, manifest_seq, expected
        )));
    }
    Ok(())
}

pub(crate) fn validate_slot_id(slot: u8) -> Result<(), io::Error> {
    if slot >= MANIFEST_SLOT_COUNT {
        return Err(invalid_input(format!(
            "manifest slot {} is out of range",
            slot
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;

    use codeq::Decode;
    use codeq::Encode;
    use codeq::FixedSize;

    use super::*;
    use crate::typ::Type;
    use crate::v001::header::Header;
    use crate::version::Version;

    fn table(table_id: u32, level: u32, smallest: &str, largest: &str) -> TableInfo {
        TableInfo::new(
            level,
            Arc::new(TableRecord::new(table_id, smallest, largest)),
        )
    }

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
    fn test_levels_round_trip() -> Result<(), io::Error> {
        let mut levels = Levels::default();
        levels.insert(table(7, 3, "a", "z"))?;

        let mut encoded = Vec::new();
        levels.encode(&mut encoded)?;
        let decoded = Levels::decode(encoded.as_slice())?;

        assert_eq!(levels, decoded);
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
    fn test_headers() {
        assert_eq!(
            Manifest::new().header(),
            Header::new(Type::ManifestFile, Version::V001)
        );
        assert_eq!(
            Levels::default().header(),
            Header::new(Type::Levels, Version::V001)
        );
        assert_eq!(
            LevelManifest::default().header(),
            Header::new(Type::LevelManifest, Version::V001)
        );
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

    #[test]
    fn test_levels_decode_rejects_uncompressed_len_mismatch() -> Result<(), io::Error> {
        let mut levels = Levels::default();
        levels.insert(table(7, 3, "a", "z"))?;
        let mut encoded = Vec::new();
        levels.encode(&mut encoded)?;

        // `uncompressed_len` is the `u64` right after the `Levels` header.
        let off = Header::encoded_size();

        let mut too_large = encoded.clone();
        too_large[off..off + 8].copy_from_slice(&9_999u64.to_be_bytes());
        let err = Levels::decode(too_large.as_slice()).unwrap_err();
        assert!(err.to_string().contains("expected 9999"), "{err}");

        let mut too_small = encoded.clone();
        too_small[off..off + 8].copy_from_slice(&1u64.to_be_bytes());
        let err = Levels::decode(too_small.as_slice()).unwrap_err();
        assert!(err.to_string().contains("expected 1"), "{err}");
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

    #[test]
    fn test_levels_decode_rejects_duplicate_level() -> Result<(), io::Error> {
        let mut level_manifest = LevelManifest::default();
        level_manifest.insert(table(7, 3, "a", "z"))?;

        // Hand-build an uncompressed payload with two entries for the same level.
        let mut raw = Vec::new();
        2u64.encode(&mut raw)?;
        3u32.encode(&mut raw)?;
        level_manifest.encode(&mut raw)?;
        3u32.encode(&mut raw)?;
        level_manifest.encode(&mut raw)?;

        let payload = zstd::encode_all(raw.as_slice(), 1)?;
        let mut framed = Vec::new();
        Header::new(Type::Levels, Version::V001).encode(&mut framed)?;
        (raw.len() as u64).encode(&mut framed)?;
        payload.encode(&mut framed)?;

        let err = Levels::decode(framed.as_slice()).unwrap_err();
        assert!(
            err.to_string().contains("duplicate manifest level 3"),
            "{err}"
        );
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
}
