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
    use super::*;

    /// A manifest_seq maps to `seq % MANIFEST_SLOT_COUNT`, so the four slots
    /// cycle 0,1,2,3,0,...
    #[test]
    fn test_slot_for_manifest_seq_wraps_by_slot_count() {
        assert_eq!(MANIFEST_SLOT_COUNT, 4);
        let slots: Vec<u8> = (0..6).map(slot_for_manifest_seq).collect();
        assert_eq!(slots, vec![0, 1, 2, 3, 0, 1]);
    }

    #[test]
    fn test_validate_slot_id_rejects_out_of_range() {
        for slot in 0..MANIFEST_SLOT_COUNT {
            validate_slot_id(slot).unwrap();
        }
        let err = validate_slot_id(MANIFEST_SLOT_COUNT).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("out of range"));
    }

    #[test]
    fn test_validate_manifest_slot_matches_seq_mapping() {
        // seq 5 -> slot 1 is the only slot accepted for that seq.
        validate_manifest_slot(1, 5).unwrap();

        let err = validate_manifest_slot(0, 5).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("manifest slot mismatch"));

        // An out-of-range slot is rejected before the seq mapping is checked.
        let err = validate_manifest_slot(MANIFEST_SLOT_COUNT, 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("out of range"));
    }
}
