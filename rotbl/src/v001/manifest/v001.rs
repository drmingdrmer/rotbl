use std::io;
use std::io::Read;
use std::io::Write;

use codeq::config::CodeqConfig;
use codeq::ChecksumReader;
use codeq::Decode;
use codeq::Encode;

use super::Levels;
use super::Manifest;
use crate::v001::types::Checksum;

impl<UserData> Manifest<UserData>
where UserData: Encode + Decode
{
    pub(super) fn decode_v001<R: Read>(
        mut cr: ChecksumReader<Checksum, R>,
    ) -> Result<Self, io::Error> {
        let manifest_seq = u64::decode(&mut cr)?;
        let next_table_id = u32::decode(&mut cr)?;
        let next_level = u32::decode(&mut cr)?;
        let last_key_seq = u64::decode(&mut cr)?;
        let user_data = UserData::decode(&mut cr)?;
        let levels = Levels::decode(&mut cr)?;
        cr.verify_checksum(|| "Manifest::decode_file()")?;

        let manifest = Self {
            levels,
            next_table_id,
            next_level,
            last_key_seq,
            user_data,
            manifest_seq,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub(super) fn encode_v001<W: Write>(&self, mut w: W) -> Result<usize, io::Error> {
        self.validate()?;

        let mut cw = Checksum::new_writer(&mut w);
        self.header().encode(&mut cw)?;
        self.manifest_seq.encode(&mut cw)?;
        self.next_table_id.encode(&mut cw)?;
        self.next_level.encode(&mut cw)?;
        self.last_key_seq.encode(&mut cw)?;
        self.user_data.encode(&mut cw)?;
        self.levels.encode(&mut cw)?;
        cw.finalize()
    }
}
