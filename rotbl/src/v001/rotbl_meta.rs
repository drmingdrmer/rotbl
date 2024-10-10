use std::fmt;
use std::io::Error;
use std::io::Read;
use std::io::Write;

use crate::buf;
use crate::codec::Codec;
use crate::typ::Type;
use crate::v001::checksum_reader::ChecksumReader;
use crate::v001::checksum_writer::ChecksumWriter;
use crate::v001::header::Header;
use crate::v001::rotbl_meta_payload::RotblMetaPayload;
use crate::v001::with_checksum::WithChecksum;
use crate::version::Version;

#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
pub struct RotblMeta {
    header: Header,

    payload: RotblMetaPayload,
}

impl RotblMeta {
    pub fn new(seq: u64, user_data: impl ToString) -> Self {
        let header = Header::new(Type::RotblMeta, Version::V001);
        let meta = RotblMetaPayload::new(seq, user_data);
        Self {
            header,
            payload: meta,
        }
    }

    pub fn seq(&self) -> u64 {
        self.payload.seq
    }

    pub fn user_data(&self) -> &str {
        &self.payload.user_data
    }
}

impl fmt::Display for RotblMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{header: {}, payload: {}}}", self.header, self.payload)
    }
}

impl Codec for RotblMeta {
    fn encode<W: Write>(&self, mut w: W) -> Result<usize, Error> {
        let mut n = 0usize;

        let mut cw = ChecksumWriter::new(&mut w);

        n += self.header.encode(&mut cw)?;

        let encoded_data = serde_json::to_vec(&self.payload)?;
        let encoded_size = encoded_data.len() as u64;

        let s = WithChecksum::new(encoded_size);
        n += s.encode(&mut cw)?;

        cw.write_all(&encoded_data)?;
        n += encoded_size as usize;

        n += cw.write_checksum()?;

        Ok(n)
    }

    fn decode<R: Read>(r: R) -> Result<Self, Error> {
        let mut cr = ChecksumReader::new(r);

        let header = Header::decode(&mut cr)?;
        assert_eq!(header, Header::new(Type::RotblMeta, Version::V001));

        let payload_size = WithChecksum::<u64>::decode(&mut cr)?.into_inner();

        let mut buf = buf::new_uninitialized(payload_size as usize);
        cr.read_exact(&mut buf)?;

        cr.verify_checksum(|| "RotblMeta::decode()")?;

        let data = serde_json::from_slice(&buf)?;

        let block = Self {
            header,
            payload: data,
        };

        Ok(block)
    }
}

#[cfg(test)]
#[allow(clippy::redundant_clone)]
mod tests {

    #[allow(unused_imports)]
    use pretty_assertions::assert_eq;

    use crate::v001::rotbl_meta::RotblMeta;
    use crate::v001::testing::bbs;
    use crate::v001::testing::test_codec;
    use crate::v001::testing::vec_chain;

    #[test]
    fn test_rotbl_meta_codec() -> anyhow::Result<()> {
        let user_data = "hello";
        let rotbl_meta = RotblMeta::new(5, user_data);

        let encoded = vec_chain([
            bbs(["rotbl_m\0"]), // header.type
            vec![
                0, 0, 0, 0, 0, 0, 0, 1, // header.version
                0, 0, 0, 0, 170, 167, 191, 39, // header checksum
                0, 0, 0, 0, 0, 0, 0, 29, // data_encoded_size
                0, 0, 0, 0, 6, 36, 179, 176, // data_encoded_size checksum
            ],
            bbs([r#"{"seq":5,"user_data":"hello"}"#]), // data
            vec![
                0, 0, 0, 0, 230, 11, 170, 59, // checksum
            ],
        ]);

        test_codec(&encoded, &rotbl_meta)?;

        Ok(())
    }
}
