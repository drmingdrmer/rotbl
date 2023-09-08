use std::io::Error;
use std::io::Read;
use std::io::Write;

use crate::buf;
use crate::codec::Codec;
use crate::typ::Type;
use crate::v001::checksum_reader::ChecksumReader;
use crate::v001::checksum_writer::ChecksumWriter;
use crate::v001::header::Header;
use crate::v001::with_checksum::WithChecksum;
use crate::version::Version;

#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RotblMetaPayload {
    seq: u64,
    user_data: String,
}

impl RotblMetaPayload {
    pub fn new(seq: u64, user_data: impl ToString) -> Self {
        Self {
            seq,
            user_data: user_data.to_string(),
        }
    }
}

#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
pub struct RotblMeta {
    header: Header,

    /// The size of the encoded `data` part of a block.
    pub(crate) data_encoded_size: u64,

    payload: RotblMetaPayload,
}

impl RotblMeta {
    pub fn new(seq: u64, user_data: impl ToString) -> Self {
        let header = Header::new(Type::RotblMeta, Version::V001);
        let meta = RotblMetaPayload::new(seq, user_data);
        Self {
            header,
            data_encoded_size: 0,
            payload: meta,
        }
    }

    pub fn data_encoded_size(&self) -> u64 {
        self.data_encoded_size
    }

    pub fn seq(&self) -> u64 {
        self.payload.seq
    }

    pub fn user_data(&self) -> &str {
        &self.payload.user_data
    }
}

impl Codec for RotblMeta {
    // Variable sized block.
    const ENCODED_SIZE: u64 = 0;

    fn encode<W: Write>(&self, mut w: W) -> Result<usize, Error> {
        let mut n = 0usize;
        let encoded_data = serde_json::to_vec(&self.payload)?;
        let encoded_size = encoded_data.len() as u64;

        let mut cw = ChecksumWriter::new(&mut w);

        n += self.header.encode(&mut cw)?;

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

        let data_size = WithChecksum::<u64>::decode(&mut cr)?.into_inner();

        let mut buf = buf::new_uninitialized(data_size as usize);
        cr.read_exact(&mut buf)?;
        cr.verify_checksum()?;

        let data = serde_json::from_slice(&buf)?;

        let block = Self {
            header,
            data_encoded_size: data_size,
            payload: data,
        };

        Ok(block)
    }
}

#[cfg(test)]
#[allow(clippy::redundant_clone)]
mod tests {

    use pretty_assertions::assert_eq;

    use crate::codec::Codec;
    use crate::v001::rotbl_meta::RotblMeta;
    use crate::v001::testing::bbs;
    use crate::v001::testing::test_codec;
    use crate::v001::testing::vec_chain;

    #[test]
    fn test_rotbl_meta_codec() -> anyhow::Result<()> {
        let user_data = "hello";
        let mut rotbl_meta = RotblMeta::new(5, user_data);

        let encoded_payload = serde_json::to_vec(&rotbl_meta.payload)?;
        let encoded_size = encoded_payload.len() as u64;

        let mut b = Vec::new();
        let n = rotbl_meta.encode(&mut b)?;
        assert_eq!(n, b.len());
        println!("b = {:?}", b);

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
        assert_eq!(encoded, b);

        // Block does not know about the encoded size when it is created.
        rotbl_meta.data_encoded_size = encoded_size;

        test_codec(&b[..], &rotbl_meta)?;

        Ok(())
    }
}
