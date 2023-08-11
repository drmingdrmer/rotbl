use std::io::Error;
use std::io::Read;
use std::io::Write;
use std::sync::Arc;

use crate::codec::Codec;
use crate::typ::Type;
use crate::v00x::var_len_array::payload::RawVLArrayPayload;
use crate::version::Version;

/// A list of packed entries of variable length raw bytes.
#[derive(Debug)]
#[derive(PartialEq, Eq)]
pub struct RawVLArray {
    /// The version of this data.
    pub(crate) version: Version,

    pub(crate) payload: Arc<RawVLArrayPayload>,
}

impl RawVLArray {
    #[allow(dead_code)]
    pub fn get(&self, i: usize) -> Option<&[u8]> {
        let l = self.len();
        if i >= l {
            return None;
        }

        Some(self.payload.get_unchecked(i))
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }
}

impl Codec for RawVLArray {
    const ENCODED_SIZE: u64 = 0;

    fn encode<W: Write>(&self, mut w: W) -> Result<usize, Error> {
        let mut n = 0;

        let t = Type::VLArray;
        n += t.encode(&mut w)?;
        n += self.version.encode(&mut w)?;
        n += self.payload.encode(&mut w)?;

        Ok(n)
    }

    fn decode<R: Read>(mut r: R) -> Result<Self, Error> {
        let t = Type::decode(&mut r)?;
        if t != Type::VLArray {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid type: {}; expect: {}", t, Type::VLArray),
            ));
        }

        let version = Version::decode(&mut r)?;
        if version != Version::V001 {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid version: {}; expect: {}", version, Version::V001),
            ));
        }

        let payload = RawVLArrayPayload::decode(&mut r)?;

        Ok(Self {
            version,
            payload: Arc::new(payload),
        })
    }
}
