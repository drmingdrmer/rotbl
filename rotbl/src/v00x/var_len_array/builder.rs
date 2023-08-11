use std::sync::Arc;

use crate::v00x::var_len_array::array::RawVLArray;
use crate::v00x::var_len_array::payload::RawVLArrayPayload;
use crate::version::Version;

#[allow(dead_code)]
pub struct RawVLArrayBuilder {
    cnt: usize,
    avg_len: usize,
}

impl RawVLArrayBuilder {
    #[allow(dead_code)]
    pub fn new(count_hint: Option<usize>) -> Self {
        Self {
            cnt: count_hint.unwrap_or(32),
            avg_len: 32,
        }
    }

    #[allow(dead_code)]
    pub fn build(self, entries: impl IntoIterator<Item = impl AsRef<[u8]>>) -> RawVLArray {
        let mut offsets = Vec::with_capacity(self.cnt + 1);
        let mut bytes = bytes::BytesMut::with_capacity(self.cnt * self.avg_len);

        for ent in entries {
            offsets.push(bytes.len() as u32);
            bytes.extend_from_slice(ent.as_ref());

            assert!(bytes.len() < u32::MAX as usize);
        }
        offsets.push(bytes.len() as u32);

        RawVLArray {
            version: Version::V001,
            payload: Arc::new(RawVLArrayPayload {
                offsets,
                entries: bytes.freeze(),
            }),
        }
    }
}
