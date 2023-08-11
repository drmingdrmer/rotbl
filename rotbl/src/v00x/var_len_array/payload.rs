use std::io;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

use crate::buf;
use crate::codec::Codec;

#[derive(Debug)]
#[derive(PartialEq, Eq)]
pub(crate) struct RawVLArrayPayload {
    /// `cnt+1` offsets to the start of each entry.
    ///
    /// `offsets[0]` is always 0, and `offsets[cnt]` is the total size of this list.
    pub(crate) offsets: Vec<u32>,

    /// Packed `cnt` raw bytes entries.
    pub(crate) entries: bytes::Bytes,
}

impl RawVLArrayPayload {
    #[allow(dead_code)]
    pub(crate) fn get_unchecked(&self, i: usize) -> &[u8] {
        let start = self.offsets[i] as usize;
        let end = self.offsets[i + 1] as usize;
        &self.entries[start..end]
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }
}

impl Codec for RawVLArrayPayload {
    const ENCODED_SIZE: u64 = 0;

    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        let mut n = 0;

        let len = self.len();

        // offsets

        // Note that len == offset.len() - 1
        w.write_u64::<BigEndian>(len as u64)?;
        n += 8;
        for offset in &self.offsets {
            w.write_u32::<BigEndian>(*offset)?;
            n += 4;
        }

        // packed entries

        w.write_all(&self.entries)?;
        n += self.entries.len();

        Ok(n)
    }

    fn decode<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
        let len = r.read_u64::<BigEndian>()? as usize;

        let mut offsets = Vec::with_capacity(len + 1);
        for _ in 0..(len + 1) {
            offsets.push(r.read_u32::<BigEndian>()?);
        }

        let size = offsets[len] as usize;
        let mut entries = buf::new_uninitialized(size);

        r.read_exact(&mut entries)?;

        Ok(Self {
            offsets,
            entries: entries.into(),
        })
    }
}
