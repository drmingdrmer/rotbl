use std::io;
use std::mem::size_of;

use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

pub(crate) trait Codec: Sized {
    /// The size of the encoded data if Self is fix sized.
    const ENCODED_SIZE: u64;

    fn encode<W: io::Write>(&self, w: W) -> Result<usize, io::Error>;
    fn decode<R: io::Read>(r: R) -> Result<Self, io::Error>;
}

impl Codec for u64 {
    const ENCODED_SIZE: u64 = size_of::<Self>() as u64;

    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        w.write_u64::<byteorder::BigEndian>(*self)?;
        Ok(Self::ENCODED_SIZE as usize)
    }

    fn decode<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
        let v = r.read_u64::<byteorder::BigEndian>()?;
        Ok(v)
    }
}

impl Codec for u32 {
    const ENCODED_SIZE: u64 = size_of::<Self>() as u64;

    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        w.write_u32::<byteorder::BigEndian>(*self)?;
        Ok(Self::ENCODED_SIZE as usize)
    }

    fn decode<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
        let v = r.read_u32::<byteorder::BigEndian>()?;
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::mem::size_of;

    use crate::codec::Codec;

    #[test]
    fn test_u64_codec() -> anyhow::Result<()> {
        test_int_coded(0x1234567890abcdefu64)
    }

    #[test]
    fn test_u32_codec() -> anyhow::Result<()> {
        test_int_coded(0x12345678u32)
    }

    fn test_int_coded<T: Codec + PartialEq + Debug>(v: T) -> anyhow::Result<()> {
        let size = size_of::<T>();

        assert_eq!(T::ENCODED_SIZE, size as u64);

        let mut buf = Vec::new();
        let n = v.encode(&mut buf)?;
        assert_eq!(n, buf.len());

        let b = T::decode(&mut buf.as_slice())?;
        assert_eq!(v, b);

        Ok(())
    }
}
