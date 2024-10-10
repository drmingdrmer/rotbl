pub(crate) mod fixed_size;

use std::io;

use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

use self::fixed_size::FixedSize;

pub(crate) trait Codec: Sized {
    fn encode<W: io::Write>(&self, w: W) -> Result<usize, io::Error>;
    fn decode<R: io::Read>(r: R) -> Result<Self, io::Error>;
}

impl Codec for u64 {
    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        w.write_u64::<byteorder::BigEndian>(*self)?;
        Ok(Self::encoded_size())
    }

    fn decode<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
        let v = r.read_u64::<byteorder::BigEndian>()?;
        Ok(v)
    }
}

impl Codec for u32 {
    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        w.write_u32::<byteorder::BigEndian>(*self)?;
        Ok(Self::encoded_size())
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

    use crate::codec::fixed_size::FixedSize;
    use crate::codec::Codec;

    #[test]
    fn test_u64_codec() -> anyhow::Result<()> {
        test_int_coded(0x1234567890abcdefu64)
    }

    #[test]
    fn test_u32_codec() -> anyhow::Result<()> {
        test_int_coded(0x12345678u32)
    }

    fn test_int_coded<T: Codec + FixedSize + PartialEq + Debug>(v: T) -> anyhow::Result<()> {
        let size = size_of::<T>();

        assert_eq!(T::encoded_size(), size);

        let mut buf = Vec::new();
        let n = v.encode(&mut buf)?;
        assert_eq!(n, buf.len());

        let b = T::decode(&mut buf.as_slice())?;
        assert_eq!(v, b);

        Ok(())
    }
}
