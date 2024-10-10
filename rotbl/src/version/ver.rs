use std::fmt;
use std::io;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use codeq::Codec;
use codeq::FixedSize;

#[derive(Debug)]
#[derive(Clone, Copy)]
#[derive(PartialEq, Eq)]
pub enum Version {
    V001,
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Version {
    pub fn as_u64(&self) -> u64 {
        match self {
            Version::V001 => 1,
        }
    }

    pub fn from_u64(v: u64) -> Result<Self, u64> {
        match v {
            1 => Ok(Version::V001),
            _ => Err(v),
        }
    }
}

impl FixedSize for Version {
    fn encoded_size() -> usize {
        8
    }
}

impl Codec for Version {
    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        w.write_u64::<BigEndian>(self.as_u64())?;
        Ok(Self::encoded_size())
    }

    fn decode<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
        let ver = r.read_u64::<BigEndian>()?;
        Self::from_u64(ver).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid version:{}", ver),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use codeq::testing::test_codec;
    use codeq::Codec;

    use crate::version::Version;

    #[test]
    fn test_version_codec() -> anyhow::Result<()> {
        let v = Version::V001;
        let mut b = Vec::new();
        let n = v.encode(&mut b)?;
        assert_eq!(n, b.len());

        test_codec(&b, &v)?;

        Ok(())
    }
}
