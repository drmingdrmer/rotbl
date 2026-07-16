use std::fmt;
use std::io;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use codeq::FixedSize;

#[derive(Debug)]
#[derive(Clone, Copy)]
#[derive(PartialEq, Eq)]
pub enum Version {
    V001,
    V002,
    V003,
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
            Version::V002 => 2,
            Version::V003 => 3,
        }
    }

    pub fn from_u64(v: u64) -> Result<Self, u64> {
        match v {
            1 => Ok(Version::V001),
            2 => Ok(Version::V002),
            3 => Ok(Version::V003),
            _ => Err(v),
        }
    }
}

impl FixedSize for Version {
    fn encoded_size() -> usize {
        8
    }
}

impl codeq::Encode for Version {
    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        w.write_u64::<BigEndian>(self.as_u64())?;
        Ok(Self::encoded_size())
    }
}

impl codeq::Decode for Version {
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
    use codeq::Decode;
    use codeq::Encode;

    use crate::version::Version;

    #[test]
    fn test_version_codec() -> anyhow::Result<()> {
        // V003 fully round-trips under `test_codec`: its corruption sweep does +1
        // per byte, and the value byte 3→4 is not a valid version, so decode fails
        // as the sweep requires.
        test_codec(&[0, 0, 0, 0, 0, 0, 0, 3], &Version::V003)?;

        // V001 cannot use `test_codec`: the same +1 sweep turns its value byte 1→2,
        // which decodes as a valid version. Version integrity is
        // instead guarded by the enclosing Header checksum, so a plain round-trip
        // is all this test asserts for V001.
        let mut b = Vec::new();
        let n = Version::V001.encode(&mut b)?;
        assert_eq!(n, b.len());
        assert_eq!(b, [0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(Version::decode(&b[..])?, Version::V001);

        Ok(())
    }
}
