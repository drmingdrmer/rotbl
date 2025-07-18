use std::fmt;
use std::io;
use std::io::Read;
use std::io::Write;

use codeq::config::CodeqConfig;
use codeq::FixedSize;

use crate::typ::Type;
use crate::v001::types::Checksum;
use crate::version::Version;

#[derive(Debug)]
#[derive(Clone, Copy)]
#[derive(PartialEq, Eq)]
pub struct Header {
    typ: Type,
    version: Version,
}

impl Header {
    pub fn new(typ: Type, version: Version) -> Self {
        Self { typ, version }
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{typ: {:?}, version: {:?}}}", self.typ, self.version)
    }
}

impl FixedSize for Header {
    fn encoded_size() -> usize {
        Type::encoded_size() + Version::encoded_size() + 8
    }
}

impl codeq::Encode for Header {
    fn encode<W: Write>(&self, w: W) -> Result<usize, io::Error> {
        let mut n = 0;
        let mut hw = Checksum::new_writer(w);

        n += self.typ.encode(&mut hw)?;
        n += self.version.encode(&mut hw)?;

        n += hw.write_checksum()?;
        Ok(n)
    }
}

impl codeq::Decode for Header {
    fn decode<R: Read>(mut r: R) -> Result<Self, io::Error> {
        let mut cr = Checksum::new_reader(&mut r);

        let t = Type::decode(&mut cr)?;
        let version = Version::decode(&mut cr)?;
        cr.verify_checksum(|| "Header::decode()")?;

        Ok(Self { typ: t, version })
    }
}

#[cfg(test)]
mod tests {
    use codeq::testing::test_codec;
    use codeq::Encode;

    use crate::typ::Type;
    use crate::v001::header::Header;
    use crate::version::Version;

    #[test]
    fn test_header_codec() -> anyhow::Result<()> {
        let h = Header::new(Type::Rotbl, Version::V001);
        let mut b = Vec::new();
        let n = h.encode(&mut b)?;
        assert_eq!(n, 24);

        assert_eq!(b.len(), 24);
        assert_eq!(b, vec![
            114, 111, 116, 98, 108, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 1, //
            0, 0, 0, 0, // padding
            101, 248, 25, 5 // checksum
        ]);

        test_codec(b.as_slice(), &h)?;

        Ok(())
    }
}
