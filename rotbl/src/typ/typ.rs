use std::fmt;
use std::io;
use std::io::Error;
use std::io::Read;
use std::io::Write;

use crate::codec::Codec;

const VL_ARRAY: [u8; 8] = *b"vla\0\0\0\0\0";
const ROTBL: [u8; 8] = *b"rotbl\0\0\0";
const ROTBL_META: [u8; 8] = *b"rotbl_m\0";
const BLOCK: [u8; 8] = *b"blk\0\0\0\0\0";
const BLOCK_INDEX: [u8; 8] = *b"blk_idx\0";

#[derive(Debug)]
#[derive(Clone, Copy)]
#[derive(PartialEq, Eq)]
pub enum Type {
    VLArray,
    Rotbl,
    RotblMeta,
    Block,
    BlockIndex,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Codec for Type {
    const ENCODED_SIZE: u64 = 8;

    fn encode<W: Write>(&self, mut w: W) -> Result<usize, Error> {
        let b = match self {
            Type::VLArray => &VL_ARRAY,
            Type::Rotbl => &ROTBL,
            Type::RotblMeta => &ROTBL_META,
            Type::Block => &BLOCK,
            Type::BlockIndex => &BLOCK_INDEX,
        };
        w.write_all(b)?;

        Ok(b.len())
    }

    fn decode<R: Read>(mut r: R) -> Result<Self, Error> {
        let mut buf = [0u8; 8];
        r.read_exact(&mut buf)?;

        match buf {
            VL_ARRAY => Ok(Type::VLArray),
            ROTBL => Ok(Type::Rotbl),
            ROTBL_META => Ok(Type::RotblMeta),
            BLOCK => Ok(Type::Block),
            BLOCK_INDEX => Ok(Type::BlockIndex),
            _ => Err(Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid type: {:?}", buf),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::codec::Codec;
    use crate::typ::typ::BLOCK;
    use crate::typ::typ::BLOCK_INDEX;
    use crate::typ::typ::ROTBL;
    use crate::typ::typ::ROTBL_META;
    use crate::typ::typ::VL_ARRAY;
    use crate::typ::Type;

    #[test]
    fn test_type_codec() -> anyhow::Result<()> {
        {
            let mut b = Vec::new();
            let n = Type::VLArray.encode(&mut b)?;
            assert_eq!(n, 8);
            assert_eq!(b, VL_ARRAY);
            assert_eq!(Type::decode(&mut b.as_slice())?, Type::VLArray);
        }

        {
            let mut b = Vec::new();
            let n = Type::Rotbl.encode(&mut b)?;
            assert_eq!(n, 8);
            assert_eq!(b, ROTBL);
            assert_eq!(Type::decode(&mut b.as_slice())?, Type::Rotbl);
        }

        {
            let mut b = Vec::new();
            let n = Type::RotblMeta.encode(&mut b)?;
            assert_eq!(n, 8);
            assert_eq!(b, ROTBL_META);
            assert_eq!(Type::decode(&mut b.as_slice())?, Type::RotblMeta);
        }

        {
            let mut b = Vec::new();
            let n = Type::Block.encode(&mut b)?;
            assert_eq!(n, 8);
            assert_eq!(b, BLOCK);
            assert_eq!(Type::decode(&mut b.as_slice())?, Type::Block);
        }

        {
            let mut b = Vec::new();
            let n = Type::BlockIndex.encode(&mut b)?;
            assert_eq!(n, 8);
            assert_eq!(b, BLOCK_INDEX);
            assert_eq!(Type::decode(&mut b.as_slice())?, Type::BlockIndex);
        }
        Ok(())
    }
}
