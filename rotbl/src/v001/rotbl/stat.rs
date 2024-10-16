use std::fmt;
use std::io::Error;
use std::io::Read;
use std::io::Write;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use codeq::ChecksumReader;
use codeq::ChecksumWriter;

use crate::buf::new_uninitialized;
use crate::num::format_num;

/// Stats about a [`Rotbl`] instance.
///
/// [`Rotbl`]: `crate::v001::rotbl::Rotbl`
#[derive(Debug, Clone)]
#[derive(Default)]
#[derive(PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RotblStat {
    /// Total number of blocks.
    pub block_num: u32,

    /// Total number of keys.
    pub key_num: u64,

    /// Size of all user data(in blocks) in bytes.
    pub data_size: u64,

    /// Size of serialized block index in bytes.
    pub index_size: u64,
}

impl RotblStat {
    fn block_num(&self) -> u32 {
        self.block_num
    }

    fn key_num(&self) -> u64 {
        self.key_num
    }

    fn data_size(&self) -> u64 {
        self.data_size
    }

    fn index_size(&self) -> u64 {
        self.index_size
    }

    /// Average size in bytes of a block.
    fn block_avg_size(&self) -> u64 {
        if self.block_num == 0 {
            0
        } else {
            self.data_size / self.block_num as u64
        }
    }
}

impl fmt::Display for RotblStat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} keys in {} blocks: data({} B), index({} B), avg block size({} B)",
            format_num(self.key_num()),
            format_num(self.block_num() as u64),
            format_num(self.data_size()),
            format_num(self.index_size()),
            format_num(self.block_avg_size()),
        )
    }
}

impl codeq::Encode for RotblStat {
    fn encode<W: Write>(&self, mut w: W) -> Result<usize, Error> {
        let mut n = 0;

        let buf = serde_json::to_vec(self)?;
        let len = buf.len() as u64;

        {
            let mut cw = ChecksumWriter::new(&mut w);
            cw.write_u64::<BigEndian>(len)?;
            n += 8;

            n += cw.write_checksum()?;
        }

        let mut cw = ChecksumWriter::new(w);
        cw.write_all(&buf)?;
        n += len as usize;

        n += cw.write_checksum()?;

        Ok(n)
    }
}

impl codeq::Decode for RotblStat {
    fn decode<R: Read>(mut r: R) -> Result<Self, Error> {
        let len = {
            let mut cr = ChecksumReader::new(&mut r);
            let len = cr.read_u64::<BigEndian>()? as usize;
            cr.verify_checksum(|| "RotblStat::decode() for `len`")?;
            len
        };

        let mut cr = ChecksumReader::new(r);

        let mut buf = new_uninitialized(len);
        cr.read_exact(&mut buf)?;

        cr.verify_checksum(|| "RotblStat::decode() for payload")?;

        let stat: RotblStat = serde_json::from_slice(&buf)?;

        Ok(stat)
    }
}

#[cfg(test)]
mod tests {
    use codeq::testing::test_codec;

    use crate::v001::rotbl::stat::RotblStat;
    use crate::v001::testing::bbs;
    use crate::v001::testing::vec_chain;

    #[test]
    fn test_rotbl_stat_codec() -> anyhow::Result<()> {
        let stat = RotblStat {
            block_num: 5,
            key_num: 10,
            data_size: 100,
            index_size: 200,
        };
        println!("{}", serde_json::to_string(&stat)?);

        let b = vec_chain([
            vec![
                0, 0, 0, 0, 0, 0, 0, 61, // len
                0, 0, 0, 0, 61, 74, 147, 120, // checksum
            ],
            bbs([r#"{"block_num":5,"key_num":10,"data_size":100,"index_size":200}"#]), // data
            vec![
                0, 0, 0, 0, 138, 165, 51, 176, // checksum
            ],
        ]);

        test_codec(b.as_slice(), &stat)?;

        Ok(())
    }

    #[test]
    fn test_rotbl_stat_api() -> anyhow::Result<()> {
        let stat = RotblStat {
            block_num: 5,
            key_num: 10,
            data_size: 100,
            index_size: 200,
        };

        assert_eq!(stat.block_num(), 5);
        assert_eq!(stat.key_num(), 10);
        assert_eq!(stat.data_size(), 100);
        assert_eq!(stat.index_size(), 200);
        assert_eq!(stat.block_avg_size(), 20);

        Ok(())
    }

    #[test]
    fn test_rotbl_stat_display() -> anyhow::Result<()> {
        let stat = RotblStat {
            block_num: 5000,
            key_num: 10,
            data_size: 100,
            index_size: 200,
        };

        assert_eq!(
            "10 keys in 5_000 blocks: data(100 B), index(200 B), avg block size(0 B)",
            stat.to_string()
        );

        Ok(())
    }
}
