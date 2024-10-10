use std::fmt;
use std::io;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use crc32fast::Hasher;

/// A reader that calculates the crc32 checksum of the data read from it.
pub struct ChecksumReader<R> {
    hasher: Hasher,
    inner: R,
}

impl<R> ChecksumReader<R>
where R: io::Read
{
    pub fn new(inner: R) -> Self {
        Self {
            hasher: Hasher::new(),
            inner,
        }
    }

    /// Finalize the crc32 checksum and consume `self`.
    ///
    /// Return the checksum of all read data.
    #[allow(dead_code)]
    pub fn finalize_checksum(self) -> u32 {
        self.hasher.finalize()
    }

    /// Read the crc32 checksum from the least significant 32 bits of a `u64` in BigEndian,
    /// and compare it with the calculated checksum.
    pub fn verify_checksum<D: fmt::Display>(self, context: impl Fn() -> D) -> io::Result<()> {
        let mut r = self.inner;
        let actual = self.hasher.finalize() as u64;

        let got = r.read_u64::<BigEndian>()?;
        if actual != got {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "crc32 checksum mismatch: expected {:x}, got {:x}, while {}",
                    actual,
                    got,
                    context()
                ),
            ))
        } else {
            Ok(())
        }
    }
}

impl<R> io::Read for ChecksumReader<R>
where R: io::Read
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        if read > 0 {
            self.hasher.update(&buf[..read]);
        }
        Ok(read)
    }
}

#[cfg(test)]
#[allow(clippy::redundant_clone)]
mod tests {
    use std::io::Read;
    use std::io::Write;

    use crate::codec::checksum_writer::ChecksumWriter;

    #[test]
    fn test_checksum_reader() -> anyhow::Result<()> {
        let mut b = Vec::new();

        // write something
        {
            let mut n = 0;

            let mut w = ChecksumWriter::new(&mut b);

            n += w.write(b"foo")?;
            n += w.write(b"bar")?;
            n += w.write_checksum()?;
            assert_eq!(n, 14);
        }

        // Finalize the checksum and verify it
        {
            let mut r = super::ChecksumReader::new(&b[..]);
            let mut read_buf = [0u8; 6];
            r.read_exact(&mut read_buf)?;
            let crc = r.finalize_checksum();
            assert_eq!(crc32fast::hash(b"foobar"), crc);
        }

        // Verify the checksum
        {
            let mut r = super::ChecksumReader::new(&b[..]);
            let mut read_buf = [0u8; 6];
            r.read_exact(&mut read_buf)?;
            r.verify_checksum(|| "")?; // No error
        }

        // Verify against wrong checksum
        let last = b.len() - 1;
        b[last] = b[last].wrapping_add(1);

        {
            let mut r = super::ChecksumReader::new(&b[..]);
            let mut read_buf = [0; 6];
            r.read_exact(&mut read_buf)?;
            let res = r.verify_checksum(|| ""); // checksum error
            assert!(res.is_err());
        }

        Ok(())
    }
}
