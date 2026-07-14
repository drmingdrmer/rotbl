//! Provides the file system based storage implementation.

use std::fs;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::io_util::DEFAULT_WRITE_BUF_SIZE;
use crate::storage;
use crate::storage::BoxReaderAt;
use crate::storage::BoxWriter;
use crate::storage::ReaderAt;
use crate::storage::Storage;

/// The storage implementation that uses the file system.
#[derive(Debug, Clone)]
pub struct FsStorage {
    base_dir: PathBuf,
}

impl FsStorage {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Return the containing directory of the storage.
    pub fn base_dir(&self) -> &Path {
        self.base_dir.as_path()
    }

    pub fn base_dir_str(&self) -> &str {
        self.base_dir.to_str().expect("base_dir should be valid UTF-8")
    }

    /// A unique suffix for a temp file name.
    ///
    /// Process-wide monotonic counter in the low 32 bits (unique within this
    /// process, no syscall, no sleep) with the pid in the high 32 bits, so two
    /// processes writing the same key pick disjoint temp names. `create_new(true)`
    /// turns any residual collision into a clean error rather than a clobber.
    fn temp_fn_num() -> u64 {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        (u64::from(std::process::id()) << 32) | (seq & 0xFFFF_FFFF)
    }
}

impl Storage for FsStorage {
    fn reader_at(&mut self, key: &str) -> Result<BoxReaderAt, io::Error> {
        let path = self.base_dir.join(key);
        let file = fs::OpenOptions::new().create(false).create_new(false).read(true).open(&path)?;
        Ok(Box::new(FsReaderAt { file }))
    }

    fn writer(&mut self, key: &str) -> Result<BoxWriter, io::Error> {
        let target_path = self.base_dir.join(key);
        let micros = Self::temp_fn_num();

        let temp_path = self.base_dir.join(format!("{key}.tmp-{micros}"));

        let w = FsWriter::new(temp_path, target_path)?;
        Ok(Box::new(w))
    }
}

/// Positional reader backed by a single shared `File` handle.
///
/// `File` itself is `Send + Sync`; positional reads via `pread(2)` (unix) do
/// not touch the kernel file cursor, so the same handle can service many
/// concurrent block loads without any userspace lock. On windows the same
/// guarantee is provided via `ReadFile` with an explicit `OVERLAPPED` offset,
/// exposed as `FileExt::seek_read`.
#[derive(Debug)]
struct FsReaderAt {
    file: File,
}

impl ReaderAt for FsReaderAt {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.read_exact_at(buf, offset)
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            // `seek_read` on windows does a positioned `ReadFile` and does
            // not mutate the shared file cursor, so it is safe to call
            // concurrently from multiple threads on the same handle. It
            // can, however, return a short read, so we loop until the whole
            // buffer is filled.
            let total = buf.len();
            let mut read = 0;
            while read < total {
                let n = self.file.seek_read(&mut buf[read..], offset + read as u64)?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "failed to fill whole buffer",
                    ));
                }
                read += n;
            }
            Ok(())
        }
        #[cfg(not(any(unix, windows)))]
        {
            compile_error!("FsReaderAt requires a unix or windows target")
        }
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

/// The writer implementation that uses the file system.
///
/// This writer writes data to a temporary file and then moves it to the target file.
/// This ensures that the target file is always in a consistent state.
#[derive(Debug)]
pub struct FsWriter {
    file: Option<io::BufWriter<File>>,
    target_path: PathBuf,
    temp_path: PathBuf,
}

impl FsWriter {
    /// Create a new writer.
    pub fn new(temp_path: PathBuf, target_path: PathBuf) -> Result<Self, io::Error> {
        let f = fs::OpenOptions::new()
            .create_new(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&temp_path)?;

        let file = io::BufWriter::with_capacity(DEFAULT_WRITE_BUF_SIZE, f);

        Ok(Self {
            file: Some(file),
            target_path,
            temp_path,
        })
    }
}

impl Write for FsWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.file.as_mut().unwrap().write(buf)
    }
    fn flush(&mut self) -> Result<(), io::Error> {
        self.file.as_mut().unwrap().flush()
    }
}

impl storage::Writer for FsWriter {
    fn commit(&mut self) -> Result<(), io::Error> {
        let Some(f) = self.file.take() else {
            unreachable!("FsWriter::commit() should not be called multiple times");
        };

        // Flush and get inner writer
        let f = f.into_inner().map_err(|e| e.into_error())?;

        f.sync_all()?;

        fs::rename(&self.temp_path, &self.target_path)?;

        // The rename lives in the parent directory entry; without syncing the
        // directory a power loss can revert the target to its previous
        // content, silently rolling back an acknowledged commit.
        #[cfg(unix)]
        if let Some(parent) = self.target_path.parent() {
            File::open(parent)?.sync_all()?;
        }

        Ok(())
    }
}

impl Drop for FsWriter {
    fn drop(&mut self) {
        // Best-effort cleanup so an aborted or failed write does not leak the
        // temp file. After a successful commit the temp path no longer exists.
        if let Err(e) = fs::remove_file(&self.temp_path) {
            if e.kind() != io::ErrorKind::NotFound {
                log::warn!("failed to remove temp file {:?}: {}", self.temp_path, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Writer;

    #[test]
    fn test_fs_writer() -> Result<(), io::Error> {
        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path().join("test.txt.tmp");
        let target_path = temp_dir.path().join("test.txt");

        let mut writer = FsWriter::new(temp_path.clone(), target_path.clone())?;

        writer.write_all(b"Hello, world!")?;

        // check the file is not visible
        assert!(temp_path.exists());
        assert!(!target_path.exists());

        writer.commit()?;

        // check the file is visible
        assert!(!temp_path.exists());
        assert!(target_path.exists());

        let content = fs::read_to_string(target_path)?;
        assert_eq!(content, "Hello, world!");

        Ok(())
    }

    #[test]
    fn test_fs_storage() -> Result<(), io::Error> {
        let temp_dir = tempfile::tempdir()?;

        let mut storage = FsStorage::new(temp_dir.path().to_path_buf());

        let mut writer = storage.writer("test.txt")?;
        writer.write_all(b"Hello, world!")?;
        writer.commit()?;

        let reader = storage.reader_at("test.txt")?;
        assert_eq!(reader.len()?, 13);

        let mut buf = vec![0u8; 13];
        reader.read_exact_at(&mut buf, 0)?;
        assert_eq!(&buf[..], b"Hello, world!");
        Ok(())
    }

    #[test]
    fn test_fs_writer_drop_removes_temp_file() -> Result<(), io::Error> {
        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path().join("test.txt.tmp");
        let target_path = temp_dir.path().join("test.txt");

        let mut writer = FsWriter::new(temp_path.clone(), target_path.clone())?;
        writer.write_all(b"abandoned")?;
        drop(writer);

        assert!(!temp_path.exists());
        assert!(!target_path.exists());
        Ok(())
    }

    #[test]
    fn test_fs_storage_base_dir() -> Result<(), io::Error> {
        let temp_dir = tempfile::tempdir()?;

        let storage = FsStorage::new(temp_dir.path().to_path_buf());
        assert_eq!(storage.base_dir_str(), temp_dir.path().to_str().unwrap());

        Ok(())
    }

    #[test]
    fn test_temp_fn() {
        // The high 32 bits carry the pid; the low bits are the sequence.
        let got = FsStorage::temp_fn_num();
        assert_eq!(got >> 32, u64::from(std::process::id()));
    }

    #[test]
    fn test_temp_fn_non_dup() {
        let mut prev = None;
        for _i in 0..1000 {
            let got = Some(FsStorage::temp_fn_num());
            assert!(prev < got);
            prev = got;
        }
    }
}
