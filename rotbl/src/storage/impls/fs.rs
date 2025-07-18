//! Provides the file system based storage implementation.

use std::fs;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::io_util::DEFAULT_READ_BUF_SIZE;
use crate::io_util::DEFAULT_WRITE_BUF_SIZE;
use crate::storage;
use crate::storage::BoxReader;
use crate::storage::BoxWriter;
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

    fn temp_fn_num() -> u64 {
        // Sleep to avoid timestamp collision when this function is called twice in a short time.
        std::thread::sleep(std::time::Duration::from_micros(2));

        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();
        ts as u64
    }
}

impl Storage for FsStorage {
    fn reader(&mut self, key: &str) -> Result<BoxReader, io::Error> {
        let path = self.base_dir.join(key);

        let f = fs::OpenOptions::new().create(false).create_new(false).read(true).open(&path)?;
        let f = io::BufReader::with_capacity(DEFAULT_READ_BUF_SIZE, f);

        let f = Box::new(f) as Box<dyn storage::Reader>;
        Ok(f)
    }

    fn writer(&mut self, key: &str) -> Result<BoxWriter, io::Error> {
        let target_path = self.base_dir.join(key);
        let micros = Self::temp_fn_num();

        let temp_path = self.base_dir.join(format!("{key}.tmp-{micros}"));

        let w = FsWriter::new(temp_path, target_path)?;
        Ok(Box::new(w))
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

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

        let mut reader = storage.reader("test.txt")?;
        let mut content = String::new();
        reader.read_to_string(&mut content)?;
        assert_eq!(content, "Hello, world!");
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
        let got = FsStorage::temp_fn_num();

        // typical timestamp in macro-seconds is `1_752_062_180_209_798`
        assert!(got > 1_000_000_000_000_000);
        assert!(got < 2_000_000_000_000_000);
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
