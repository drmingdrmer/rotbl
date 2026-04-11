//! Storage API to adapt to different storage backends.

pub mod impls;

use std::fmt::Debug;
use std::io;
use std::io::Write;

pub type BoxReaderAt = Box<dyn ReaderAt>;
pub type BoxWriter = Box<dyn Writer + Send>;

/// A stateless positional reader.
///
/// `read_exact_at` takes an explicit `offset` and `&self`, so implementations
/// must be safe to call concurrently from multiple threads on the same
/// handle — typically by using positional I/O such as `pread(2)` on unix or
/// `ReadFile` with `OVERLAPPED` on windows. This is the sole read API used
/// by rotbl's hot block-load path, so that N concurrent cache misses can
/// issue N parallel disk reads instead of serializing behind a single file
/// mutex.
#[allow(clippy::len_without_is_empty)]
pub trait ReaderAt: Send + Sync + Debug + 'static {
    /// Read exactly `buf.len()` bytes starting at `offset`.
    ///
    /// Returns `Err(ErrorKind::UnexpectedEof)` if fewer bytes are available.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;

    /// Return the total size of the underlying object in bytes.
    ///
    /// Used at `Rotbl::open` time to locate the fixed-size footer at the
    /// tail of the file without needing a separate `seek` call.
    fn len(&self) -> io::Result<u64>;
}

/// Represents a writer for the storage system.
///
/// The writer must implement the `Write` trait to handle data writing operations.
/// Additionally, it must implement the `finalize()` method to ensure data is properly
/// finalized and persisted to storage. The target file should remain hidden until
/// the `finalize()` method is called, ensuring data consistency and integrity.
pub trait Writer
where Self: Write + Debug + 'static
{
    /// Commits the data to persistent storage.
    ///
    /// This method ensures that all data is properly written and finalized.
    /// After calling this method, the writer object should not be used again.
    ///
    /// This method cannot consume `self` (e.g., `fn commit(self)`), because `self` requires
    /// `Sized`. As a result, `Box<dyn Writer>::commit()` cannot be used.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the commit operation fails.
    fn commit(&mut self) -> Result<(), io::Error>;
}

/// This trait defines the behavior required to read and write data to persistent storage.
pub trait Storage
where Self: Debug + Clone + Send + 'static
{
    /// Get a positional reader for the given key.
    ///
    /// The returned handle is the only read API rotbl uses: both the
    /// one-shot metadata parsing in `Rotbl::open` and the concurrent
    /// block-load hot path go through [`ReaderAt::read_exact_at`]. Backends
    /// are expected to use positional I/O (`pread`, etc.) so that N
    /// concurrent block loads can progress in parallel.
    fn reader_at(&mut self, key: &str) -> Result<BoxReaderAt, io::Error>;

    /// Get a writer to write data to a specific key in the storage.
    fn writer(&mut self, key: &str) -> Result<BoxWriter, io::Error>;
}
