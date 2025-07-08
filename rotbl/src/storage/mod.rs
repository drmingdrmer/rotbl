//! Storage API to adapt to different storage backends.

pub mod impls;

use std::fmt::Debug;
use std::io;
use std::io::BufRead;
use std::io::Seek;
use std::io::Write;

pub type BoxReader = Box<dyn Reader + Send>;
pub type BoxWriter = Box<dyn Writer + Send>;

/// The type of the reader.
///
/// The reader requires `Seek` to load a specific position of the data.
/// And it is the implementation's duty to provide a `BufRead` implementation.
/// Usually using `BufReader` to wrap the reader would be the best choice.
///
/// The reader must implement `Send` to ensure it can be safely used across await points.
pub trait Reader
where Self: Seek + BufRead + Send + Debug + 'static
{
}

impl<T: Seek + BufRead + Send + Debug + 'static> Reader for T {}

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
    /// Get a reader to read the data of the given key.
    fn reader(&mut self, key: &str) -> Result<BoxReader, io::Error>;

    /// Get a writer to write data to a specific key in the storage.
    fn writer(&mut self, key: &str) -> Result<BoxWriter, io::Error>;
}
