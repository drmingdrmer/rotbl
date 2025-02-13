pub use codeq::config::Crc32fast as Checksum;

pub type Segment = codeq::Segment<Checksum>;
pub type WithChecksum<T> = codeq::WithChecksum<Checksum, T>;
