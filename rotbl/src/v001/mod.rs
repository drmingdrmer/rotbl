mod block;
mod block_cache;
mod block_encoding_meta;
mod block_id;
mod block_index;
mod block_stream;
mod checksum_reader;
mod checksum_writer;
mod config;
mod db;
mod footer;
mod header;
mod marked;
mod range;
mod rotbl;
mod rotbl_io;
mod rotbl_io_get;
mod rotbl_io_stream;
mod rotbl_meta;
pub(crate) mod testing;
mod with_checksum;

pub use block_id::BlockId;
pub use block_index::BlockIndex;
pub use block_stream::BlockStream;
pub use config::BlockCacheConfig;
pub use config::BlockConfig;
pub use config::Config;
pub use db::DB;
pub use footer::Footer;
pub use marked::Marked;
pub use marked::SeqMarked;
pub use rotbl_meta::RotblMeta;

pub use crate::v001::rotbl::Rotbl;

// TODO: introduce an Error for rotbl
