mod block;
mod block_cache;
mod block_encoding_meta;
mod block_id;
mod block_index;
mod block_stream;
mod cache_stat;
mod checksum_reader;
mod checksum_writer;
mod config;
mod db;
mod footer;
mod header;
mod marked;
mod range;
mod rotbl;
mod rotbl_meta;
pub mod rotbl_meta_payload;
mod segment;
pub(crate) mod testing;
mod with_checksum;

pub(crate) mod bincode_config;

pub use block_id::BlockId;
pub use block_index::BlockIndex;
pub use block_stream::BlockStream;
pub use cache_stat::CacheStat;
pub use config::BlockCacheConfig;
pub use config::BlockConfig;
pub use config::Config;
pub use db::DB;
pub use footer::Footer;
pub use marked::Marked;
pub use marked::SeqMarked;
pub use rotbl::builder::Builder;
pub use rotbl::dump::Dump;
pub use rotbl::stat;
pub use rotbl::Rotbl;
pub use rotbl_meta::RotblMeta;

// TODO: introduce an Error for rotbl
