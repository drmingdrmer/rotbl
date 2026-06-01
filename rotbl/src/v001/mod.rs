mod block;
mod config;
mod db;
mod header;
mod range;
mod rotbl;
pub(crate) mod testing;

pub(crate) mod bincode_config;
pub(crate) mod types;

pub use block::id::BlockId;
pub use block::segmented_key::SegmentedKey;
pub use block::stream::BlockStream;
pub use config::BlockCacheConfig;
pub use config::BlockConfig;
pub use config::Config;
pub use db::cache_stat::CacheStat;
pub use db::DB;
pub use header::Header;
pub use rotbl::access_stat;
pub use rotbl::builder::Builder;
pub use rotbl::dump::Dump;
pub use rotbl::footer::Footer;
pub use rotbl::index::BlockIndex;
pub use rotbl::index::BlockIndexEntry;
pub use rotbl::meta::RotblMeta;
pub use rotbl::meta_payload as rotbl_meta_payload;
pub use rotbl::stat;
pub use rotbl::Rotbl;
pub use seq_marked::Marked;
pub use seq_marked::SeqMarked;
pub use types::Segment;

// TODO: introduce an Error for rotbl
