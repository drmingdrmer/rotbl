use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use codeq::config::CodeqConfig;
use codeq::Encode;

use crate::storage::BoxWriter;
use crate::storage::Storage;
use crate::typ::Type;
use crate::v001::block::Block;
use crate::v001::block_index::BlockIndexEntry;
use crate::v001::header::Header;
use crate::v001::rotbl::stat::RotblStat;
use crate::v001::types::Checksum;
use crate::v001::types::Segment;
use crate::v001::BlockIndex;
use crate::v001::Config;
use crate::v001::Footer;
use crate::v001::Rotbl;
use crate::v001::RotblMeta;
use crate::v001::SeqMarked;
use crate::v001::DB;
use crate::version::Version;

pub struct Builder<S>
where S: Storage
{
    config: Config,

    offset: usize,
    header: Header,
    table_id: u32,

    chunk_size: usize,

    stat: RotblStat,

    this_chunk: Vec<(String, SeqMarked)>,

    prev: Option<String>,

    storage: S,

    path: String,

    writer: BoxWriter,

    index: Vec<BlockIndexEntry>,
}

impl<S> Builder<S>
where S: Storage
{
    pub fn new(mut storage: S, config: Config, path: &str) -> Result<Self, io::Error> {
        // Table id is not supported yet in this version,
        // and is always 0.
        let table_id = 0;

        let f = storage.writer(path)?;

        let chunk_size = config.block_config.max_items();
        if chunk_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "BlockConfig.max_items must be greater than 0",
            ));
        }

        let mut builder = Self {
            config,
            offset: 0,
            header: Header::new(Type::Rotbl, Version::V001),
            table_id,
            chunk_size,
            stat: RotblStat::default(),
            this_chunk: Vec::with_capacity(chunk_size),
            prev: None,
            storage,
            path: path.to_string(),
            writer: f,
            index: Vec::new(),
        };

        builder.offset += builder.header.encode(&mut builder.writer)?;

        let tid = Checksum::wrap(builder.table_id);
        builder.offset += tid.encode(&mut builder.writer)?;

        Ok(builder)
    }

    pub fn append_kv(&mut self, k: impl ToString, v: SeqMarked) -> Result<(), io::Error> {
        let k = k.to_string();

        if self.config.debug_check() {
            assert!(
                Some(&k) > self.prev.as_ref(),
                "this key {:?} must be greater than prev {:?}",
                k,
                self.prev
            );

            self.prev = Some(k.clone());
        }

        self.stat.key_num += 1;
        self.this_chunk.push((k, v));

        if self.this_chunk.len() == self.chunk_size {
            let chunk =
                std::mem::replace(&mut self.this_chunk, Vec::with_capacity(self.chunk_size));

            self.write_chunk(chunk)?;
        }

        Ok(())
    }

    fn write_chunk(
        &mut self,
        chunk: impl IntoIterator<Item = (String, SeqMarked)>,
    ) -> Result<(), io::Error> {
        let bt: BTreeMap<_, _> = chunk.into_iter().collect();

        let first_key: String = bt.first_key_value().unwrap().0.clone();
        let last_key: String = bt.last_key_value().unwrap().0.clone();

        let block = Block::new(self.stat.block_num, bt);

        let block_offset = self.offset as u64;
        let block_size = block.encode(&mut self.writer)?;
        self.offset += block_size;
        self.stat.data_size += block_size as u64;

        let index_entry = BlockIndexEntry {
            block_num: self.stat.block_num,
            offset: block_offset,
            size: block_size as u64,
            first_key,
            last_key,
        };

        self.index.push(index_entry);
        self.stat.block_num += 1;

        Ok(())
    }

    pub fn commit(mut self, rotbl_meta: RotblMeta) -> Result<Rotbl, io::Error> {
        if !self.this_chunk.is_empty() {
            let chunk = std::mem::take(&mut self.this_chunk);
            self.write_chunk(chunk)?;
        }

        // Write block index

        let block_index = BlockIndex::new(self.index);
        self.stat.index_size = block_index.encode(&mut self.writer)? as u64;

        let blog_index_seg = Segment::new(self.offset as u64, self.stat.index_size);
        self.offset += self.stat.index_size as usize;

        // Write Meta

        let meta_size = rotbl_meta.encode(&mut self.writer)?;
        let meta_seg = Segment::new(self.offset as u64, meta_size as u64);
        self.offset += meta_size;

        // Write Stat

        let stat_size = self.stat.encode(&mut self.writer)?;
        let stat_seg = Segment::new(self.offset as u64, stat_size as u64);
        self.offset += stat_size;

        // Write footer

        let footer = Footer::new(blog_index_seg, meta_seg, stat_seg);
        self.offset += footer.encode(&mut self.writer)?;

        self.writer.commit()?;

        let reader = self.storage.reader(&self.path)?;

        let block_cache = DB::new_cache(self.config.clone());

        let r = Rotbl {
            block_cache,
            file: Arc::new(Mutex::new(reader)),
            file_size: self.offset as u64,
            header: self.header,
            table_id: self.table_id,
            meta: rotbl_meta,
            block_index,
            stat: self.stat,
            access_stat: Default::default(),
            footer,
        };

        Ok(r)
    }
}
