use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::io;
use std::io::Seek;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use codeq::Encode;
use codeq::Segment;
use codeq::WithChecksum;

use crate::io_util::DEFAULT_READ_BUF_SIZE;
use crate::io_util::DEFAULT_WRITE_BUF_SIZE;
use crate::typ::Type;
use crate::v001::block::Block;
use crate::v001::block_index::BlockIndexEntry;
use crate::v001::header::Header;
use crate::v001::rotbl::stat::RotblStat;
use crate::v001::BlockIndex;
use crate::v001::Config;
use crate::v001::Footer;
use crate::v001::Rotbl;
use crate::v001::RotblMeta;
use crate::v001::SeqMarked;
use crate::v001::DB;
use crate::version::Version;

pub struct Builder {
    config: Config,

    offset: usize,
    header: Header,
    table_id: u32,

    chunk_size: usize,

    stat: RotblStat,

    this_chunk: Vec<(String, SeqMarked)>,

    prev: Option<String>,

    f: io::BufWriter<File>,
    index: Vec<BlockIndexEntry>,
}

impl Builder {
    pub fn new<P: AsRef<Path>>(config: Config, path: P) -> Result<Self, io::Error> {
        // Table id is not supported yet in this version,
        // and is always 0.
        let table_id = 0;

        let f = fs::OpenOptions::new()
            // .create(true)
            .create_new(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;

        let f = io::BufWriter::with_capacity(DEFAULT_WRITE_BUF_SIZE, f);

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
            f,
            index: Vec::new(),
        };

        builder.offset += builder.header.encode(&mut builder.f)?;

        let tid = WithChecksum::new(builder.table_id);
        builder.offset += tid.encode(&mut builder.f)?;

        Ok(builder)
    }

    pub fn append_kv(&mut self, k: impl ToString, v: SeqMarked) -> Result<(), io::Error> {
        let k = k.to_string();

        if self.config.debug_check == Some(true) {
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
        let block_size = block.encode(&mut self.f)?;
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
        self.stat.index_size = block_index.encode(&mut self.f)? as u64;

        let blog_index_seg = Segment::new(self.offset as u64, self.stat.index_size);
        self.offset += self.stat.index_size as usize;

        // Write Meta

        let meta_size = rotbl_meta.encode(&mut self.f)?;
        let meta_seg = Segment::new(self.offset as u64, meta_size as u64);
        self.offset += meta_size;

        // Write Stat

        let stat_size = self.stat.encode(&mut self.f)?;
        let stat_seg = Segment::new(self.offset as u64, stat_size as u64);
        self.offset += stat_size;

        // Write footer

        let footer = Footer::new(blog_index_seg, meta_seg, stat_seg);
        self.offset += footer.encode(&mut self.f)?;

        self.f.flush()?;

        let mut f = self.f.into_inner().map_err(|e| e.into_error())?;

        f.sync_all()?;

        let file_size = f.seek(io::SeekFrom::End(0))?;

        let reader = io::BufReader::with_capacity(DEFAULT_READ_BUF_SIZE, f);

        let block_cache = DB::new_cache(self.config.clone());

        let r = Rotbl {
            block_cache,
            file: Arc::new(Mutex::new(reader)),
            file_size,
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
