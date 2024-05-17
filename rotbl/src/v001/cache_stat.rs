#[derive(Debug, Clone)]
#[derive(PartialEq, Eq)]
pub struct CacheStat {
    item_cnt: u64,
    size: u64,
}

impl CacheStat {
    pub fn new(item_cnt: u64, size: u64) -> Self {
        Self { item_cnt, size }
    }

    pub fn item_cnt(&self) -> u64 {
        self.item_cnt
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}
