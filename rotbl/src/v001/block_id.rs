#[derive(Clone, Copy)]
#[derive(Debug)]
#[derive(Hash)]
#[derive(PartialEq, Eq)]
#[derive(PartialOrd, Ord)]
pub struct BlockId {
    table_id: u32,
    block_num: u32,
}

impl BlockId {
    pub fn new(table_id: u32, block_num: u32) -> Self {
        Self { table_id, block_num }
    }

    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    pub fn block_num(&self) -> u32 {
        self.block_num
    }
}
