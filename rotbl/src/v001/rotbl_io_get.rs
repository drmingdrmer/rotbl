use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::v001::block::Block;
use crate::v001::block_id::BlockId;
use crate::v001::rotbl_io::IOPort;
use crate::v001::SeqMarked;

/// A [`Future`] that returns a value from the rotbl table.
///
/// If returns a `Ready` if the block containing the key is already in cache,
/// or returns a `Pending` if it needs to load the block from [`IODriver`].
///
/// [`IODriver`]: `crate::v001::rotbl_io::IODriver`
pub struct Get<'a> {
    pub(crate) waiting_for_block: bool,
    pub(crate) io: &'a IOPort,
    pub(crate) block_id: Option<BlockId>,
    pub(crate) key: &'a str,
    pub(crate) block: Option<Arc<Block>>,
}

impl<'a> Future for Get<'a> {
    type Output = Option<SeqMarked>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output>
    where Self: 'a {
        if self.waiting_for_block {
            let block = self.io.ready().unwrap();
            self.block = Some(block);
        }

        if let Some(block) = self.block.as_ref() {
            let v = block.get(self.key).cloned();
            return Poll::Ready(v);
        }

        if let Some(bi) = self.block_id {
            self.io.request(bi);
            self.waiting_for_block = true;
            return Poll::Pending;
        }

        Poll::Ready(None)
    }
}
