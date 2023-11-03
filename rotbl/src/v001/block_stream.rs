use std::marker::PhantomPinned;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::Stream;

use crate::v001::block::Block;
use crate::v001::block::BlockIter;
use crate::v001::SeqMarked;

/// A stream of key-value pairs in a block.
///
/// ### Safety
///
/// It can move before being pinned.
///
/// Because the Block behind Arc won't be changed by other threads,
/// and it is not moved. Therefore the reference is always valid even when it is moved.
pub struct BlockStream {
    /// `iter` is a `BlockIter` that iterates over the key-value pairs in the `Block`.
    ///
    /// It holds a reference to the `Block` in `block`, meaning that `iter` must be dropped before
    /// `block`. If `block` were dropped first, `iter` would hold an invalid reference.
    ///
    /// Note that Rust's default drop order is in the declaration order, so `iter` will be dropped
    /// before `block` by default.
    iter: BlockIter<'static>,

    #[allow(dead_code)]
    block: Arc<Block>,

    _p: PhantomPinned,
}

impl BlockStream {
    pub fn new<R>(block: Arc<Block>, range: R) -> Self
    where R: RangeBounds<String> {
        // ### Build a reference to the block.
        // Safety: 1) The Block behind Arc won't be changed by other threads.
        //         2) And it is not moved during the following building process in this function.
        //         Therefore the reference is always valid.
        let block_ptr = block.as_ref() as *const Block;
        let block_ref = unsafe { &*block_ptr };

        let iter = block_ref.range::<String, _>(range);

        Self {
            block,
            iter,
            _p: Default::default(),
        }
    }

    /// Returns the next key-value pair in the block.
    ///
    /// This method wraps unsafe operation and provide lifetime safety.
    fn next(self: Pin<&mut Self>) -> Option<(&String, &SeqMarked)> {
        // Safety: We do not move the mutable reference Thus Pin is safe.
        let it = unsafe { &mut self.get_unchecked_mut().iter };
        it.next()
    }
}

/// This stream always returns `Ready` thus it is purely a `generator`.
impl Stream for BlockStream {
    type Item = (String, SeqMarked);

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = self.next().map(|(k, v)| (k.clone(), v.clone()));
        Poll::Ready(next)
    }
}

#[cfg(test)]
#[allow(clippy::redundant_clone)]
mod tests {
    use std::sync::Arc;

    use futures::executor::block_on;
    use futures::StreamExt;

    use crate::v001::block::Block;
    use crate::v001::block_stream::BlockStream;
    use crate::v001::testing::bb;
    use crate::v001::testing::ss;
    use crate::v001::testing::ss_vec;
    use crate::v001::SeqMarked;

    #[test]
    fn test_block_stream() -> anyhow::Result<()> {
        //
        let block_data = maplit::btreemap! {
            ss("a") => SeqMarked::new(1, false, bb("A")),
            ss("b") => SeqMarked::new(2, true,  bb("B")),
            ss("c") => SeqMarked::new(3, true,  bb("C")),
            ss("d") => SeqMarked::new(4, true,  bb("D")),
        };

        let block = Block::new(5, block_data.clone());
        let block = Arc::new(block);

        // debug
        {
            let stream = BlockStream::new(block.clone(), ..);

            let block_ptr = stream.block.as_ref() as *const Block;
            println!("block_ptr: {:x}", block_ptr as usize);
        }

        // Test range queries

        fn collect(strm: BlockStream) -> Vec<String> {
            block_on(strm.map(|(k, _v)| k).collect::<Vec<_>>())
        }

        // Range: all
        {
            let stream = BlockStream::new(block.clone(), ..);
            let got = collect(stream);
            assert_eq!(ss_vec(["a", "b", "c", "d"]), got);
        }

        // Range: empty
        {
            let stream = BlockStream::new(block.clone(), ..ss("a"));
            let got = collect(stream);
            assert_eq!(Vec::<String>::new(), got);
        }

        // Range: right unbounded
        {
            let stream = BlockStream::new(block.clone(), ss("b1")..);
            let got = collect(stream);
            assert_eq!(ss_vec(["c", "d"]), got);
        }

        // Range: left unbounded
        {
            let stream = BlockStream::new(block.clone(), ..ss("c1"));
            let got = collect(stream);
            assert_eq!(ss_vec(["a", "b", "c"]), got);
        }

        // Range: both bounded
        {
            let stream = BlockStream::new(block.clone(), ss("b1")..ss("c1"));
            let got = collect(stream);
            assert_eq!(ss_vec(["c"]), got);
        }

        Ok(())
    }
}
