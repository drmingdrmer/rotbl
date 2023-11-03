use std::collections::Bound;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::Stream;

use crate::v001::block_id::BlockId;
use crate::v001::block_index::BlockMeta;
use crate::v001::rotbl_io::IOPort;
use crate::v001::BlockStream;
use crate::v001::SeqMarked;

#[derive(Clone, Copy)]
pub(crate) enum State {
    RequestingBlock,

    // Iterating over a block.
    Iterating,

    /// Waiting for next block to be loaded.
    WaitingBlock,
}

pub struct TableStream<'a> {
    pub(crate) state: State,

    pub(crate) io: &'a IOPort,

    /// Key range
    pub(crate) range: (Bound<String>, Bound<String>),

    pub(crate) table_id: u32,
    pub(crate) block_index: std::slice::Iter<'a, BlockMeta>,

    pub(crate) block_stream: Option<BlockStream>,
}

impl<'a> TableStream<'a> {
    pub(crate) fn set_state(self: &mut Pin<&mut Self>, state: State) {
        unsafe { self.as_mut().get_unchecked_mut().state = state };
    }

    pub(crate) fn next_block_num(self: &mut Pin<&mut Self>) -> Option<u32> {
        unsafe { self.as_mut().get_unchecked_mut().block_index.next() }.map(|x| x.block_num)
    }

    pub(crate) fn project_block_stream(self: Pin<&mut Self>) -> Pin<&mut BlockStream> {
        unsafe { self.map_unchecked_mut(|s| s.block_stream.as_mut().unwrap()) }
    }

    pub(crate) fn set_block_stream(self: &mut Pin<&mut Self>, block_stream: BlockStream) {
        unsafe { self.as_mut().get_unchecked_mut().block_stream = Some(block_stream) };
    }
}

impl<'a> Stream for TableStream<'a>
where Self: 'a
{
    type Item = (String, SeqMarked);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                State::RequestingBlock => {
                    let next_block_num = self.next_block_num();

                    if let Some(next) = next_block_num {
                        // Submit io request.

                        self.io.request(BlockId::new(self.table_id, next));
                        self.set_state(State::WaitingBlock);

                        return Poll::Pending;
                    } else {
                        // No more block to iterate. Done

                        return Poll::Ready(None);
                    }
                }
                State::Iterating => {
                    let strm = self.as_mut().project_block_stream();
                    let poll_res = strm.poll_next(cx);

                    if let Poll::Ready(r) = poll_res {
                        if r.is_some() {
                            return Poll::Ready(r);
                        }

                        // This block is exhausted. Request next block.
                        self.set_state(State::RequestingBlock);
                        // continue;
                    } else {
                        unreachable!("block_stream always return Ready")
                    }
                }
                State::WaitingBlock => {
                    if let Some(b) = self.io.ready() {
                        // Request load-block io is ready for use.

                        let next_block_stream = BlockStream::new(b, self.range.clone());
                        self.set_block_stream(next_block_stream);
                        self.set_state(State::Iterating);
                        // continue;
                    } else {
                        unreachable!("WaitingBlock state should not be polled")
                    }
                }
            }
        }
    }
}
