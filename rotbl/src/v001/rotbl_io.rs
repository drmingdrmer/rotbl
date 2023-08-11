use std::future::Future;
use std::io;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use crate::v001::block::Block;
use crate::v001::block_id::BlockId;
use crate::v001::rotbl::Rotbl;
use crate::v001::rotbl_io_get::Get;
use crate::v001::rotbl_io_stream::State;
use crate::v001::rotbl_io_stream::TableStream;

pub(crate) type BlockIOPayload = IOPayload<BlockId, Arc<Block>>;

pub(crate) enum IOPayload<R, D> {
    Request(R),
    Response(D),
}

impl<I, T> IOPayload<I, T> {}

/// A port to exchange data between [`IODriver`] and IO consumer, such as [`Get`].
pub(crate) struct IOPort {
    pub(crate) inner: Arc<Mutex<Option<BlockIOPayload>>>,
}

impl IOPort {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
        }
    }

    /// Take the response data from the IO port.
    pub fn ready(&self) -> Option<Arc<Block>> {
        let mut g = self.inner.lock().unwrap();

        let got = g.take().unwrap();

        match got {
            IOPayload::Response(v) => Some(v),
            IOPayload::Request(_) => {
                unreachable!("IOPort::Request")
            }
        }
    }

    /// Request to load a block.
    pub fn request(&self, req: BlockId) {
        let mut g = self.inner.lock().unwrap();

        let curr = g.take();

        assert!(curr.is_none());

        *g = Some(IOPayload::Request(req));
    }
}

/// Dedicate IO provider for reading data from the rotbl table.
///
/// All read operations are provided by `IODriver`.
pub struct IODriver<'a> {
    /// Reference to the `Rotbl` instance.
    pub(crate) rotbl: &'a Rotbl,

    pub(crate) io: IOPort,
}

impl<'a> IODriver<'a> {
    /// Build a `Stream` that reads kv pairs from the table sequentially.
    pub fn range<R>(&self, range: R) -> TableStream
    where R: RangeBounds<String> + Clone {
        let indexes = self.rotbl.block_index.lookup_range(range.clone());

        TableStream {
            state: State::RequestingBlock,
            io: &self.io,
            range: (range.start_bound().cloned(), range.end_bound().cloned()),
            table_id: self.rotbl.table_id,
            block_index: indexes.iter(),
            block_stream: None,
        }
    }

    /// Return a `Future` that returns the value of the given key.
    pub fn get(&'a self, key: &'a str) -> Get<'a> {
        let block_num = self.rotbl.block_index.lookup(key).map(|x| x.block_num);

        let block = if let Some(bn) = block_num {
            self.rotbl.get_block(bn)
        } else {
            None
        };

        let block_id = block_num.map(|n| BlockId::new(self.rotbl.table_id, n));

        Get {
            waiting_for_block: false,
            io: &self.io,
            block_id,
            key,
            block,
        }
    }

    /// Run an IO procedure in blocking mode.
    ///
    /// The provided closure `f` returns a future that consumes data provided by this
    /// `IODriver`. Note that `f` does not handle IO errors. IO errors will be returned before
    /// passing to `f`.
    ///
    /// `f` must not depends on any other async runtime functionality, such as `tokio::timer`.
    ///
    /// Example:
    /// ```ignore
    /// let res = self.run(|io| async {
    ///     io.range(..).collect::<Vec<_>>()
    /// });
    /// ```
    pub fn run<F, T>(&self, f: F) -> Result<T, io::Error>
    where F: for<'f> FnOnce(&'f Self) -> Pin<Box<dyn Future<Output = T> + 'f>> {
        let fu = f(self);
        self.block_on(fu)
    }

    /// Execute a future in blocking mode.
    ///
    /// This function expect the input future returns `Pending` every time an IO request is sent.
    /// Then it will handle the IO request and poll the future again.
    pub fn block_on<T>(&self, mut fu: impl Future<Output = T>) -> Result<T, io::Error> {
        let cx = &mut Context::from_waker(futures::task::noop_waker_ref());

        loop {
            #[allow(unused_mut)]
            let mut fu = unsafe { Pin::new_unchecked(&mut fu) };

            let res = fu.poll(cx);
            match res {
                Poll::Pending => {
                    Self::handle_io(self.rotbl, &self.io.inner)?;
                    continue;
                }
                Poll::Ready(v) => {
                    return Ok(v);
                }
            }
        }
    }

    /// Handle the IO request and send the IO response, both via `self.io_port`.
    fn handle_io(rotbl: &Rotbl, io_port: &Arc<Mutex<Option<BlockIOPayload>>>) -> Result<(), io::Error> {
        let block_id = {
            let mut g = io_port.lock().unwrap();
            let port = g.take();
            let port = port.unwrap();
            match port {
                IOPayload::Request(x) => x,
                IOPayload::Response(_) => {
                    unreachable!("IOPort::Response")
                }
            }
        };

        let block = rotbl.load_block(block_id.block_num())?;

        {
            let mut g = io_port.lock().unwrap();
            *g = Some(IOPayload::Response(block));
        }

        Ok(())
    }
}
