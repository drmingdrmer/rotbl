//! `Rotbl` is a read-only on disk table of key-value.
//!
//! ```
//! # use std::sync::Arc;
//! # use futures::TryStreamExt;
//! use rotbl::v001::{Builder, Config, Rotbl, RotblMeta, SeqMarked};
//! #[tokio::main(flavor = "multi_thread")]
//! async fn main() {
//! let config = Config::default();
//!   #
//!   # // remove ./foo if exists
//!   # let _ = std::fs::remove_file("./foo");
//!   let r = {
//!     let mut b = Builder::new(config, "./foo", RotblMeta::new(1, "hello")).unwrap();
//!
//!     // keys must be sorted
//!     b.append_kv("bar", SeqMarked::new_normal(1, b"bar".to_vec())).unwrap();
//!     b.append_kv("foo", SeqMarked::new_normal(2, b"foo".to_vec())).unwrap();
//!
//!     Arc::new(b.commit().unwrap())
//!   };
//!
//!   // Read the value back
//!   let val = r.get("foo").await.unwrap();
//!   assert_eq!(val, Some(SeqMarked::new_normal(2, b"foo".to_vec())));
//!
//!   // Scan the table
//!   let kvs = r.range(..).try_collect::<Vec<_>>().await.unwrap();
//!   assert_eq!(kvs, vec![
//!                   ("bar".to_string(), SeqMarked::new_normal(1, b"bar".to_vec())),
//!                   ("foo".to_string(), SeqMarked::new_normal(2, b"foo".to_vec())),
//!   ]);
//! }
//! ```

#![feature(coroutines)]

extern crate core;

pub(crate) mod buf;
pub(crate) mod codec;
pub(crate) mod io_util;
pub mod num;
pub(crate) mod typ;
pub mod v001;
pub(crate) mod v00x;
pub mod version;
