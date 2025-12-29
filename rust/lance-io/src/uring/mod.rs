// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! io_uring-based I/O for high-performance local file access.
//!
//! This module provides a [`UringReader`] that implements the [`Reader`](crate::traits::Reader) trait
//! using Linux's io_uring interface for asynchronous I/O. It uses a dedicated process-wide thread
//! running a synchronous io_uring event loop, communicating via synchronous SPSC channels.
//!
//! # Architecture
//!
//! - A static process-wide thread handles all io_uring operations
//! - The thread is NOT managed by tokio and runs a synchronous event loop
//! - Communication uses `std::sync::mpsc::sync_channel` for bounded SPSC queues
//! - Futures on the reader side bridge sync responses to async API
//!
//! # Configuration
//!
//! The io_uring reader is enabled by using the `file+uring://` URI scheme instead of `file://`.
//! Additional tuning parameters are controlled by environment variables:
//!
//! - `LANCE_URING_CORE` - CPU core to pin the uring thread to (optional)
//! - `LANCE_URING_BLOCK_SIZE` - Block size in bytes (default: 64KB)
//! - `LANCE_URING_IO_PARALLELISM` - Max concurrent operations (default: 32)
//! - `LANCE_URING_QUEUE_DEPTH` - io_uring queue depth (default: 16K)
//!
//! # Platform Support
//!
//! This module is only available on Linux and requires kernel 5.1 or newer.
//! On other platforms, the code falls back to [`LocalObjectReader`](crate::local::LocalObjectReader).
//!
//! # Example
//!
//! ```no_run
//! # use lance_io::object_store::ObjectStore;
//! # async fn example() -> lance_core::Result<()> {
//! // Enable io_uring by using the file+uring:// scheme
//! let uri = "file+uring:///path/to/file.dat";
//! let (store, path) = ObjectStore::from_uri(uri).await?;
//! let reader = store.open(&path).await?;
//!
//! // Reader will use io_uring
//! let data = reader.get_range(0..1024).await?;
//! # Ok(())
//! # }
//! ```

mod future;
mod reader;
mod requests;
mod thread;

#[cfg(test)]
mod tests;

pub use reader::UringReader;

/// Default block size for io_uring reads (64KB)
pub const DEFAULT_URING_BLOCK_SIZE: usize = 64 * 1024;

/// Default I/O parallelism for io_uring (32 concurrent operations)
pub const DEFAULT_URING_IO_PARALLELISM: usize = 32;

/// Default io_uring queue depth (16K entries)
pub const DEFAULT_URING_QUEUE_DEPTH: usize = 16 * 1024;
