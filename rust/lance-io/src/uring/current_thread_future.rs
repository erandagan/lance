// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Future implementation for thread-local io_uring operations.
//!
//! This future actively processes completions during polling instead of
//! relying on background threads to wake it up.

use super::current_thread::{process_thread_local_completions, submit_and_wait_thread_local};
use super::requests::IoRequest;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Future that awaits completion of a thread-local io_uring read operation
pub struct UringCurrentThreadFuture {
    request: Arc<IoRequest>,
}

impl UringCurrentThreadFuture {
    pub(super) fn new(request: Arc<IoRequest>) -> Self {
        Self { request }
    }
}

impl Future for UringCurrentThreadFuture {
    type Output = object_store::Result<Bytes>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Process any available completions
        if let Err(e) = process_thread_local_completions() {
            log::warn!("Error processing completions: {:?}", e);
        }

        if self.request.thread_id != std::thread::current().id() {
            panic!("Request thread ID does not match current thread ID");
        }

        // Check if our request completed
        let mut state = self.request.state.lock().unwrap();

        if state.completed {
            // Take result
            match state.err.take() {
                Some(err) => {
                    return Poll::Ready(Err(object_store::Error::Generic {
                        store: "io_uring_ct",
                        source: Box::new(err),
                    }));
                }
                None => {
                    let bytes = std::mem::take(&mut state.buffer).freeze();
                    return Poll::Ready(Ok(bytes));
                }
            }
        }

        // Not ready yet - submit and wait with timeout 0 (non-blocking)
        drop(state);

        if let Err(e) = submit_and_wait_thread_local() {
            log::debug!("Submit and wait error: {:?}", e);
        }

        // Process completions again after submit_and_wait
        if let Err(e) = process_thread_local_completions() {
            log::warn!(
                "Error processing completions after submit_and_wait: {:?}",
                e
            );
        }

        // Check again after processing
        let mut state = self.request.state.lock().unwrap();
        if state.completed {
            match state.err.take() {
                Some(err) => {
                    return Poll::Ready(Err(object_store::Error::Generic {
                        store: "io_uring_ct",
                        source: Box::new(err),
                    }));
                }
                None => {
                    let bytes = std::mem::take(&mut state.buffer).freeze();
                    return Poll::Ready(Ok(bytes));
                }
            }
        }

        // Still not ready - store waker and yield
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
