// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Future implementation for io_uring read operations.

use super::requests::IoRequest;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Future that awaits completion of an io_uring read operation.
///
/// This future holds a direct reference to the IoRequest and polls its
/// completion flag without any global registry or HashMap lookups.
pub(super) struct UringReadFuture {
    pub(super) request: Arc<IoRequest>,
}

impl Future for UringReadFuture {
    type Output = object_store::Result<Bytes>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.request.state.lock().unwrap();
        if state.completed {
            match state.err.take() {
                Some(err) => {
                    return Poll::Ready(Err(object_store::Error::Generic {
                        store: "io_uring",
                        source: Box::new(err),
                    }));
                }
                None => {
                    let bytes = std::mem::take(&mut state.buffer).freeze();
                    return Poll::Ready(Ok(bytes));
                }
            }
        }

        state.waker = Some(cx.waker().clone());

        Poll::Pending
    }
}
