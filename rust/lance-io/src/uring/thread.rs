// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Dedicated thread for io_uring operations.
//!
//! This module provides a process-wide thread that runs a synchronous io_uring event loop.
//! Communication happens via synchronous SPSC channels for bounded backpressure.

use super::requests::IoRequest;
use super::DEFAULT_URING_QUEUE_DEPTH;
use io_uring::{opcode, types, IoUring};
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

/// Process-wide io_uring thread handle.
///
/// This is initialized lazily on first use and is accessible globally.
pub(super) static URING_THREAD: LazyLock<UringThreadHandle> = LazyLock::new(UringThreadHandle::new);

/// Counter for generating unique user_data values.
///
/// Each io_uring operation needs a unique user_data ID to match completions
/// with their corresponding requests.
static USER_DATA_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Handle to the io_uring thread with channel for submitting requests.
pub(super) struct UringThreadHandle {
    /// Channel for sending read requests to the uring thread.
    ///
    /// This is a bounded synchronous channel that provides backpressure
    /// if the thread falls behind.
    pub request_tx: SyncSender<Arc<IoRequest>>,
}

impl UringThreadHandle {
    /// Create and spawn the io_uring thread.
    ///
    /// This is called once via LazyLock on first access.
    fn new() -> Self {
        let queue_depth = std::env::var("LANCE_URING_QUEUE_DEPTH")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_URING_QUEUE_DEPTH);

        // Create bounded channel for requests
        let (request_tx, request_rx) = sync_channel(queue_depth);

        // Spawn the dedicated thread
        std::thread::Builder::new()
            .name("lance-uring".to_string())
            .spawn(move || run_uring_thread(request_rx, queue_depth))
            .expect("Failed to spawn io_uring thread");

        Self { request_tx }
    }
}

/// Run the io_uring event loop.
///
/// This function runs in a dedicated thread and handles all io_uring operations.
fn run_uring_thread(request_rx: Receiver<Arc<IoRequest>>, queue_depth: usize) {
    // Pin to specific core if requested
    pin_to_core();

    // Initialize io_uring
    let mut ring = IoUring::builder()
        .build(queue_depth as u32)
        .expect("Failed to create io_uring");

    log::info!("io_uring thread started (queue_depth={})", queue_depth);

    // Track pending operations by user_data
    let mut pending: HashMap<u64, Arc<IoRequest>> = HashMap::with_capacity(queue_depth);

    loop {
        // Process completions
        if let Err(e) = process_completions(&mut ring, &mut pending) {
            log::error!("Error processing completions: {}", e);
        }

        // Submit new requests from channel
        match request_rx.recv_timeout(Duration::from_millis(10)) {
            Ok(request) => {
                if let Err(e) = submit_request(&mut ring, &mut pending, request) {
                    log::error!("Error submitting request: {}", e);
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                // No new requests, continue processing completions
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                log::info!("io_uring thread shutting down");
                break;
            }
        }
    }
}

/// Submit a read request to io_uring.
fn submit_request(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, Arc<IoRequest>>,
    request: Arc<IoRequest>,
) -> io::Result<()> {
    // Generate unique user_data
    let user_data = USER_DATA_COUNTER.fetch_add(1, Ordering::Relaxed);

    // Get buffer pointer from request state
    let buffer_ptr = {
        let state = request.state.lock().unwrap();
        state.buffer.as_ptr() as *mut u8
    };

    // Prepare read operation
    let read_op = opcode::Read::new(types::Fd(request.fd), buffer_ptr, request.length as u32)
        .offset(request.offset);

    // Submit to ring
    unsafe {
        let mut sq = ring.submission();
        sq.push(&read_op.build().user_data(user_data))
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to push to SQ"))?;
    }

    // Track request
    pending.insert(user_data, request);

    // Submit
    ring.submit()?;

    Ok(())
}

/// Process all available completions.
fn process_completions(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, Arc<IoRequest>>,
) -> io::Result<()> {
    for cqe in ring.completion() {
        let user_data = cqe.user_data();
        let result = cqe.result();

        // Look up request
        if let Some(request) = pending.remove(&user_data) {
            let mut state = request.state.lock().unwrap();
            state.completed = true;

            // Handle result
            if result < 0 {
                state.err = Some(io::Error::from_raw_os_error(-result));
            }

            // Wake future
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
        } else {
            log::warn!("Received completion for unknown user_data: {}", user_data);
        }
    }

    Ok(())
}

/// Pin the current thread to a specific CPU core if LANCE_URING_CORE is set.
fn pin_to_core() {
    if let Some(core_id) = std::env::var("LANCE_URING_CORE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        #[cfg(target_os = "linux")]
        unsafe {
            let mut cpu_set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(core_id, &mut cpu_set);
            let result =
                libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpu_set);

            if result != 0 {
                log::warn!(
                    "Failed to pin io_uring thread to core {}: errno {}",
                    core_id,
                    io::Error::last_os_error()
                );
            } else {
                log::info!("io_uring thread pinned to CPU core {}", core_id);
            }
        }
    }
}
