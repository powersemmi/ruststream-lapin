//! The channel a live handle keeps: opened on first use, opened again once the broker closes it.
//!
//! A channel-level error closes the channel and leaves the connection up - publishing to an
//! exchange that does not exist is enough - so a handle that kept its first channel for its
//! lifetime would fail every later call on it, with a restart as the only way back. Opening
//! lazily is the other half of the same cell: pairing a publisher is a synchronous constructor
//! call, and a publisher that never publishes should hold no channel at all.

use std::fmt;
use std::future::Future;
use std::sync::Mutex;

use lapin::Channel;

use crate::error::AmqpError;

/// What a cell holds: a channel, or a value built around one.
///
/// The requester keeps a dispatch task alongside its channel, so the cell stores the pair and
/// reads the channel's state through this.
pub(crate) trait Holds: Clone {
    fn channel(&self) -> &Channel;
}

impl Holds for Channel {
    fn channel(&self) -> &Self {
        self
    }
}

/// A channel-backed value opened on demand and replaced once the one held is gone.
pub(crate) struct ChannelCell<T> {
    held: Mutex<Option<T>>,
}

impl<T: Holds> ChannelCell<T> {
    pub(crate) const fn new() -> Self {
        Self {
            held: Mutex::new(None),
        }
    }

    /// The held value while its channel can still carry a frame, whether or not one is held.
    ///
    /// The value is a handle, so the clone costs a refcount and lets the lock go before the call
    /// that uses it.
    pub(crate) fn live(&self) -> Option<T> {
        let held = self.held.lock().expect("channel cell mutex poisoned");
        held.clone()
            .filter(|live| live.channel().status().connected())
    }

    /// Whatever is held, live or not.
    ///
    /// For the one caller that must not silently start again: an AMQP server transaction lives in
    /// its channel, so a channel that died took the transaction with it, and a fresh one would
    /// commit nothing while reporting success.
    pub(crate) fn held(&self) -> Option<T> {
        self.held
            .lock()
            .expect("channel cell mutex poisoned")
            .clone()
    }

    /// The held value, opening one with `open` when there is none or the broker closed the one
    /// there was.
    ///
    /// # Errors
    ///
    /// Returns whatever `open` reports when the channel cannot be opened or set up.
    pub(crate) async fn get<Open, Opening>(&self, open: Open) -> Result<T, AmqpError>
    where
        Open: FnOnce() -> Opening,
        Opening: Future<Output = Result<T, AmqpError>>,
    {
        if let Some(live) = self.live() {
            return Ok(live);
        }

        let opened = open().await?;
        let shared = {
            let mut held = self.held.lock().expect("channel cell mutex poisoned");
            // Another task may have opened one while this one was setting its own up; that one is
            // the handle's channel, and this one goes away with the value.
            if !held
                .as_ref()
                .is_some_and(|live| live.channel().status().connected())
            {
                *held = Some(opened);
            }
            held.clone().expect("the cell was filled just above")
        };
        Ok(shared)
    }
}

impl<T> fmt::Debug for ChannelCell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelCell").finish_non_exhaustive()
    }
}
