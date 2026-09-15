//! The channel a live handle keeps: opened on first use, opened again once the broker closes it.
//!
//! A channel-level error closes the channel and leaves the connection up - publishing to an
//! exchange that does not exist is enough - so a handle that kept its first channel for its
//! lifetime would fail every later call on it, with a restart as the only way back. Opening
//! lazily is the other half of the same cell: pairing a publisher is a synchronous constructor
//! call, and a publisher that never publishes should hold no channel at all.
//!
//! Reading the channel is on the path of every message and replacing it happens once per broker
//! failure, so the two sides are split: the read is an atomic load, and the lock belongs to
//! whoever opens a channel, so two callers racing on a dead one do not open two.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use lapin::Channel;
use tokio::sync::Mutex;

use crate::error::AmqpError;

/// What a cell holds: a channel, or a value built around one.
///
/// The requester keeps a dispatch task alongside its channel, so the cell stores the pair and
/// reads the channel's state through this. A cell is shared by every clone of the handle that
/// owns it, which is what the bounds say.
pub(crate) trait Holds: Clone + Send + Sync {
    fn channel(&self) -> &Channel;
}

impl Holds for Channel {
    fn channel(&self) -> &Self {
        self
    }
}

/// A channel-backed value opened on demand and replaced once the one held is gone.
pub(crate) struct ChannelCell<T> {
    held: ArcSwapOption<T>,
    /// Held while a channel is being opened, and never on the read path.
    opening: Mutex<()>,
}

impl<T: Holds> ChannelCell<T> {
    /// An empty cell: the first call opens the channel.
    pub(crate) fn new() -> Self {
        Self {
            held: ArcSwapOption::empty(),
            opening: Mutex::new(()),
        }
    }

    /// A cell already holding `value`, for the connection's shared channel, which is opened
    /// together with the connection itself.
    pub(crate) fn holding(value: T) -> Self {
        Self {
            held: ArcSwapOption::from_pointee(value),
            opening: Mutex::new(()),
        }
    }

    /// The held value while its channel can still carry a frame, and `None` when there is none or
    /// the broker closed it.
    ///
    /// An atomic load and a state read: every publish goes through this, and the value is a
    /// handle, so the clone costs a refcount.
    pub(crate) fn live(&self) -> Option<T> {
        let held = self.held.load();
        let value = held.as_deref()?;
        value.channel().status().connected().then(|| value.clone())
    }

    /// Whatever is held, live or not.
    ///
    /// For the one caller that must not silently start again: an AMQP server transaction lives in
    /// its channel, so a channel that died took the transaction with it, and a fresh one would
    /// commit nothing while reporting success.
    pub(crate) fn held(&self) -> Option<T> {
        self.held.load().as_deref().cloned()
    }

    /// The held value, opening one with `open` when there is none or the broker closed the one
    /// there was.
    ///
    /// # Errors
    ///
    /// Returns whatever `open` reports when the channel cannot be opened or set up.
    pub(crate) async fn get<Open, Opening>(&self, open: Open) -> Result<T, AmqpError>
    where
        // The lock is held across the opening, so the future is only `Send` if the opening is.
        Open: FnOnce() -> Opening + Send,
        Opening: Future<Output = Result<T, AmqpError>> + Send,
    {
        if let Some(live) = self.live() {
            return Ok(live);
        }

        // One opener at a time: without it every caller that found the channel dead would build a
        // channel of its own and all but one would be dropped the moment it was ready.
        let _opening = self.opening.lock().await;
        if let Some(live) = self.live() {
            return Ok(live);
        }
        let opened = open().await?;
        self.held.store(Some(Arc::new(opened.clone())));
        Ok(opened)
    }
}

impl<T> fmt::Debug for ChannelCell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelCell").finish_non_exhaustive()
    }
}
