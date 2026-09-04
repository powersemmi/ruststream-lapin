//! Per-message AMQP properties, taken as steps on a live publisher.
//!
//! [`LapinPublishExt`] sets the `priority` and `expiration` (TTL) properties of what is published
//! through it. A step hands back a publisher the builder continues on, so a publish reads as one
//! chain:
//!
//! ```text
//! publisher.with_priority(3).message(&order).publish().await?;
//! ```
//!
//! The step carries each property as a [base header](ruststream::Publisher::base_headers) -
//! [`PRIORITY_HEADER`] and [`EXPIRATION_HEADER`] - which this crate's publishers write onto the
//! AMQP frame instead of into the header table. Riding the ordinary publish path is what keeps a
//! stepped publish attributed to its [`Out`](ruststream::runtime::Out) slot under the test
//! harness, and what lets a message assembled by hand carry the same properties.
//!
//! The protocol's own field names (`priority`, `expiration`) are NOT these headers: written
//! under those names both values travel in the AMQP header table, which the broker reads for
//! neither purpose. That quiet failure is what the steps exist to prevent.

use std::time::Duration;

use ruststream::runtime::{OutPipeline, OutSlot, Slot};
use ruststream::{HeaderMap, OutgoingMessage, Publisher, TransactionalPublisher};

use crate::convert;

/// Header carrying the AMQP `priority` property: whole decimal digits.
///
/// The property only orders deliveries on a queue declared with `x-max-priority`; elsewhere the
/// broker carries it to the consumer and nothing more. A delivery reports it back under the same
/// name.
pub const PRIORITY_HEADER: &str = "amqp-priority";

/// Header carrying the AMQP per-message `expiration` property: whole milliseconds, as decimal
/// digits, which is how AMQP spells a TTL.
///
/// The broker drops the message once the TTL has passed without it being consumed, dead-lettering
/// it when the queue says so. A delivery reports it back under the same name.
pub const EXPIRATION_HEADER: &str = "amqp-expiration";

/// A publisher with per-message AMQP properties attached, produced by the steps of
/// [`LapinPublishExt`].
///
/// It is a [`Publisher`] itself: `message(..)` follows the step as it would on the publisher,
/// and the steps chain, each filling its own property. The properties travel as the step's base
/// headers, so a header named at the call site wins over the step, exactly as the framework
/// merges any other base.
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
///
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Broker, Outgoing};
/// use ruststream_lapin::{LapinBroker, LapinPublish, LapinPublishExt};
/// use serde::Serialize;
///
/// #[derive(Outgoing, Serialize)]
/// #[outgoing(name = "orders")]
/// struct Order {
///     id: u64,
/// }
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let connected = LapinBroker::new("amqp://localhost:5672").connect().await?;
/// let publisher = connected.publisher(LapinPublish::default());
///
/// publisher
///     .with_priority(3)
///     .with_expiration(Duration::from_secs(30))
///     .message(&Order { id: 1 })
///     .publish()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
#[must_use = "a step carries its properties into a publish; nothing is sent until one runs"]
pub struct WithProperties<'a, P: ?Sized> {
    inner: &'a P,
    base: HeaderMap,
}

impl<'a, P: Publisher + ?Sized> WithProperties<'a, P> {
    fn new(inner: &'a P) -> Self {
        Self {
            // Seeded from the wrapped handle, so whatever it contributes to every message
            // survives the step.
            base: inner.base_headers().cloned().unwrap_or_default(),
            inner,
        }
    }

    /// Sets the AMQP `priority` property of what is published through this step.
    ///
    /// The property only orders deliveries on a queue declared with `x-max-priority`; elsewhere
    /// the broker carries it to the consumer and nothing more.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    ///
    /// use ruststream::runtime::PublishExt;
    /// use ruststream::{Broker, Outgoing};
    /// use ruststream_lapin::{LapinBroker, LapinPublish, LapinPublishExt};
    /// use serde::Serialize;
    ///
    /// #[derive(Outgoing, Serialize)]
    /// #[outgoing(name = "orders")]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// let connected = LapinBroker::new("amqp://localhost:5672").connect().await?;
    /// let publisher = connected.publisher(LapinPublish::default());
    ///
    /// publisher
    ///     .with_expiration(Duration::from_secs(30))
    ///     .with_priority(9)
    ///     .message(&Order { id: 1 })
    ///     .publish()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.base.insert(PRIORITY_HEADER, priority.to_string());
        self
    }

    /// Sets the AMQP per-message `expiration` property: the broker drops the message once `ttl`
    /// has passed without it being consumed (dead-lettering it when the queue says so).
    ///
    /// AMQP counts the TTL in whole milliseconds; a shorter non-zero `ttl` becomes one
    /// millisecond.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    ///
    /// use ruststream::runtime::PublishExt;
    /// use ruststream::{Broker, Outgoing};
    /// use ruststream_lapin::{LapinBroker, LapinPublish, LapinPublishExt};
    /// use serde::Serialize;
    ///
    /// #[derive(Outgoing, Serialize)]
    /// #[outgoing(name = "orders")]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// let connected = LapinBroker::new("amqp://localhost:5672").connect().await?;
    /// let publisher = connected.publisher(LapinPublish::default());
    ///
    /// publisher
    ///     .with_expiration(Duration::from_secs(30))
    ///     .message(&Order { id: 1 })
    ///     .publish()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_expiration(mut self, ttl: Duration) -> Self {
        self.base.insert(
            EXPIRATION_HEADER,
            convert::expiration_millis(ttl).as_str().to_owned(),
        );
        self
    }
}

impl<P: Publisher + ?Sized> Publisher for WithProperties<'_, P> {
    type Error = P::Error;

    /// Publishes `msg` through the publisher underneath.
    ///
    /// The properties reach the frame through [`base_headers`](Publisher::base_headers), which
    /// the publish builder merges under the call site's own headers. A message handed to this
    /// method directly is sent as it was built.
    ///
    /// # Errors
    ///
    /// Whatever the publisher underneath reports; a step adds no failure of its own.
    ///
    /// # Cancel safety
    ///
    /// As cancel safe as the publisher underneath.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.inner.publish(msg).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        Some(&self.base)
    }
}

/// Begin and commit reach the publisher underneath, and every message buffered through the step
/// keeps its properties.
impl<P: TransactionalPublisher + ?Sized> TransactionalPublisher for WithProperties<'_, P> {
    /// Opens the transaction on the publisher underneath.
    ///
    /// # Errors
    ///
    /// Whatever the publisher underneath reports.
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.inner.begin_transaction().await
    }

    /// Commits the transaction of the publisher underneath.
    ///
    /// # Errors
    ///
    /// Whatever the publisher underneath reports.
    async fn commit(&self) -> Result<(), Self::Error> {
        self.inner.commit().await
    }

    /// Aborts the transaction of the publisher underneath.
    ///
    /// # Errors
    ///
    /// Whatever the publisher underneath reports.
    async fn abort(&self) -> Result<(), Self::Error> {
        self.inner.abort().await
    }
}

/// The per-message AMQP properties, as steps on a live publisher.
///
/// Each step hands back a [`WithProperties`] the builder continues on, so a publish reads as one
/// chain:
///
/// ```text
/// publisher.with_priority(3).message(&command).publish().await?;
/// ```
///
/// Implemented for this crate's live publishers, the in-process test publisher and the
/// [`Out`](ruststream::runtime::Out) slot entry, so the same call works in a handler, in a
/// startup hook and under the test harness.
///
/// # Examples
///
/// ```no_run
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Broker, Outgoing};
/// use ruststream_lapin::{LapinBroker, LapinPublish, LapinPublishExt};
/// use serde::Serialize;
///
/// #[derive(Outgoing, Serialize)]
/// #[outgoing(name = "orders.expedited")]
/// struct Expedited {
///     id: u64,
/// }
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let connected = LapinBroker::new("amqp://localhost:5672").connect().await?;
/// let publisher = connected.publisher(LapinPublish::default());
///
/// publisher
///     .with_priority(9)
///     .message(&Expedited { id: 1 })
///     .publish()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a RabbitMQ publisher, so it has no per-message AMQP properties",
    note = "the publish steps live on this crate's live publishers: bind a `LapinPublish` \
            policy (or one of its transitions) to the slot at the include site with \
            `.out(marker, policy)`, and bound the `Out` parameter with `LapinPublishExt` to \
            take a step inside a handler"
)]
pub trait LapinPublishExt: Publisher {
    /// Publishes through this publisher with the AMQP `priority` property set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream::runtime::PublishExt;
    /// use ruststream::{Broker, Outgoing};
    /// use ruststream_lapin::{LapinBroker, LapinPublish, LapinPublishExt};
    /// use serde::Serialize;
    ///
    /// #[derive(Outgoing, Serialize)]
    /// #[outgoing(name = "orders")]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// let connected = LapinBroker::new("amqp://localhost:5672").connect().await?;
    /// let publisher = connected.publisher(LapinPublish::default());
    ///
    /// publisher.with_priority(9).message(&Order { id: 1 }).publish().await?;
    /// # Ok(())
    /// # }
    /// ```
    fn with_priority(&self, priority: u8) -> WithProperties<'_, Self> {
        WithProperties::new(self).with_priority(priority)
    }

    /// Publishes through this publisher with the AMQP per-message `expiration` (TTL) set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    ///
    /// use ruststream::runtime::PublishExt;
    /// use ruststream::{Broker, Outgoing};
    /// use ruststream_lapin::{LapinBroker, LapinPublish, LapinPublishExt};
    /// use serde::Serialize;
    ///
    /// #[derive(Outgoing, Serialize)]
    /// #[outgoing(name = "orders")]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// let connected = LapinBroker::new("amqp://localhost:5672").connect().await?;
    /// let publisher = connected.publisher(LapinPublish::default());
    ///
    /// publisher
    ///     .with_expiration(Duration::from_secs(30))
    ///     .message(&Order { id: 1 })
    ///     .publish()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    fn with_expiration(&self, ttl: Duration) -> WithProperties<'_, Self> {
        WithProperties::new(self).with_expiration(ttl)
    }
}

impl LapinPublishExt for crate::publisher::LapinPublisher {}
impl LapinPublishExt for crate::publisher::ConfirmsPublisher {}
impl LapinPublishExt for crate::publisher::ServerTxPublisher {}
#[cfg(feature = "testing")]
impl LapinPublishExt for crate::testing::LapinTestPublisher {}

// Grafted onto the slot entry a handler body actually holds, next to the framework's own
// capability delegations on it. Resolving the step there keeps the publish attributed to its
// slot; an impl one layer down would be reached by autoderef past the entry instead, and the
// publish would leave through the unwrapped publisher, where the harness's per-slot capture
// never sees it.
impl<M: OutSlot, W: LapinPublishExt, E: Send + Sync, Pipe: OutPipeline, Body> LapinPublishExt
    for Slot<M, W, E, Pipe, Body>
{
}

#[cfg(test)]
mod tests {
    use std::future::{Future, ready};
    use std::sync::Mutex;

    use ruststream::runtime::PublishExt;
    use ruststream::{HeaderMap, Outgoing, Serialized};

    use super::{
        Duration, EXPIRATION_HEADER, LapinPublishExt, OutgoingMessage, PRIORITY_HEADER, Publisher,
    };
    use crate::error::AmqpError;

    /// The payload the steps travel with: bytes under a name of their own, so the publish stays
    /// on the builder without dragging a codec into a test about AMQP properties.
    #[derive(Outgoing, Serialized)]
    struct Wire(Vec<u8>);

    impl Wire {
        fn empty_object() -> Self {
            Self(b"{}".to_vec())
        }
    }

    /// A publisher that keeps the headers each publish arrived with.
    #[derive(Debug, Default)]
    struct Recorder(Mutex<Vec<HeaderMap>>);

    impl Recorder {
        fn seen(&self) -> Vec<HeaderMap> {
            self.0.lock().expect("recorder mutex poisoned").clone()
        }
    }

    impl Publisher for Recorder {
        type Error = AmqpError;

        fn publish(
            &self,
            msg: OutgoingMessage<'_>,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            self.0
                .lock()
                .expect("recorder mutex poisoned")
                .push(msg.headers().clone());
            ready(Ok(()))
        }
    }

    impl LapinPublishExt for Recorder {}

    fn property(headers: &HeaderMap, name: &str) -> Option<String> {
        headers
            .get(name)
            .map(|value| String::from_utf8_lossy(value).into_owned())
    }

    #[tokio::test]
    async fn chained_steps_arrive_together_in_either_order() {
        let recorder = Recorder::default();

        recorder
            .with_priority(3)
            .with_expiration(Duration::from_secs(30))
            .message(&Wire::empty_object())
            .to("orders")
            .publish()
            .await
            .expect("publish");
        recorder
            .with_expiration(Duration::from_millis(1500))
            .with_priority(9)
            .message(&Wire::empty_object())
            .to("orders")
            .publish()
            .await
            .expect("publish");
        recorder
            .message(&Wire::empty_object())
            .to("orders")
            .publish()
            .await
            .expect("publish");

        let seen = recorder.seen();
        assert_eq!(property(&seen[0], PRIORITY_HEADER).as_deref(), Some("3"));
        assert_eq!(
            property(&seen[0], EXPIRATION_HEADER).as_deref(),
            Some("30000")
        );
        assert_eq!(property(&seen[1], PRIORITY_HEADER).as_deref(), Some("9"));
        assert_eq!(
            property(&seen[1], EXPIRATION_HEADER).as_deref(),
            Some("1500")
        );
        assert!(
            seen[2].is_empty(),
            "a publish without a step must leave both properties off the frame"
        );
    }

    #[tokio::test]
    async fn a_later_step_replaces_the_property_it_names() {
        let recorder = Recorder::default();

        recorder
            .with_priority(1)
            .with_expiration(Duration::from_secs(1))
            .with_priority(5)
            .message(&Wire::empty_object())
            .to("orders")
            .publish()
            .await
            .expect("publish");

        let seen = recorder.seen();
        assert_eq!(property(&seen[0], PRIORITY_HEADER).as_deref(), Some("5"));
        assert_eq!(
            property(&seen[0], EXPIRATION_HEADER).as_deref(),
            Some("1000")
        );
    }

    // The step is a base, so it loses to the call site on the key it names - the merge rule the
    // framework applies to every publisher's base headers.
    #[tokio::test]
    async fn the_call_site_wins_over_the_step() {
        let recorder = Recorder::default();
        let mut headers = HeaderMap::new();
        headers.insert(PRIORITY_HEADER, "1");

        recorder
            .with_priority(9)
            .message(&Wire::empty_object())
            .with_headers(headers)
            .to("orders")
            .publish()
            .await
            .expect("publish");

        let seen = recorder.seen();
        assert_eq!(property(&seen[0], PRIORITY_HEADER).as_deref(), Some("1"));
    }
}
