//! Per-message AMQP properties, taken as steps on a live publisher.
//!
//! [`LapinPublishExt`] sets the `priority` and `expiration` (TTL) properties on the frame, before
//! the publish builder starts. A header of either name does not reach them: it travels in the AMQP
//! header table, which the broker reads for neither purpose.
//!
//! ```text
//! publisher.with_priority(3).message(&order).publish().await?;
//! ```

use std::time::Duration;

use lapin::types::ShortString;
use ruststream::runtime::{OutSlot, SlotPublisher};
use ruststream::{OutgoingMessage, Publisher, TransactionalPublisher};

use crate::convert;

pub(crate) use self::sealed::NativePublish;

/// The per-message AMQP properties a step carries; an unset one leaves the property off the
/// frame.
///
/// Reachable only through the sealed [`NativePublish`], so nothing outside this crate can
/// construct or read one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageProperties {
    pub(crate) priority: Option<u8>,
    pub(crate) expiration: Option<ShortString>,
}

mod sealed {
    use std::future::Future;

    use ruststream::{OutgoingMessage, Publisher};

    use super::MessageProperties;

    /// Publishing an [`OutgoingMessage`] with per-message AMQP properties written onto the frame.
    ///
    /// Implemented for this crate's live publishers and the `Out` slot wrapper around them, and
    /// unnameable outside the crate, so it seals [`LapinPublishExt`](super::LapinPublishExt).
    pub trait NativePublish: Publisher {
        /// Publishes `msg`, applying `properties` to the AMQP frame.
        fn publish_native(
            &self,
            msg: OutgoingMessage<'_>,
            properties: &MessageProperties,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    }
}

/// A publisher with per-message AMQP properties attached, produced by the steps of
/// [`LapinPublishExt`].
///
/// It is a [`Publisher`] itself: `message(..)` follows the step as it would on the publisher,
/// and the steps chain, each filling its own property.
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
    properties: MessageProperties,
}

impl<'a, P: NativePublish + ?Sized> WithProperties<'a, P> {
    fn new(inner: &'a P) -> Self {
        Self {
            inner,
            properties: MessageProperties::default(),
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
        self.properties.priority = Some(priority);
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
        self.properties.expiration = Some(convert::expiration_millis(ttl));
        self
    }
}

impl<P: NativePublish + ?Sized> Publisher for WithProperties<'_, P> {
    type Error = P::Error;

    /// Publishes `msg` with this step's properties written onto the AMQP frame.
    ///
    /// # Errors
    ///
    /// Whatever the publisher underneath reports; a step adds no failure of its own.
    ///
    /// # Cancel safety
    ///
    /// As cancel safe as the publisher underneath.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.inner.publish_native(msg, &self.properties).await
    }
}

/// Begin and commit reach the publisher underneath, and every message buffered through the step
/// keeps its properties.
impl<P: NativePublish + TransactionalPublisher + ?Sized> TransactionalPublisher
    for WithProperties<'_, P>
{
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
/// A header named `priority` or `expiration` does not set these properties: headers travel in the
/// AMQP header table, which the broker reads for neither purpose.
///
/// Where a step does not reach:
///
/// * an owned transaction ([`OwnedTransactions`](ruststream::OwnedTransactions)) - take the step
///   between `begin` and `commit` on the borrowed form ([`TransactionalPublisher`]) instead, which
///   keeps the properties;
/// * a publish inside a `TestApp`-driven handler, which the [`Out`](ruststream::runtime::Out)
///   slot does not attribute to the slot, and which the in-process test broker carries nowhere -
///   assert on properties against a real broker.
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
    note = "the publish steps live on this crate's live publishers: attach a `LapinPublish` \
            policy (or one of its transitions) at the include site, and bound an `Out` slot \
            with `LapinPublishExt` to take a step inside a handler"
)]
pub trait LapinPublishExt: NativePublish {
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

impl<P: NativePublish + ?Sized> LapinPublishExt for P {}

/// Carries the steps onto an [`Out`](ruststream::runtime::Out) slot, so a handler bounding its
/// slot with [`LapinPublishExt`] takes them on the injected publisher.
impl<P: NativePublish, M: OutSlot> NativePublish for SlotPublisher<P, M> {
    async fn publish_native(
        &self,
        msg: OutgoingMessage<'_>,
        properties: &MessageProperties,
    ) -> Result<(), Self::Error> {
        self.inner().publish_native(msg, properties).await
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, ready};
    use std::sync::Mutex;

    use ruststream::runtime::PublishExt;
    use ruststream::{Outgoing, Serialized};

    use super::{
        Duration, LapinPublishExt, MessageProperties, NativePublish, OutgoingMessage, Publisher,
        ShortString,
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

    /// A publisher that keeps what the step handed it.
    #[derive(Debug, Default)]
    struct Recorder(Mutex<Vec<MessageProperties>>);

    impl Recorder {
        fn seen(&self) -> Vec<MessageProperties> {
            self.0.lock().expect("recorder mutex poisoned").clone()
        }
    }

    impl Publisher for Recorder {
        type Error = AmqpError;

        async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
            self.publish_native(msg, &MessageProperties::default())
                .await
        }
    }

    impl NativePublish for Recorder {
        fn publish_native(
            &self,
            _msg: OutgoingMessage<'_>,
            properties: &MessageProperties,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            self.0
                .lock()
                .expect("recorder mutex poisoned")
                .push(properties.clone());
            ready(Ok(()))
        }
    }

    fn expiration_of(properties: &MessageProperties) -> Option<&str> {
        properties.expiration.as_ref().map(ShortString::as_str)
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
        assert_eq!(seen[0].priority, Some(3));
        assert_eq!(expiration_of(&seen[0]), Some("30000"));
        assert_eq!(seen[1].priority, Some(9));
        assert_eq!(expiration_of(&seen[1]), Some("1500"));
        assert_eq!(
            seen[2],
            MessageProperties::default(),
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
        assert_eq!(seen[0].priority, Some(5));
        assert_eq!(expiration_of(&seen[0]), Some("1000"));
    }
}
