//! Per-message AMQP basic properties: the policy's defaults, and the steps that adjust one
//! message.
//!
//! [`LapinPublishOptions`] is what one publish may differ from the next in - the `priority`, the
//! per-message `expiration` (TTL), and the delivery mode. Every field is optional: what a call
//! leaves alone keeps what the publish policy fixed at the mount site.
//!
//! ```text
//! b.include(expedite).out(Alerts, Publish::default().priority(3));  // the default
//! alerts.message(&alert).to("alerts").priority(9).publish().await?; // this one message
//! ```
//!
//! [`LapinPublishSteps`] puts those steps on the publish builder, bounded on the options type, so
//! they appear over this crate's publishers and over no others. Nothing wraps the publisher: the
//! publish still leaves through the mount site's own entry, with the codec and the transforms that
//! entry named, and the test harness still attributes it to its [`Out`](ruststream::runtime::Out)
//! slot.
//!
//! The values reach the frame as the protocol fields they are, never as headers. Written into the
//! header table under the protocol's own names (`priority`, `expiration`) they would travel as
//! table entries `RabbitMQ` reads for neither purpose; that quiet failure is what the steps exist
//! to prevent.

use std::time::Duration;

use ruststream::runtime::{PublishBuilder, PublishSink};

/// Header carrying the AMQP `priority` property of a DELIVERY.
///
/// A delivery reports the property under this name, so a handler reads it from the message
/// headers. Publishing under it sets no property: the priority of an outgoing message is
/// [`LapinPublishSteps::priority`] or the policy's own
/// [`LapinPublish::priority`](crate::LapinPublish::priority).
///
/// The property only orders deliveries on a queue declared with `x-max-priority`; elsewhere the
/// broker carries it to the consumer and nothing more.
pub const PRIORITY_HEADER: &str = "amqp-priority";

/// Header carrying the AMQP per-message `expiration` property of a DELIVERY: whole milliseconds,
/// as decimal digits, which is how AMQP spells a TTL.
///
/// A delivery reports the property under this name. Publishing under it sets no property: the TTL
/// of an outgoing message is [`LapinPublishSteps::expiration`] or the policy's own
/// [`LapinPublish::expiration`](crate::LapinPublish::expiration).
///
/// The broker drops the message once the TTL has passed without it being consumed, dead-lettering
/// it when the queue says so.
pub const EXPIRATION_HEADER: &str = "amqp-expiration";

/// The AMQP basic properties one publish may differ from the next in.
///
/// Every field is optional because a publish carries only what its call site adjusted; the rest is
/// what the publish policy fixed when it paired the publisher. The policy carries the defaults
/// ([`LapinPublish::priority`](crate::LapinPublish::priority),
/// [`expiration`](crate::LapinPublish::expiration),
/// [`persistent`](crate::LapinPublish::persistent)), the steps of [`LapinPublishSteps`] adjust one
/// message, and the publisher resolves the two before it builds the frame.
///
/// A test reads the value back from the slot view: `tb.out::<Alerts>().with_options(..)` takes
/// this type, which is why it derives `Debug` and `PartialEq`.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "testing")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use ruststream::OutSlot;
/// use ruststream::testing::TestApp;
/// use ruststream_lapin::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
/// struct Alert {
///     level: u8,
/// }
///
/// #[derive(OutSlot)]
/// #[publishes(Alert)]
/// struct Alerts;
///
/// #[subscriber("alerts.raw")]
/// async fn escalate(
///     alert: &Alert,
///     Out(alerts): Out<impl Publisher<Options = LapinPublishOptions>, Alerts>,
/// ) -> HandlerOutcome {
///     let sent = alerts.message(alert).to("alerts").priority(alert.level).publish().await;
///     if sent.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// let app = RustStream::new(AppInfo::new("alerts", "0.1.0")).with_broker(
///     LapinBroker::new("amqp://localhost:5672"),
///     |b| {
///         b.include(escalate).out(Alerts, Publish::default()).build();
///     },
/// );
/// let tb = TestApp::start(app).await?;
///
/// tb.broker::<LapinBroker>()
///     .message(&Alert { level: 9 })
///     .to("alerts.raw")
///     .publish()
///     .await?;
///
/// tb.out::<Alerts>()
///     .assert_called_once()
///     .with_options(&LapinPublishOptions {
///         priority: Some(9),
///         ..LapinPublishOptions::default()
///     });
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LapinPublishOptions {
    /// The AMQP `priority` property (0..=255), or `None` to keep the policy's.
    pub priority: Option<u8>,
    /// The AMQP per-message `expiration` (TTL), or `None` to keep the policy's. AMQP counts it in
    /// whole milliseconds; a shorter non-zero value becomes one millisecond.
    pub expiration: Option<Duration>,
    /// Whether the message is marked persistent (delivery mode 2), or `None` to keep the policy's.
    pub persistent: Option<bool>,
}

impl LapinPublishOptions {
    /// What a publish with neither a call site nor a policy carries: persistent, nothing else.
    ///
    /// The native delayed redelivery publishes a copy on the delivery's own channel, outside any
    /// policy, and a copy that outlives a broker restart is what a delay is for.
    pub(crate) const PERSISTENT: Self = Self {
        priority: None,
        expiration: None,
        persistent: Some(true),
    };

    /// This value's fields where it has them, `defaults` where it does not.
    ///
    /// What a publisher does first: the call site's options over the policy's.
    #[must_use]
    pub(crate) fn over(&self, defaults: &Self) -> Self {
        Self {
            priority: self.priority.or(defaults.priority),
            expiration: self.expiration.or(defaults.expiration),
            persistent: self.persistent.or(defaults.persistent),
        }
    }
}

/// The AMQP basic properties, as steps on the publish builder.
///
/// A step adjusts one field for one message and hands the builder back, so a publish reads as one
/// chain. It reaches every publish surface this crate ships - a publisher, an
/// [`Out`](ruststream::runtime::Out) slot inside a handler, a transaction scope - because it is
/// bounded on the options type rather than on a publisher type.
///
/// A handler body that takes a step is the one place a body names this crate: bound the slot
/// `Out<impl Publisher<Options = LapinPublishOptions>, Marker>` and import this crate's prelude.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use ruststream_lapin::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
///     expedited: bool,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "shipments")]
/// struct Shipment {
///     order_id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn ship(
///     order: &Order,
///     Out(shipments): Out<impl Publisher<Options = LapinPublishOptions>>,
/// ) -> HandlerOutcome {
///     let shipment = Shipment { order_id: order.id };
///     let sent = if order.expedited {
///         shipments
///             .message(&shipment)
///             .priority(9)
///             .expiration(Duration::from_secs(30))
///             .publish()
///             .await
///     } else {
///         shipments.message(&shipment).publish().await
///     };
///     if sent.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// // Every shipment takes priority 3 from the policy; an expedited one overrides it.
/// fn app() -> RustStream {
///     let broker = LapinBroker::new("amqp://localhost:5672");
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
///         b.include(ship)
///             .out(DefaultSlot, Publish::default().priority(3))
///             .build();
///     })
/// }
/// ```
pub trait LapinPublishSteps {
    /// Sends this one message with the AMQP `priority` property set, whatever the policy's
    /// default is.
    ///
    /// The property only orders deliveries on a queue declared with `x-max-priority`; elsewhere
    /// the broker carries it to the consumer and nothing more.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_lapin::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Ticket {
    ///     id: u64,
    ///     vip: bool,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// #[outgoing(name = "support.queue")]
    /// struct Escalation {
    ///     ticket: u64,
    /// }
    ///
    /// #[subscriber("tickets")]
    /// async fn escalate(
    ///     ticket: &Ticket,
    ///     Out(support): Out<impl Publisher<Options = LapinPublishOptions>>,
    /// ) -> HandlerOutcome {
    ///     let priority = if ticket.vip { 9 } else { 1 };
    ///     let escalation = Escalation { ticket: ticket.id };
    ///     let sent = support.message(&escalation).priority(priority).publish().await;
    ///     if sent.is_err() {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    /// ```
    #[must_use]
    fn priority(self, priority: u8) -> Self;

    /// Sends this one message with the AMQP per-message `expiration` (TTL) set: the broker drops
    /// it once `ttl` has passed without it being consumed, dead-lettering it when the queue says
    /// so.
    ///
    /// AMQP counts the TTL in whole milliseconds; a shorter non-zero `ttl` becomes one
    /// millisecond.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use ruststream_lapin::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct QuoteRequest {
    ///     sku: String,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// #[outgoing(name = "quotes")]
    /// struct Quote {
    ///     sku: String,
    ///     cents: u64,
    /// }
    ///
    /// // A price is only good for half a minute; nobody should act on a stale one.
    /// #[subscriber("quote.requests")]
    /// async fn price(
    ///     request: &QuoteRequest,
    ///     Out(quotes): Out<impl Publisher<Options = LapinPublishOptions>>,
    /// ) -> HandlerOutcome {
    ///     let quote = Quote {
    ///         sku: request.sku.clone(),
    ///         cents: 1_999,
    ///     };
    ///     let sent = quotes
    ///         .message(&quote)
    ///         .expiration(Duration::from_secs(30))
    ///         .publish()
    ///         .await;
    ///     if sent.is_err() {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    /// ```
    #[must_use]
    fn expiration(self, ttl: Duration) -> Self;

    /// Sends this one message persistent (delivery mode 2) or transient (1), whatever the
    /// policy's default is.
    ///
    /// A persistent message on a durable queue survives a broker restart. The publish policies
    /// default to persistent; [`LapinRequest`](crate::LapinRequest) defaults to transient,
    /// because a request nobody is waiting for after the timeout gains nothing from surviving a
    /// restart.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_lapin::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// #[outgoing(name = "metrics")]
    /// struct Sample {
    ///     order_id: u64,
    /// }
    ///
    /// // The order is the record; a lost sample after a broker restart costs nothing.
    /// #[subscriber("orders")]
    /// async fn measure(
    ///     order: &Order,
    ///     Out(metrics): Out<impl Publisher<Options = LapinPublishOptions>>,
    /// ) -> HandlerOutcome {
    ///     let sample = Sample { order_id: order.id };
    ///     let sent = metrics.message(&sample).persistent(false).publish().await;
    ///     if sent.is_err() {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    /// ```
    #[must_use]
    fn persistent(self, persistent: bool) -> Self;
}

impl<Sink, Body, Enc, Hdrs, Dest> LapinPublishSteps for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = LapinPublishOptions>,
{
    fn priority(mut self, priority: u8) -> Self {
        self.options_mut()
            .get_or_insert_with(LapinPublishOptions::default)
            .priority = Some(priority);
        self
    }

    fn expiration(mut self, ttl: Duration) -> Self {
        self.options_mut()
            .get_or_insert_with(LapinPublishOptions::default)
            .expiration = Some(ttl);
        self
    }

    fn persistent(mut self, persistent: bool) -> Self {
        self.options_mut()
            .get_or_insert_with(LapinPublishOptions::default)
            .persistent = Some(persistent);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{Duration, LapinPublishOptions};

    #[test]
    fn a_call_site_field_wins_over_the_policy_and_the_rest_is_kept() {
        let defaults = LapinPublishOptions {
            priority: Some(3),
            expiration: Some(Duration::from_secs(60)),
            persistent: Some(true),
        };
        let call = LapinPublishOptions {
            priority: Some(9),
            ..LapinPublishOptions::default()
        };

        let resolved = call.over(&defaults);
        assert_eq!(resolved.priority, Some(9));
        assert_eq!(resolved.expiration, Some(Duration::from_secs(60)));
        assert_eq!(resolved.persistent, Some(true));
    }

    #[test]
    fn a_publish_that_adjusted_nothing_is_the_policy_alone() {
        let defaults = LapinPublishOptions {
            priority: Some(3),
            ..LapinPublishOptions::default()
        };

        assert_eq!(LapinPublishOptions::default().over(&defaults), defaults);
    }
}
