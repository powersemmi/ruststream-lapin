//! The responder half of the direct reply-to convention, packaged as a publish transform.

use ruststream::runtime::{Outgoing, PublishContext, PublishTransform};

/// Redirects each reply of a `#[subscriber(.., publish(..))]` handler to the requester's
/// private reply-to address, echoing its correlation id.
///
/// This is the canonical responder wiring for [request/reply over `RabbitMQ` direct
/// reply-to](crate::LapinRequester): compose it onto the reply publisher at mount time and the
/// handler stays a pure request-to-reply function. Requests without a `reply-to` header fall
/// through to the mount's static destination.
///
/// # Examples
///
/// ```
/// use ruststream_lapin::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Ask {
///     sku: String,
/// }
///
/// #[derive(Serialize)]
/// struct Stock {
///     available: bool,
/// }
///
/// #[subscriber("inventory.check", publish("inventory.check.unrouted"))]
/// async fn check(ask: &Ask) -> Stock {
///     Stock {
///         available: !ask.sku.is_empty(),
///     }
/// }
///
/// let broker = LapinBroker::new("amqp://localhost:5672");
/// let app = RustStream::new(AppInfo::new("inventory", "0.1.0")).with_broker(broker, |b| {
///     b.include(check)
///         .out(Reply, Publish::default())
///         .transform(DirectReplyTo);
/// });
/// # let _ = app;
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DirectReplyTo;

// --8<-- [start:transform]
impl<C> PublishTransform<C> for DirectReplyTo {
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
        if let Some(reply_to) = cx.headers().reply_to() {
            out.set_name(reply_to.to_owned());
        }
        if let Some(correlation_id) = cx.headers().correlation_id() {
            out.headers_mut()
                .insert("correlation-id", correlation_id.as_bytes().to_vec());
        }
    }
}
// --8<-- [end:transform]
