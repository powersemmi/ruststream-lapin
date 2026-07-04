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
/// use ruststream::runtime::TypedPublisher;
/// use ruststream_lapin::{DirectReplyTo, LapinBroker};
///
/// let broker = LapinBroker::new("amqp://localhost:5672");
/// let replies = TypedPublisher::new(broker.publisher()).transform(DirectReplyTo);
/// # let _ = replies;
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
