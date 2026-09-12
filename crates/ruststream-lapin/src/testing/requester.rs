//! In-process request/reply: the [`LapinRequest`] policy paired against the test broker.

use std::future::{Future, ready};
use std::sync::Arc;
use std::time::Duration;

use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher, RequestReply};
use tokio::time::{Instant, timeout_at};
use tracing::debug;

use super::broker::{ConnectedLapinTestBroker, TestBrokerState};
use super::publisher::{LapinTestPublishPolicy, Routed};
use super::router::{DeliveryReceiver, SubscriptionId};
use super::subscriber::LapinTestMessage;
use crate::error::AmqpError;
use crate::publish_step::LapinPublishOptions;
use crate::requester::{LapinRequest, REPLY_TO};

/// The private reply address of one in-flight request.
///
/// `RabbitMQ` rewrites the direct reply-to pseudo-queue per request, so a responder sees an
/// address of its own for every question it is asked; the transport does the same, with a
/// subscription that lives exactly as long as the request waiting on it.
struct Inbox {
    state: Arc<TestBrokerState>,
    id: SubscriptionId,
    address: String,
    correlation_id: String,
    replies: DeliveryReceiver,
    next_tag: u64,
}

impl Inbox {
    fn open(state: &Arc<TestBrokerState>) -> Self {
        let seq = state.next_inbox();
        let address = format!("{REPLY_TO}.{seq}");
        let (id, sender, replies) = state.router.subscribe(address.clone());
        // The router keeps its own clone: a second one here would hold the reply channel open
        // past a shutdown that cleared the subscription table, and the wait below would read
        // that as silence rather than as a closed transport.
        drop(sender);
        Self {
            state: Arc::clone(state),
            id,
            address,
            correlation_id: format!("rs-{seq}"),
            replies,
            next_tag: 1,
        }
    }

    /// Balances the coordinator for a delivery this request does not hand back.
    fn discard(&self) {
        if let Some(coordinator) = self.state.coordinator() {
            coordinator.consumed();
        }
    }
}

impl Drop for Inbox {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
        // A reply that arrived while the request was giving up was counted in on the way to this
        // channel; draining it keeps the harness's in-flight accounting balanced.
        while self.replies.try_recv().is_ok() {
            self.discard();
        }
    }
}

/// The in-process stand-in for [`LapinRequester`](crate::LapinRequester).
///
/// Paired from [`LapinRequest`], so a mount site keeps the production policy. The convention is
/// reproduced whole: every request goes out with a `reply-to` header naming a private address and
/// a generated `correlation-id`, a responder (usually the crate's
/// [`DirectReplyTo`](crate::DirectReplyTo) transform) publishes the reply to that address echoing
/// the id, and a reply that does not correlate is dropped rather than resolving the call. An
/// unanswered request fails with [`AmqpError::RequestTimeout`], which is the same failure
/// boundary a handler codes against on a server.
///
/// What is not reproduced is the transport underneath the convention. Direct reply-to on a server
/// is at-most-once channel state on one broker node, so a lost connection loses in-flight replies
/// and a responder that publishes to a stale address gets nothing; here the addresses are router
/// subscriptions, which cannot fail that way. The at-most-once behaviour belongs to a live
/// server, and the crate's integration tests exercise it there.
///
/// Like every live handle it aliases the transport and may outlive it: after the broker shuts
/// down every request reports [`AmqpError::Closed`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use futures::StreamExt;
/// use ruststream::{
///     Broker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher, RequestReply, Subscriber,
/// };
/// use ruststream_lapin::testing::LapinTestBroker;
/// use ruststream_lapin::{LapinPublish, LapinRequest};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let broker = LapinTestBroker::new().connect().await?;
/// let mut inventory = broker.subscribe("inventory.check").await?;
/// let publisher = broker.publisher(LapinPublish::default());
/// let requester = broker.requester(LapinRequest::default());
///
/// let respond = async {
///     let mut stream = std::pin::pin!(inventory.stream());
///     let ask = stream.next().await.expect("a request arrives")?;
///     let reply_to = ask.headers().reply_to().expect("a private reply address").to_owned();
///     let mut headers = HeaderMap::new();
///     headers.insert("correlation-id", ask.headers().correlation_id().unwrap_or("").to_owned());
///     publisher
///         .publish(OutgoingMessage::new(&reply_to, b"in stock").with_headers(headers), None)
///         .await?;
///     ask.ack().await?;
///     Ok::<_, Box<dyn std::error::Error>>(())
/// };
/// let ask = requester.request(
///     OutgoingMessage::new("inventory.check", b"widget"),
///     Duration::from_secs(1),
/// );
///
/// let (reply, responded) = futures::join!(ask, respond);
/// responded?;
/// assert_eq!(reply?.payload(), b"in stock");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct LapinTestRequester {
    route: Routed,
}

impl Publisher for LapinTestRequester {
    type Error = AmqpError;
    type Options = LapinPublishOptions;

    /// Publishes `msg` without expecting a reply, like the live requester's plain publish.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::InvalidOptions`] when the routing key is empty and
    /// [`AmqpError::Closed`] once the transport has shut down.
    fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.route.send(&msg, options))
    }
}

impl RequestReply for LapinTestRequester {
    type Reply = LapinTestMessage;

    /// Sends `msg` to a private reply address and awaits the correlated reply.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::RequestTimeout`] when no correlated reply arrives within `timeout`,
    /// [`AmqpError::InvalidOptions`] when the routing key is empty, and [`AmqpError::Closed`]
    /// once the transport has shut down, including a shutdown that lands while the request is
    /// waiting.
    ///
    /// # Cancel safety
    ///
    /// Cancel safe: dropping the future closes the reply address, and a reply that arrives after
    /// that is discarded.
    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let mut inbox = Inbox::open(self.route.state());

        let mut headers = msg.headers().clone();
        headers.insert("reply-to", inbox.address.clone());
        headers.insert("correlation-id", inbox.correlation_id.clone());
        // The request itself has no call site to adjust settings: the policy's are the whole
        // answer, which is what the live requester does too.
        self.route.send(
            &OutgoingMessage::new(msg.name(), msg.payload()).with_headers(headers),
            None,
        )?;

        // One deadline for the whole wait, not per delivery: an uncorrelated reply must not buy
        // the request another full timeout.
        let deadline = Instant::now() + timeout;
        loop {
            let Ok(received) = timeout_at(deadline, inbox.replies.recv()).await else {
                return Err(AmqpError::RequestTimeout(timeout));
            };
            let Some(delivery) = received else {
                // The router dropped its sender: the transport shut down under the request.
                return Err(AmqpError::closed(&inbox.address));
            };
            if delivery.headers.correlation_id() == Some(inbox.correlation_id.as_str()) {
                let delivery_tag = inbox.next_tag;
                inbox.next_tag += 1;
                return Ok(LapinTestMessage::from_reply(
                    &inbox.state,
                    inbox.address.clone(),
                    delivery_tag,
                    delivery,
                ));
            }
            // Same as the live requester, which multiplexes one reply consumer across requests:
            // a delivery that does not correlate is not this request's answer.
            debug!(
                target: "ruststream_lapin",
                address = inbox.address,
                "dropping a reply that does not correlate with the pending request"
            );
            inbox.discard();
        }
    }
}

impl PublishPolicy<ConnectedLapinTestBroker> for LapinRequest {
    type Live = LapinTestRequester;

    fn pair(
        self,
        connected: &ConnectedLapinTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(LapinTestPublishPolicy::bind(self, connected)))
    }
}

impl LapinTestPublishPolicy for LapinRequest {
    fn bind(self, connected: &ConnectedLapinTestBroker) -> Self::Live {
        LapinTestRequester {
            route: Routed::new(connected, self.publish_options().clone()),
        }
    }
}
