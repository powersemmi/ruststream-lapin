//! The broker's in-process mode, behind the `testing` feature: the transport a connected broker
//! carries when the test harness connects it through `InProcess::connect_in_process` rather than
//! through `connect`.
//!
//! The connected broker, its subscriber, its publishers and its delivery type each carry this
//! transport as a variant of their own, so a service's routes, descriptors and publish policies
//! run against it unchanged. It has no configuration of its own: it reads the production broker's
//! settings, and it frames a publish with the same conversion a live publish goes through. It
//! never succeeds where a server fails. A publish, an acknowledgement or a subscription the server
//! refuses (a name the protocol cannot carry, a header it cannot frame, a declaration it refuses, a
//! handle outliving its connection) is refused here with the same error.
//!
//! What it models: the default exchange and the direct, topic, fanout and headers exchanges
//! through the bindings the service's descriptors describe; competing consumers; acknowledgement,
//! rejection and requeue; a quorum queue's delivery count, delivery limit and dead-letter route;
//! the waiting queue of a native delayed redelivery; and direct reply-to. What belongs to the
//! server and is left to the live mode: a queue's storage while it has no consumer (a message no
//! consumer takes is dropped here), the prefetch window, publisher confirms, the atomicity of a
//! server transaction, a plugin exchange's routing, and a topology the service expects to find
//! but did not declare (a subscription here finds every queue it names).

mod bus;
mod deliveries;
mod matching;
mod request;

use std::sync::Arc;

pub(crate) use bus::{Bus, BusDelivery};
pub(crate) use deliveries::{BusDeliveries, QueueBehaviour, Settlement};
pub(crate) use request::request;

use crate::convert;
use crate::delay::DelayTarget;
use crate::error::AmqpError;
use crate::queue::{QueueDescriptor, declared_arguments};
use crate::subscriber::LapinSubscriber;

/// Opens a subscription for `def` on the in-process transport, declaring its topology first when
/// the broker declares topology, and refusing what a server refuses.
///
/// # Errors
///
/// Returns [`AmqpError::InvalidOptions`] for an empty queue name, a name the protocol cannot
/// carry, and a retry declaration the queue cannot carry; [`AmqpError::Declare`] for a
/// declaration a server refuses; and [`AmqpError::Closed`] once the connection has shut down.
pub(crate) fn subscribe(
    bus: &Arc<Bus>,
    def: &impl QueueDescriptor,
    declares: bool,
) -> Result<LapinSubscriber, AmqpError> {
    let spec = def.spec();
    def.check_retry(declares)?;
    if spec.name.is_empty() {
        return Err(AmqpError::InvalidOptions(
            "queue name must not be empty; subscribe with the queue the handler consumes"
                .to_owned(),
        ));
    }
    convert::short(&spec.name, "queue name")?;
    bus.ensure_live(&spec.name)?;
    if let Some(delay) = &spec.delay {
        match delay.target_for(&spec.name) {
            DelayTarget::WaitingQueue { waiting_queue } => {
                convert::short(&waiting_queue, "waiting queue name")?;
            }
            #[cfg(feature = "plugin-dme")]
            DelayTarget::DelayedExchange { exchange, .. } => {
                convert::short(&exchange, "delayed exchange name")?;
            }
        }
    }
    let arguments = declared_arguments(spec, def.declared_retry());
    bus.describe(spec, &arguments, declares)?;
    let behaviour = QueueBehaviour::of(spec, &arguments);
    Ok(LapinSubscriber::in_process(
        BusDeliveries::open(bus, spec.name.clone(), behaviour),
        spec.name.clone(),
        spec.batch_wait,
    ))
}
