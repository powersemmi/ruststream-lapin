//! Transactional publishing: pick the guarantee on the publisher.
//!
//! Two `TransactionalPublisher` implementations share the same
//! `begin / publish / commit / abort` surface:
//!
//! - `.confirms()` buffers client-side and awaits every broker confirm on commit: durable and
//!   fast, the recommended default.
//! - `.server_tx()` uses AMQP channel transactions (`tx.select`): atomic visibility at commit,
//!   at the cost of a synchronous commit round trip.
//!
//! ```text
//! just brokers-up
//! cargo run --example lapin_transactions
//! ```

use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, IncomingMessage, OutgoingMessage, Publisher, Subscriber, TransactionalPublisher,
};
use ruststream_lapin::{LapinBroker, RabbitQueue};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    Broker::connect(&broker).await?;

    let mut subscriber = broker
        .subscribe(RabbitQueue::new("processed").durable(false).exclusive(true))
        .await?;

    // --8<-- [start:confirms]
    // Publisher confirms: both messages flush on commit, and commit resolves only after the
    // broker confirmed each one. An abort before commit publishes nothing.
    let publisher = broker.publisher().confirms();
    publisher.begin_transaction().await?;
    publisher
        .publish(OutgoingMessage::new("processed", b"one"))
        .await?;
    publisher
        .publish(OutgoingMessage::new("processed", b"two"))
        .await?;
    publisher.commit().await?;
    // --8<-- [end:confirms]

    // --8<-- [start:server-tx]
    // AMQP server transaction: the publish reaches the broker inside the channel transaction
    // and is rolled back by abort, so it is never delivered.
    let atomic = broker.publisher().server_tx();
    atomic.begin_transaction().await?;
    atomic
        .publish(OutgoingMessage::new("processed", b"discarded"))
        .await?;
    atomic.abort().await?;
    // --8<-- [end:server-tx]

    let mut stream = std::pin::pin!(subscriber.stream());
    for expected in [b"one".as_slice(), b"two"] {
        let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await?
            .expect("stream open")?;
        assert_eq!(msg.payload(), expected);
        msg.ack().await?;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(300), stream.next())
            .await
            .is_err(),
        "the aborted message must not be delivered"
    );
    println!("confirmed commit delivered, server-side abort discarded");

    broker.shutdown().await?;
    Ok(())
}
