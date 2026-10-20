//! What one transaction-buffer publish allocates: the routing key, and a payload copy only for a lending publisher.
#![cfg(feature = "testing")]

use allocation_counter::measure;
use ruststream::testing::InProcess;
use ruststream::{
    BytesMut, ConnectedBroker, HeaderMap, OutgoingFor, OutgoingMessage, OwnedTransactions,
    Publisher, Take, Transaction, TransactionalPublisher,
};
use ruststream_lapin::{LapinBroker, LapinPublish};
use tokio::runtime::{Builder, Runtime};

/// The address the in-process broker parses without dialing.
const URI: &str = "amqp://localhost:5672";

/// A runtime on this thread, so measured polls and their tasks share the allocation counter.
fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime builds")
}

/// Two headers, built outside every counted region.
fn two_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    headers
}

/// A message in the form a transaction is handed.
fn taken_message() -> OutgoingFor<'static, Take> {
    OutgoingMessage::produced("orders", BytesMut::from(&br#"{"id":1}"#[..]))
        .with_headers(two_headers())
}

/// A message in the form a lending publisher is handed.
fn lent_message() -> OutgoingMessage<'static> {
    OutgoingMessage::new("orders", br#"{"id":1}"#.as_slice()).with_headers(two_headers())
}

#[test]
fn buffering_an_owned_transaction_publish_allocates_only_its_routing_key() {
    let runtime = runtime();
    let (broker, mut txn) = runtime.block_on(async {
        let broker = LapinBroker::new(URI)
            .connect_in_process()
            .await
            .expect("connect");
        let publisher = broker.publisher(LapinPublish::default().confirms());
        let txn = publisher.transaction().await.expect("open the transaction");
        (broker, txn)
    });

    // The buffer growth is warmed up, and the measured message is built before counting.
    runtime
        .block_on(txn.publish(taken_message(), None))
        .expect("buffered");
    let msg = taken_message();
    let mut publish_result = None;
    let measured = measure(|| {
        publish_result = Some(runtime.block_on(txn.publish(msg, None)));
    });
    publish_result
        .expect("the measured publish ran")
        .expect("buffered");

    runtime.block_on(async {
        txn.abort().await.expect("abort");
        broker.shutdown().await.expect("shutdown");
    });

    assert_eq!(
        measured.count_total, 1,
        "the routing key owns the only allocation; the payload and header map move"
    );
}

#[test]
fn buffering_a_handle_transaction_publish_allocates_its_key_and_payload() {
    let runtime = runtime();
    let (broker, publisher) = runtime.block_on(async {
        let broker = LapinBroker::new(URI)
            .connect_in_process()
            .await
            .expect("connect");
        let publisher = broker.publisher(LapinPublish::default().confirms());
        publisher
            .begin_transaction()
            .await
            .expect("open the transaction");
        (broker, publisher)
    });

    runtime
        .block_on(publisher.publish(lent_message(), None))
        .expect("buffered");
    let msg = lent_message();
    let mut publish_result = None;
    let measured = measure(|| {
        publish_result = Some(runtime.block_on(publisher.publish(msg, None)));
    });
    publish_result
        .expect("the measured publish ran")
        .expect("buffered");

    runtime.block_on(async {
        publisher.abort().await.expect("abort");
        broker.shutdown().await.expect("shutdown");
    });

    assert_eq!(
        measured.count_total, 2,
        "the routing key and the copied payload allocate; the header map moves"
    );
}
