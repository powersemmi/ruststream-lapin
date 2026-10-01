//! What one publish into a client-side transaction buffer costs.
//!
//! A transaction keeps the message until the commit, so the buffer it builds is the crate's own
//! conversion of an outgoing message. Address equality cannot tell a moved header map from a
//! cloned one - `Bytes` keeps the data pointer across a clone - so the proof is the number of
//! allocations the buffering call makes.
//!
//! The broker is connected in process because a live one needs a server; the buffering is the
//! publisher's own either way, so the count is the one a service pays.
#![cfg(feature = "testing")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ruststream::testing::InProcess;
use ruststream::{
    BytesMut, ConnectedBroker, HeaderMap, OutgoingFor, OutgoingMessage, OwnedTransactions,
    Publisher, Take, Transaction, TransactionalPublisher,
};
use ruststream_lapin::{LapinBroker, LapinPublish};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URI: &str = "amqp://localhost:5672";

/// Counts this thread's allocations, so the cost of one buffering call can be read off directly.
/// Thread-local rather than global: the other tests of this binary run beside it and their
/// allocations are none of this measurement's business.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        // SAFETY: the layout is the caller's, forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What this thread has allocated so far.
fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
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

#[tokio::test]
async fn buffering_an_owned_transaction_publish_allocates_only_its_routing_key() {
    let broker = LapinBroker::new(URI)
        .connect_in_process()
        .await
        .expect("connect");
    let publisher = broker.publisher(LapinPublish::default().confirms());
    let mut txn = publisher.transaction().await.expect("open the transaction");

    // The buffer's own growth is not the subject: one publish outside the counted region leaves
    // it with room for the next. The message the region measures is built outside it too.
    txn.publish(taken_message(), None).await.expect("buffered");
    let msg = taken_message();

    let before = allocations();
    txn.publish(msg, None).await.expect("buffered");
    let spent = allocations() - before;

    txn.abort().await.expect("abort");
    broker.shutdown().await.expect("shutdown");

    assert_eq!(
        spent, 1,
        "the routing key is the buffer's own String; the payload is taken as the framework wrote \
         it and the header map is moved, so neither allocates"
    );
}

#[tokio::test]
async fn buffering_a_handle_transaction_publish_allocates_only_its_routing_key_and_payload() {
    let broker = LapinBroker::new(URI)
        .connect_in_process()
        .await
        .expect("connect");
    let publisher = broker.publisher(LapinPublish::default().confirms());
    publisher
        .begin_transaction()
        .await
        .expect("open the transaction");

    publisher
        .publish(lent_message(), None)
        .await
        .expect("buffered");
    let msg = lent_message();

    let before = allocations();
    publisher.publish(msg, None).await.expect("buffered");
    let spent = allocations() - before;

    publisher.abort().await.expect("abort");
    broker.shutdown().await.expect("shutdown");

    assert_eq!(
        spent, 2,
        "the routing key and the copy of a payload the framework reuses, and nothing for the \
         header map"
    );
}
