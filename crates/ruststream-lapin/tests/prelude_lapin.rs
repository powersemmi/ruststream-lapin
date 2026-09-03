//! The crate prelude carries the family's uniform mount-site vocabulary.
//!
//! A routes file globs this module and writes `Publish`, `TransactionalPublish` and `Request`,
//! the names every broker in the family answers to, so a router reads the same whichever broker
//! it is written against. Handler bodies name capabilities instead and glob the framework's
//! prelude alone, which is why the two vocabularies never have to agree on a spelling.
//!
//! Pinned here because the aliases are load-bearing: they are what a router written against
//! another broker keeps compiling under, and `TransactionalPublish` in particular has to name the
//! policy whose live publisher actually carries the transaction capabilities.

use ruststream_lapin::prelude::*;

/// The capability vocabulary an include site checks a handler's bound against stays reachable
/// through the glob, next to the policy names.
fn _publisher_bound<T: Publisher>() {}
fn _transactional_bound<T: TransactionalPublisher>() {}
fn _owned_bound<T: OwnedTransactions>() {}
fn _request_bound<T: RequestReply>() {}

/// The uniform names resolve to this crate's policies, and each is constructible where a mount
/// site builds it - before anything has connected.
#[test]
fn the_uniform_policy_names_resolve_to_this_crates_policies() {
    let _: Publish = Publish::default();
    let _: TransactionalPublish = TransactionalPublish::default();
    let _: Request = Request::default();

    // Aliases, not separate types: a router may write either spelling, and the options builders
    // are the ones the prefixed originals carry.
    let plain: LapinPublish = Publish::default().exchange("orders");
    let transactional: ConfirmsPublish = TransactionalPublish::default().persistent(false);
    let requester: LapinRequest = Request::default();
    let queue = RabbitQueue::new("orders");

    assert_eq!(queue.name(), "orders");
    let _ = (plain, transactional, requester);
}
