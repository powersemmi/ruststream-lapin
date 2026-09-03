//! The crate prelude adds to the framework's vocabulary without taking anything out of it.
//!
//! Every name this crate contributes keeps its `Lapin` / `Rabbit` prefix. An unprefixed
//! re-export would shadow the framework's own name silently, because an explicit re-export wins
//! over the `pub use ruststream::prelude::*` glob underneath it - and the failure would land on
//! the user, in their file, as a type where a trait was expected.

use ruststream_lapin::prelude::*;

/// `Publish` is the framework's slot capability trait, not a publish policy: the bound has to
/// resolve after the glob. Naming it as a bound is the whole test - it fails to compile if the
/// prelude ever aliases a policy onto the name again.
fn _publish_is_the_core_trait<T: Publish>() {}

/// The policies keep their prefixes, so both vocabularies coexist in one file.
#[test]
fn the_policies_are_reachable_under_their_prefixed_names() {
    let plain = LapinPublish::default().exchange("orders");
    let confirms = LapinPublish::default().confirms();
    let requester = LapinRequest::default();
    let queue = RabbitQueue::new("orders");

    assert_eq!(queue.name(), "orders");
    let _ = (plain, confirms, requester);
}
