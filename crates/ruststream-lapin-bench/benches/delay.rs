// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! The consume scenario on a queue that declares a native delay: the subscription carries
//! `Delay::dlx_ttl`, and the handler acks every delivery, so no delayed retry is ever published.
//! A delay a handler does not use costs a delivery nothing, so the floor is the consume
//! scenario's.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::prelude::*;
use ruststream_lapin::{Delay, RabbitQueue};

#[subscriber(RabbitQueue::new("ruststream-bench.orders").delay(Delay::dlx_ttl()))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| b.include(consume))
}

// The longest run allocated 18176 to 18227 blocks over five runs; the floor adds a tenth of a
// percent.
#[library_benchmark(config = common::config(9, 246))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = delay_group; benchmarks = service);
main!(library_benchmark_groups = delay_group);
