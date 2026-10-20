//! Where a publish through an exchange lands on a live server, as far as the test harness needs to
//! know: which of the app's subscriptions owe it a delivery.
//!
//! The harness records a publish by its routing key alone; the exchange is the publisher's
//! setting. A live connection therefore notes what the harness cannot see: the exchange each
//! routing key was published to, the bindings each subscribed queue declared, and, for an exchange
//! whose server picks one queue by a hash, the queue each key was delivered to.

use std::collections::HashSet;
use std::sync::{Mutex, MutexGuard, PoisonError};

use lapin::ExchangeKind;

use crate::in_process::topic_matches;
use crate::queue::Binding;

/// The exchange type of the consistent-hash plugin, the one [`RabbitExchange::consistent_hash`]
/// declares.
///
/// [`RabbitExchange::consistent_hash`]: crate::RabbitExchange
const CONSISTENT_HASH: &str = "x-consistent-hash";

/// One binding a subscribed queue declared.
struct Bound {
    queue: String,
    exchange: String,
    kind: ExchangeKind,
    routing_key: String,
}

#[derive(Default)]
struct Seen {
    bindings: Vec<Bound>,
    /// `(exchange, routing key)` of every publish through a named exchange.
    published: HashSet<(String, String)>,
    /// `(exchange, routing key, queue)` of every delivery that came through a named exchange.
    delivered: HashSet<(String, String, String)>,
}

/// What one live connection saw of its publishes, bindings and deliveries.
#[derive(Default)]
pub(crate) struct LiveRoutes {
    seen: Mutex<Seen>,
}

impl LiveRoutes {
    fn seen(&self) -> MutexGuard<'_, Seen> {
        // A panic while the lock was held leaves sets that are still whole: each write is one
        // insert or one push.
        self.seen.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Notes the bindings `queue` declared.
    pub(crate) fn bound(&self, queue: &str, bindings: &[Binding]) {
        self.seen()
            .bindings
            .extend(bindings.iter().map(|binding| Bound {
                queue: queue.to_owned(),
                exchange: binding.exchange.name().to_owned(),
                kind: binding.exchange.kind().clone(),
                routing_key: binding.routing_key.clone(),
            }));
    }

    /// Notes a publish to `exchange` under `routing_key`.
    pub(crate) fn published(&self, exchange: &str, routing_key: &str) {
        // The default exchange routes by queue name, which the harness can answer without a note.
        if !exchange.is_empty() {
            self.seen()
                .published
                .insert((exchange.to_owned(), routing_key.to_owned()));
        }
    }

    /// Notes a delivery on `queue` that was published to `exchange` under `routing_key`.
    pub(crate) fn delivered(&self, exchange: &str, routing_key: &str, queue: &str) {
        if !exchange.is_empty() {
            self.seen().delivered.insert((
                exchange.to_owned(),
                routing_key.to_owned(),
                queue.to_owned(),
            ));
        }
    }

    /// The positions in `subscriptions` that a publish under `routing_key` reaches through the
    /// named exchanges it was published to.
    ///
    /// Direct, topic and fanout exchanges route by the bindings the service declared. A
    /// consistent-hash exchange hands each key to one bound queue that the server's hash picks:
    /// once a key has been delivered, the queue that took it owes every publish of that key, and
    /// until then every queue bound to the exchange does, so the harness keeps waiting for it.
    /// Any other exchange routes nothing here.
    pub(crate) fn through_exchanges(
        &self,
        routing_key: &str,
        subscriptions: &[&str],
    ) -> Vec<usize> {
        let seen = self.seen();
        let mut owed = Vec::new();
        for (exchange, _) in seen
            .published
            .iter()
            .filter(|(_, published)| published == routing_key)
        {
            let bound = || {
                seen.bindings
                    .iter()
                    .filter(move |bound| &bound.exchange == exchange)
            };
            let queues: Vec<&str> = if bound().any(|bound| is_consistent_hash(&bound.kind)) {
                let picked: Vec<&str> = seen
                    .delivered
                    .iter()
                    .filter(|(at, key, _)| at == exchange && key == routing_key)
                    .map(|(_, _, queue)| queue.as_str())
                    .collect();
                if picked.is_empty() {
                    bound().map(|bound| bound.queue.as_str()).collect()
                } else {
                    picked
                }
            } else {
                bound()
                    .filter(|bound| match &bound.kind {
                        ExchangeKind::Direct => bound.routing_key == routing_key,
                        ExchangeKind::Topic => topic_matches(&bound.routing_key, routing_key),
                        ExchangeKind::Fanout => true,
                        ExchangeKind::Headers | ExchangeKind::Custom(_) => false,
                    })
                    .map(|bound| bound.queue.as_str())
                    .collect()
            };
            for (position, name) in subscriptions.iter().enumerate() {
                if queues.contains(name) && !owed.contains(&position) {
                    owed.push(position);
                }
            }
        }
        drop(seen);
        owed
    }
}

fn is_consistent_hash(kind: &ExchangeKind) -> bool {
    matches!(kind, ExchangeKind::Custom(kind) if kind == CONSISTENT_HASH)
}

#[cfg(test)]
mod tests {
    use lapin::types::FieldTable;

    use super::*;
    use crate::RabbitExchange;

    fn binding(exchange: RabbitExchange, routing_key: &str) -> Vec<Binding> {
        vec![Binding {
            exchange,
            routing_key: routing_key.to_owned(),
            arguments: FieldTable::default(),
        }]
    }

    const APP: &[&str] = &["intake", "shard-a", "shard-b"];

    fn sharded() -> LiveRoutes {
        let routes = LiveRoutes::default();
        routes.bound(
            "shard-a",
            &binding(RabbitExchange::custom("x", CONSISTENT_HASH), "1"),
        );
        routes.bound(
            "shard-b",
            &binding(RabbitExchange::custom("x", CONSISTENT_HASH), "1"),
        );
        routes
    }

    #[test]
    fn a_key_not_yet_delivered_through_a_consistent_hash_is_owed_by_every_shard() {
        let routes = sharded();
        routes.published("x", "customer-1");

        assert_eq!(routes.through_exchanges("customer-1", APP), vec![1, 2]);
    }

    #[test]
    fn a_delivered_key_is_owed_by_the_shard_the_server_picked() {
        let routes = sharded();
        routes.published("x", "customer-1");
        routes.delivered("x", "customer-1", "shard-b");

        assert_eq!(routes.through_exchanges("customer-1", APP), vec![2]);
    }

    #[test]
    fn a_key_published_to_no_exchange_is_owed_by_none() {
        let routes = sharded();
        routes.published("", "customer-1");
        routes.delivered("", "customer-1", "shard-b");

        assert_eq!(
            routes.through_exchanges("customer-1", APP),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn direct_topic_and_fanout_route_by_the_declared_bindings() {
        let routes = LiveRoutes::default();
        routes.bound("intake", &binding(RabbitExchange::direct("d"), "orders"));
        routes.bound("shard-a", &binding(RabbitExchange::topic("t"), "orders.*"));
        routes.bound("shard-b", &binding(RabbitExchange::fanout("f"), ""));
        routes.published("d", "orders");
        routes.published("t", "orders.eu");
        routes.published("f", "anything");

        assert_eq!(routes.through_exchanges("orders", APP), vec![0]);
        assert_eq!(routes.through_exchanges("orders.eu", APP), vec![1]);
        assert_eq!(routes.through_exchanges("anything", APP), vec![2]);
        assert_eq!(
            routes.through_exchanges("refunds", APP),
            Vec::<usize>::new()
        );
    }
}
