---
title: Lifecycle
sidebar_position: 3
description: "LapinBroker::new records configuration, connect opens the connection, startup opens a channel and a consumer per subscription, and shutdown closes."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The broker's three states as types, and what each transition does on RabbitMQ",
  "text": "The diagram as text",
  "states": "Broker",
  "rabbit": "On RabbitMQ",
  "unconnected": ["LapinBroker::new", "(uri)"],
  "connected": ["ConnectedLapinBroker"],
  "closed": ["ClosedLapinBroker", "handshake()"],
  "config": ["Configuration only,", "no I/O"],
  "connection": ["The connection and", "the publish channel"],
  "subscriptions": ["Each subscription:", "its channel, its", "topology if declared,", "basic.qos and", "basic.consume"],
  "ownChannels": ["The first confirms,", "transaction or request", "opens its channel"],
  "close": ["Marked closed, then", "connection.close"],
  "delayCheck": ["Without declaration, a missing waiting queue", "or delayed exchange fails startup"],
  "afterShutdown": ["A publisher that outlives the connection", "answers AmqpError::Closed, never a silent success"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# Lifecycle

[`LapinBroker::new`](api:ruststream_lapin::LapinBroker::new) records configuration, connect
opens the connection, startup opens a channel and a consumer per subscription, and shutdown
closes.

<Diagram name="lifecycle" words={words} />

`new` does no I/O, so the service's builder stays synchronous. The runtime connects the broker when
the app starts. Connect opens the AMQP connection to the URI, with the
[`connection_name`](api:ruststream_lapin::LapinBroker::connection_name) the management UI shows,
and opens the shared publish channel. A server that refuses the connection fails the start.

Each subscription's descriptor resolves once, against the connected broker, and the start fails
on the first one that cannot open:

1. The queue checks its retry declaration. `max_attempts` and `dead_letter` become the queue's
   own arguments. A quorum queue needs both and a broker that declares the queue; a classic queue
   takes `dead_letter` alone; a bare queue name takes neither.
2. The subscription opens its channel.
3. Under [`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology) it
   declares its exchanges, its queue, its bindings and its delay target. A declaration that
   differs from what the server holds is refused with `PRECONDITION_FAILED`, and the error names
   the queue and the arguments sent.
4. It sends `basic.qos` with its prefetch window, when one is set.
5. A [`delay`](api:ruststream_lapin::RabbitQueue::delay) the broker does not declare is checked
   to exist: its waiting queue or its delayed exchange. A missing one fails the start and names
   both the subscription and the target.
6. It starts its consumer with `basic.consume`. A queue that does not exist fails here.

The delay check exists because the delayed copy is published without waiting for an answer, and
the original is acknowledged after it. The server drops a copy sent to a waiting queue that does
not exist, so without the check the message would be lost with no error. It costs one round trip
at startup and nothing per message.

A service that only consumes opens no further channel. The first publish through a
[confirms or transaction policy](rabbitmq/publishing.md), and the first request, open the channel
each of them needs.

Shutdown marks the connection closed first, so a publisher that still holds it cannot slip a
message in, and then closes the connection with the AMQP close handshake. The server requeues
whatever the subscriptions held unsettled.
[`ClosedLapinBroker::handshake`](api:ruststream_lapin::ClosedLapinBroker::handshake) reports whether
the handshake ran. A publisher or a requester that outlives the connection answers
[`AmqpError::Closed`](api:ruststream_lapin::AmqpError::Closed) after shutdown, and a delivery
settled after shutdown reports an error instead of a settlement that never happened.

## Connection recovery

Recovery is disabled by default. With `LapinBroker::auto_recover(true)`, the connected broker can be connected or reconnecting without changing its Rust type.
`is_connected()` reports the AMQP connection state; channel and topology restoration may still be running when it returns true.
Lapin restores the existing channels and their topology instead of rebuilding the application. Startup hooks are not rerun.
`recovery_retries(n)` limits retries for startup and later recovery (16 by default). Exhaustion ends a consumer with a terminal consume error.
Shutdown during recovery marks handles closed and wakes waiting consumers immediately; a later reconnected socket is closed in the background.
