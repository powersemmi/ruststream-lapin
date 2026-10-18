---
sidebar_position: 4
description: "Кто создаёт очереди и обменники RabbitMQ: сервис через declare_topology или эксплуатация; классические и кворумные очереди, аргументы объявления."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Топология

Это руководство описывает очереди, обменники и привязки сервиса и решает, кто их создаёт: сам сервис или те, кто эксплуатирует RabbitMQ.

Дескриптор очереди описывает то, что сервис ожидает найти на сервере.
Ничего не создаётся, пока брокер не включит объявление через [`declare_topology(true)`](../components/broker/declare-topology.md).
Пример - платёжный сервис с реплицируемой очередью платежей, очередью для неудавшихся платежей и собственной очередью курсов валют у каждого экземпляра.

## Классические и кворумные очереди

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/topology.rs#queues"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/topology.rs#queues"
```

</TabItem>
</Tabs>

В RabbitMQ два типа очередей, и тип задаётся при создании очереди.
[Классическая очередь](../components/subscriptions/rabbit-queue.md) хранится на одном узле.
[Кворумная очередь](../components/subscriptions/rabbit-quorum-queue.md) реплицируется по кластеру алгоритмом Raft и переживает потерю узла.
У каждого типа свой дескриптор: [`RabbitQueue`](api:ruststream_lapin::RabbitQueue) и [`RabbitQuorumQueue`](api:ruststream_lapin::RabbitQuorumQueue).
Берите кворумную очередь для данных, которые нельзя потерять, например платежей, а классическую - для остального.
Руководство RabbitMQ по [кворумным очередям](https://www.rabbitmq.com/docs/quorum-queues) сравнивает оба типа.

Кворумная очередь всегда сохраняется на диск, доступна всем соединениям и не удаляется автоматически.
Поэтому у `RabbitQuorumQueue` нет `durable`, `exclusive` и `auto_delete`, и кворумная очередь, противоречащая своему типу, не компилируется.
Кроме того, кворумная очередь считает доставки каждого сообщения, на чём строится [руководство по переобработке](retries.md).

[`dead_letter_exchange`](../components/settlement/dead-letter-exchange.md) задаёт очереди `payments` маршрут для отклонённых сообщений.
Платёж, который `charge` отбрасывает, уходит в обменник недоставленных сообщений `payments.dead`, а оттуда в привязанную к нему очередь `payments.failed`.
[`dead_letter_routing_key`](api:ruststream_lapin::RabbitQueue::dead_letter_routing_key) заменяет ключ маршрутизации сообщения на этом маршруте, если обменник маршрутизирует по ключу.

## Очередь у каждого экземпляра

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/topology.rs#instance"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/topology.rs#instance"
```

</TabItem>
</Tabs>

Каждому экземпляру нужны все курсы валют, поэтому каждый экземпляр читает собственную очередь, привязанную к обменнику `fx-rates` типа `fanout`.
Имя очереди содержит идентификатор процесса, поэтому два экземпляра никогда не делят её.
[`exclusive(true)`](api:ruststream_lapin::RabbitQueue::exclusive) разрешает пользоваться очередью только этому соединению, и RabbitMQ удаляет её, когда соединение закрывается.
[`auto_delete(true)`](api:ruststream_lapin::RabbitQueue::auto_delete) удаляет её, когда уходит последний потребитель.
[`durable(false)`](api:ruststream_lapin::RabbitQueue::durable) не сохраняет её на диск: после перезапуска курсы ничего не стоят.
RabbitMQ 4 принимает неустойчивую очередь, только если она эксклюзивная, поэтому `durable(false)` идёт вместе с `exclusive(true)`.

[`argument`](../components/subscriptions/arguments.md) передаёт аргумент объявления, для которого у дескриптора нет метода, здесь `x-message-ttl`: курс, который никто не прочитал за минуту, устаревает.
Так же задаются `x-max-length`, `x-max-priority`, `x-overflow` и `x-single-active-consumer`.
Значение имеет тип [`AMQPValue`](api:ruststream_lapin::AMQPValue), а [`arguments`](api:ruststream_lapin::RabbitQueue::arguments) принимает целую таблицу.

## Объявляет сервис

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/topology.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/topology.rs#app"
```

</TabItem>
</Tabs>

При `declare_topology(true)` каждая подписка при запуске объявляет то, что описывает её дескриптор.
Сначала она объявляет обменники, которые называют её привязки, затем очередь с её аргументами, затем привязки.
Обменник по умолчанию и встроенные обменники `amq.*` есть на любом сервере, и их никто не объявляет.
Повторное объявление безвредно, пока оно совпадает с тем, что есть на сервере, поэтому все экземпляры сервиса объявляют одни и те же очереди.

Объявление, которое расходится с существующей очередью, возвращает ошибку.
На повторное объявление очереди с другой устойчивостью или другими аргументами RabbitMQ отвечает `PRECONDITION_FAILED`, и сервис не запускается: [`AmqpError::Declare`](../components/broker/errors.md) называет очередь.
Тип и аргументы очереди меняются только удалением и повторным объявлением, при котором её содержимое теряется, поэтому их меняют миграцией, а не развёртыванием.
Какие свойства должны совпадать, RabbitMQ объясняет в разделе [property equivalence](https://www.rabbitmq.com/docs/queues#property-equivalence).

## Объявляет эксплуатация

`DECLARE_TOPOLOGY=false` запускает тот же сервис с выключенным объявлением.
Тогда дескрипторы ничего не объявляют: они описывают очереди, аргументы и привязки, которые ожидает сервис, а создаёт их эксплуатация.
Если очереди нет, подписка при запуске возвращает ошибку [`AmqpError::Subscribe`](../components/broker/errors.md), поэтому отсутствующая очередь останавливает развёртывание, а не теряет сообщения.

Лимиты и TTL эксплуатация часто задаёт [политикой](https://www.rabbitmq.com/docs/policies) RabbitMQ, а не аргументами объявления.
Политика меняет очередь без повторного объявления, а аргумент, заданный политикой, никогда не конфликтует с объявлением сервиса.
Тогда дескриптор называет только то, на что опирается сам сервис.

От выключенного объявления зависит и настройка переобработки на кворумной очереди; это описывает [руководство по переобработке](retries.md).

## Тестирование

Тесты в файле примера отклоняют платёж и находят его в `payments.failed` при включённом и при выключенном объявлении.
RabbitMQ внутри процесса следует настройке брокера: объявляет то, что описывают дескрипторы, и отвергает то, что отвергает сервер, например неустойчивую очередь без признака эксклюзивности, привязку к обменнику по умолчанию или повторное объявление очереди с другими настройками.
При выключенном объявлении каждая очередь, которую называет подписка, внутри процесса уже существует, поэтому отсутствующую очередь проверяет [тест на сервере](../components/testing/live.md).
Обвязку разбирает руководство [Тестирование сервиса на RabbitMQ](testing.md).
