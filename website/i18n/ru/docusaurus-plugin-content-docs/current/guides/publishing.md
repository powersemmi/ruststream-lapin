---
sidebar_position: 6
description: "Публикация в RabbitMQ с нужной гарантией: без подтверждения, с подтверждениями издателя, с транзакциями канала, приоритет и TTL сообщения."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Надёжная публикация

Это руководство выбирает, насколько сервис уверен, что RabbitMQ принял его сообщения, и задаёт свойства отдельного сообщения.

Гарантию задаёт [политика публикации](../components/publishing/index.md) в точке монтирования, а не флаг каждого вызова:

| Политика | Имя в прелюдии | Публикация завершается, когда |
|---|---|---|
| [`LapinPublish`](api:ruststream_lapin::LapinPublish) | `Publish` | кадр записан в соединение |
| [`ConfirmsPublish`](../components/transactions/confirms-publish.md) | `TransactionalPublish` | RabbitMQ подтвердил сообщение |
| [`ServerTxPublish`](../components/transactions/server-tx-publish.md) | `ServerTxPublish` | транзакция канала зафиксирована |

Пример - сервис заказов.
Он превращает заказ в отправления, по одному на посылку, все или ни одного, и заказывает курьера для каждого отправления, срочного - раньше остальных.

## Транзакции с подтверждениями

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#confirms"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#confirms"
```

</TabItem>
</Tabs>

Слот `pack_order` ограничен типом `Out<impl TransactionalPublisher>`, поэтому обработчик монтируется только на политику с транзакциями.
`begin` открывает область транзакции, и каждое отправление, опубликованное в ней, ждёт в сервисе.
`commit` публикует их и завершается, когда RabbitMQ подтвердил каждое.
Посылка без описания прерывает область, и ни одно отправление этого заказа не уходит.
Если фиксация не удалась, она возвращает ошибку, и заказ можно доставить и упаковать заново.

RabbitMQ подтверждает сообщение, когда берёт за него ответственность: записывает на диск постоянное сообщение в устойчивой очереди или получает согласие большинства кворумной очереди.
Когда подтверждается каждое сообщение, описывает [руководство по подтверждениям](https://www.rabbitmq.com/docs/confirms) RabbitMQ.

Подтверждение не значит, что сообщение попало в очередь.
Сообщение, которое не подходит ни под одну привязку, RabbitMQ подтверждает и отбрасывает, потому что крейт публикует без флага `mandatory`.
Поэтому подтверждённая публикация должна идти в обменник с существующими привязками или в существующую очередь. Объявите их вместе с сервисом ([Топология](topology.md)) или проверьте там, где ими владеет эксплуатация.

## Транзакции канала

[`ServerTxPublish`](../components/transactions/server-tx-publish.md), который даёт `Publish::default().server_tx()`, вместо этого использует транзакции канала AMQP.
Сообщения транзакции становятся видны вместе при `commit`, а это синхронный обмен с сервером.
Подтверждения быстрее и подходят для большинства случаев.
Транзакция канала нужна, когда потребители не должны увидеть половину группы, например только одну из двух проводок перевода.

Обработчик, ограниченный `Out<impl TransactionalPublisher>`, монтируется на любую из двух политик без изменений.
[`ConfirmsTransaction`](../components/transactions/confirms-transaction.md), открытая через `owned_transaction`, владеет своим буфером, а не заимствует издателя: можно открыть несколько таких транзакций сразу, и каждая фиксируется отдельно.
Транзакции канала дают только заимствующую область, потому что транзакция - это состояние самого канала.

## Свойства сообщения

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#properties"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#properties"
```

</TabItem>
</Tabs>

AMQP даёт каждому сообщению три собственных свойства: [приоритет](../components/publishing/properties/priority.md), [срок жизни](../components/publishing/properties/expiration.md) и [режим доставки](../components/publishing/properties/persistent.md).
Политика задаёт значения для каждого сообщения, которое через неё проходит, здесь приоритет 3.
[Шаги](../components/publishing/properties/index.md) билдера публикации задают их для одного сообщения: срочный вызов курьера уходит с приоритетом 9, и если его не прочитали за час, срок его жизни истекает.
Слот ограничен типом [`LapinPublishOptions`](api:ruststream_lapin::LapinPublishOptions), поэтому обработчик не называет тип издателя. Шаги берутся из прелюдии крейта.

Ни одно из трёх свойств не передаётся заголовком: каждое - свойство AMQP, откуда его и читает RabbitMQ.
Доставка возвращает их в виде заголовков [`PRIORITY_HEADER`](api:ruststream_lapin::PRIORITY_HEADER) и [`EXPIRATION_HEADER`](api:ruststream_lapin::EXPIRATION_HEADER).

Приоритет упорядочивает доставки только в очереди, объявленной с `x-max-priority`. В любой другой очереди RabbitMQ его не учитывает.
Очередь курьеров `pickups` объявляет с этим [аргументом](../components/subscriptions/arguments.md) тот, кто ею владеет.
Каждая политика публикует постоянные сообщения, если `persistent(false)` не задаёт иное, поэтому сообщение в устойчивой очереди переживает перезапуск брокера.

## Точка монтирования

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#app"
```

</TabItem>
</Tabs>

`TransactionalPublish::default()` - это [`ConfirmsPublish`](api:ruststream_lapin::ConfirmsPublish).
`Publish::default().priority(3)` - обычная политика с приоритетом для каждого сообщения.
`.confirms()` и `.server_tx()` превращают обычную политику в более строгую и сохраняют её настройки, например обменник.
Публикации без подтверждения делят один канал соединения. Издатель с подтверждениями или с транзакциями открывает собственный канал, потому что оба режима - состояние канала.

Сервис публикует и вне обработчика, например из HTTP-обработчика, через издателя, которого приложение выдаёт при запуске ([`LapinPublish`](../components/publishing/index.md)).

## Тестирование

Тесты в файле примера проверяют, что заказ отправляет все посылки, что заказ с плохой посылкой не отправляет ни одной и какие свойства у каждого вызова курьера. Свойства читаются через `tb.out::<Pickups>()`.
RabbitMQ внутри процесса держит транзакцию в буфере до фиксации и оформляет каждое сообщение так, как это сделал бы сервер.
Смысл подтверждения и атомарность транзакции канала определяет сервер, поэтому их проверяет [тест на сервере](../components/testing/live.md).
Обвязку разбирает руководство [Тестирование сервиса на RabbitMQ](testing.md).
