---
sidebar_position: 8
description: "Тестирование сервиса на RabbitMQ внутри процесса с TestApp: рабочее приложение, исходы, ответы, задержки и тот же тест на сервере."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Тестирование сервиса на RabbitMQ

Тест запускает собственное приложение сервиса через [`TestApp`](api:ruststream::testing::TestApp) на [RabbitMQ внутри процесса](../components/testing/index.md) или на работающем сервере.

Тестируемый сервис списывает платежи по картам.
Платёж приходит в очередь `card-payments`, и обработчик отвечает квитанцией в `card-receipts`.
Платёж без суммы отбрасывается, и очередь переносит его в `card-payments.review`.
Платёж сверх мгновенного лимита банка откладывается: обработчик просит обработать его снова через минуту с [`retry_after`](../components/settlement/retry-after.md).
Эта минута проходит в очереди ожидания `card-payments.retry`.
Приложение строит `service(broker)`, поэтому тест выбирает брокер сам, а `main` берёт рабочий.
Тестам нужна фича `testing` крейта `ruststream-lapin` в `[dev-dependencies]`.

## Платёж и квитанция

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) подключает [`LapinBroker`](../components/broker/index.md) к RabbitMQ внутри процесса.
URI разбирается так же, как при настоящем соединении, но соединение не устанавливается.
Тест обращается к брокеру по его рабочему типу: `tb.broker::<LapinBroker>()`.
`Payment` объявляет свой адрес назначения, `card-payments`, поэтому тест публикует его без `to(..)`.
`publish` возвращает управление, когда обработчик и его ответ завершены.
Поэтому следующие строки проверяют уже готовое состояние: как подписчик подтвердил доставку и что дошло до `card-receipts`.
Тело теста принимает запущенное приложение аргументом, поэтому его же выполняет и тест на сервере ниже.

## Отброшенный платёж

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` проверяет исход, который вернул обработчик.
`assert_not_called` проверяет, что квитанция не опубликована.

## Задержка без ожидания

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

Тест начинается с остановленными часами, а [`advance`](api:ruststream::testing::TestApp::advance) сдвигает их вперёд.
Минута `HOLD` не стоит ничего.
У очереди есть [`delay`](../components/settlement/delay.md), поэтому RabbitMQ внутри процесса держит копию в очереди ожидания и по окончании задержки выпускает её в `card-payments`.
Копию публикует брокер, поэтому `published` показывает только собственное сообщение теста.
Каждая отложенная доставка записывается как `Outcome::Nack`.

## Что моделирует RabbitMQ внутри процесса

RabbitMQ внутри процесса читает настройки брокера и дескрипторы маршрутов - те же, что видит сервер.
[`declare_topology`](../components/broker/declare-topology.md), как и на сервере, решает, объявляет ли подписка свою очередь.
Он моделирует именно RabbitMQ, а не очередь вообще:

- Обменники по умолчанию, `direct`, `topic`, `fanout` и `headers` маршрутизируют по привязкам, которые объявляют дескрипторы.
- Конкурирующие потребители делят одну очередь, а сообщение, возвращённое в очередь, снова проходит через неё.
- Очередь хранит то, что в неё пришло, пока её никто не читает, а закрывшийся потребитель возвращает то, что взял и не подтвердил.
- Отброшенное сообщение уходит по маршруту недоставленных сообщений своей очереди, а кворумная очередь считает доставки и так же переносит сообщение, исчерпавшее их.
- Очередь ожидания выпускает отложенную копию, а direct reply-to даёт каждому запросу собственный адрес.
- Транзакция остаётся невидимой до фиксации.

Он отвергает то, что отвергает сервер: имя или заголовок, которые протокол не может передать, тело больше 16 МиБ, неустойчивую очередь без признака эксклюзивности, привязку к обменнику по умолчанию, повторное объявление очереди с другими настройками, кворумную очередь с приоритетом.
Доставка, подтверждённая после закрытия её соединения, возвращает ошибку закрытого канала.

То, что есть только у сервера, требует сервера: окно предвыборки, подтверждения издателя, атомарность транзакции канала, обменники из плагинов, очередь, которую делят два соединения, и очередь, которую сервис ожидает найти без объявления.

## Тот же тест на сервере

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) подключает то же приложение к работающему RabbitMQ.
Тест читает URI из `AMQP_TEST_URL` через `live::url`, собственную вспомогательную функцию примера, и пропускается, если переменная не задана.
Брокер объявляет свою топологию, поэтому тест работает на пустом сервере.
Проверки те же, что и внутри процесса, без изменений.
На сервере RabbitMQ сам выбирает, какой потребитель очереди получит сообщение, поэтому тест на сервере держит одну подписку на очередь.

```bash
cargo test
AMQP_TEST_URL=amqp://127.0.0.1:5672 cargo test -- --test-threads=1
```

Для запуска на сервере достаточно RabbitMQ из руководства [Начало работы](getting-started.md).
