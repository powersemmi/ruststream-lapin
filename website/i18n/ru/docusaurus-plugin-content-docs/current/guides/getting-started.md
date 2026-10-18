---
sidebar_position: 1
description: "Установка ruststream-lapin, локальный RabbitMQ, первый подписчик на нём и его тест без сервера."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Начало работы

Это руководство устанавливает `ruststream-lapin`, запускает локальный RabbitMQ и первый сервис на нём.

## Установка

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "asyncapi", "logging"] }
ruststream-lapin = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-lapin = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` - это сам фреймворк. Фича `macros` даёт атрибуты, `json` - кодек по умолчанию, `asyncapi` - документ AsyncAPI сервиса, а `logging` выводит журнал сервиса в терминал.
`ruststream-lapin` - брокер RabbitMQ на [lapin](https://docs.rs/lapin), клиенте AMQP 0.9.1, написанном на Rust.
Для сборки не нужно ничего, кроме Cargo.
Фича `testing` брокера добавляет [RabbitMQ внутри процесса](../components/testing/index.md), на котором работают тесты.

У крейта брокера такие фичи:

| Фича | Что она даёт |
|---|---|
| `asyncapi` | [привязки AMQP](../components/asyncapi/index.md) в генерируемом документе AsyncAPI |
| `testing` | RabbitMQ внутри процесса, на котором работает `TestApp` |
| `tls-native-tls` | [TLS](../components/broker/tls.md) через TLS-библиотеку платформы |
| `tls-rustls` | TLS через rustls |
| `tls-rustls-ring` | TLS через rustls с криптопровайдером `ring` |
| `plugin-consistent-hash` | [обменник с консистентным хешированием](../components/exchanges/consistent-hash.md) из плагина RabbitMQ |
| `plugin-dme` | [отложенная переобработка](../components/settlement/plugin-dme.md) через плагин delayed-message-exchange |

## Локальный RabbitMQ

Для разработки достаточно одного узла RabbitMQ с веб-интерфейсом управления:

```yaml title="compose.yaml"
services:
  rabbitmq:
    image: rabbitmq:4-management-alpine
    ports:
      - "127.0.0.1:5672:5672"
      - "127.0.0.1:15672:15672"
```

```bash
docker compose up -d
```

Брокер слушает `localhost:5672`.
Веб-интерфейс управления открывается по адресу `http://localhost:15672`, пользователь `guest`, пароль `guest`.
RabbitMQ пускает учётную запись `guest` только с локального адреса, поэтому порты опубликованы на `127.0.0.1`.

## Первый сервис

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` читает заказы в JSON из очереди `orders`.
Заказ с суммой он подтверждает, а заказ без суммы отбрасывает.
[`LapinBroker::new`](../components/broker/index.md) принимает URI соединения и только запоминает его.
Соединение открывается при запуске сервиса.
[`declare_topology(true)`](../components/broker/declare-topology.md) разрешает сервису объявить очереди, которые он читает, поэтому сервис запускается и на пустом RabbitMQ.
`#[ruststream::app]` превращает `app` в `main`. В ручной форме то же делает [`run_main`](api:ruststream::runtime::cli::run_main).

Запустите сервис:

```bash
cargo run -- run
```

Опубликуйте заказ из другого терминала утилитой `rabbitmqadmin`, которая входит в образ:

```bash
docker compose exec rabbitmq rabbitmqadmin publish message \
  --routing-key orders --payload '{"id":1,"amount_cents":1250}'
```

Сообщение уходит в обменник по умолчанию, а он доставляет его в очередь, имя которой совпадает с ключом маршрутизации.
Сервис пишет в журнал `order taken` с номером заказа.
То же сообщение можно опубликовать со страницы очереди `orders` в веб-интерфейсе управления.
<kbd>Ctrl</kbd>+<kbd>C</kbd> останавливает сервис.

Сервис умеет и описать себя:

```bash
cargo run -- asyncapi gen
```

Команда выводит документ AsyncAPI сервиса: сервер, канал `orders` и сообщение `Order`.

`cargo generate` создаёт такой же сервис с роутером и ответом:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-queue --name my-service
```

Шаблон `templates/amqp-topic` вместо этого начинает с обменника типа `topic`.

## Первый тест

В файле примера есть и два теста.
Каждый запускает то же `app` через `TestApp::start`, публикует заказ и проверяет, как `take_order` подтвердил его доставку.
Там `LapinBroker` подключается к RabbitMQ внутри процесса, поэтому тестам не нужен сервер:

```bash
cargo test
```

Руководство [Тестирование сервиса на RabbitMQ](testing.md) разбирает эти тесты и то, что моделирует RabbitMQ внутри процесса.

## Переход с lapin

Сервис на lapin сам открывает каналы и пишет цикл потребителя.
Вот куда переходит каждая часть в RustStream:

| lapin | RustStream |
|---|---|
| `Connection::connect` и `create_channel` | [`LapinBroker::new`](../components/broker/connection.md); рантайм подключается при запуске |
| `channel.basic_qos` | [`prefetch`](../components/subscriptions/prefetch.md) у брокера или у отдельной очереди |
| `queue_declare`, `exchange_declare`, `queue_bind` | [дескриптор очереди](../components/subscriptions/rabbit-queue.md) и [`declare_topology(true)`](../components/broker/declare-topology.md) ([Топология](topology.md)) |
| `basic_consume` и цикл `while let Some(delivery)` | обработчик `#[subscriber]` |
| десериализация `delivery.data` | тип параметра обработчика, который декодирует кодек |
| `delivery.ack` и `delivery.reject` | [исход](../components/settlement/index.md) обработчика ([Подтверждения, переобработка и недоставленные сообщения](retries.md)) |
| `basic_publish` | ответ или параметр `Out`, отправленный через [политику публикации](../components/publishing/index.md) |
| `confirm_select` и ожидание подтверждений | [`ConfirmsPublish`](../components/transactions/confirms-publish.md) ([Надёжная публикация](publishing.md)) |
| очередь для ответов и таблица корреляции | [`LapinRequest`](../components/request/index.md) ([Запрос и ответ](rpc.md)) |
