# Брокер RabbitMQ

`ruststream-lapin` запускает сервис RustStream на RabbitMQ поверх [`lapin`](https://docs.rs/lapin),
клиента AMQP 0.9.1. Очередь RabbitMQ - разрушающее хранилище: подтверждённая доставка уходит из
очереди. Подтверждение, возврат в очередь и отправка в очередь недоставленных сообщений - это кадры
протокола, поэтому фреймворк завершает сообщение на самом брокере.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-lapin = "0.7"
serde = { version = "1", features = ["derive"] }
```

`LapinBroker::new` не выполняет ввод-вывод, поэтому сервис на RabbitMQ собирается тем же макросом
`#[ruststream::app]`, что и на любом другом брокере. Рантайм подключает брокер на старте, до
открытия подписок. Подключение поглощает брокер и возвращает `ConnectedLapinBroker` - единственное
значение, у которого есть поверхность подписки и публикации.

Тело обработчика называет совместимости: `Out<impl Publisher>`, `Out<impl TransactionalPublisher>`,
`Out<impl RequestReply>`. Оно импортирует только прелюдию фреймворка, и поэтому один и тот же
обработчик монтируется и на настоящий брокер, и на внутрипроцессный тестовый. Прелюдию этого крейта
импортирует одно тело: то, которое меняет
[свойство AMQP][properties] для одного сообщения. Оно называет тип
настроек, а не тип издателя - `Out<impl Publisher<Options = LapinPublishOptions>>`, - поэтому
обработчик по-прежнему монтируется на оба.

Файл маршрутов называет значения и импортирует `ruststream_lapin::prelude::*`, который приносит с
собой прелюдию фреймворка. Он добавляет единые для всего семейства имена точки монтирования:
`Publish`, `TransactionalPublish` и `Request` - псевдонимы для `LapinPublish`, `ConfirmsPublish` и
`LapinRequest`.

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_quickstart.rs:handler"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_quickstart.rs:app"
```

## Модель транспорта {#the-transport-model}

- Подписка читает одну очередь. `#[subscriber("orders")]` читает очередь `orders`, а дескриптор
  [`RabbitQueue`][subscribing] добавляет привязки, типы очередей и предвыборку. Пакетный обработчик
  называет свой размер в точке монтирования, и крейт собирает пакет на клиенте, потому что в AMQP
  нет пакета на уровне протокола; смотрите [Пакеты][batches].
- На стороне публикации имя сообщения - это ключ маршрутизации, а обменник принадлежит политике
  публикации: пока вы не назовёте другой, это обменник по умолчанию. Смотрите
  [Публикацию][publishing].
- Завершение доставки нативное: `ack` отправляет `basic.ack`, повтор - `basic.reject(requeue = true)`,
  отбрасывание - `basic.reject(requeue = false)`, который отправляет сообщение в очередь
  недоставленных, если у очереди задан такой обменник. Отказ - это кадр, который RabbitMQ считает
  потраченной доставкой, поэтому повтор из обработчика расходует собственный лимит кворумной
  очереди.
- На брокере ничего не объявляется, пока вы не включите это через `.declare_topology(true)`:
  топологией управляете вы.

## Совместимости {#capabilities}

Какие из необязательных трейтов-совместимостей фреймворка этот брокер реализует нативно:

| Совместимость | Нативно | Примечания |
| --- | --- | --- |
| `Subscribe` | да | Читает очередь, которую называет подписка; [`RabbitQueue`](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#subscribing) добавляет привязки, тип очереди и предвыборку. |
| `BatchSubscriber` | да, на клиенте | AMQP присылает по одному `basic.deliver`, поэтому пакет собирается на клиенте до размера, названного в точке монтирования, в пределах окна предвыборки дескриптора и `batch_wait`. Смотрите [Пакеты](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#batches). |
| `TransactionalPublisher` | да | Оба транзакционных издателя: `.confirms()` буферизует на клиенте и при фиксации дожидается каждого подтверждения, `.server_tx()` использует транзакции канала AMQP. Смотрите [Публикацию](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#publishing). |
| `OwnedTransactions` | да (только confirms) | Транзакция на подтверждениях - это клиентский буфер, поэтому на одном дескрипторе их может быть открыто сколько угодно. `server_tx` переводит в транзакционный режим сам канал, а это состояние канала, существующее ровно в одном экземпляре. |
| `RequestReply` | да | `LapinRequest` конструирует запросчика поверх direct reply-to с мультиплексированием по `correlation-id`. Смотрите [Запрос-ответ](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#request-and-reply). |
| `Partitioned` | да | Производитель кладёт ключ в заголовок `amqp-partition-key`, и партиции воркеров рантайма читают его; сам AMQP этот заголовок не интерпретирует. Смотрите [Метаданные доставки](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#delivery-metadata). |
| `Seekable` + `Positioned` | нет | Очередь не хранит историю, в которую можно было бы вернуться. |
| `DescribeServer` | да | Сообщает хост подключения и версию AMQP за ним, и именно это записывает документ AsyncAPI. См. [Документ AsyncAPI](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#the-asyncapi-document). |

## Каркас сервиса {#scaffold-a-service}

Сгенерируйте работающую заготовку через
[`cargo generate`](https://github.com/cargo-generate/cargo-generate), по одному шаблону на форму
обмена сообщениями:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-queue
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-topic
```

## Документация {#documentation}

Крейт документирует себя на docs.rs, и этот обзор и есть руководство:

- [Подписка][subscribing] - два дескриптора очередей, типы очередей, привязки, предвыборка,
  предел повторов, отложенная повторная доставка, пакеты и метаданные доставки.
- [Публикация][publishing] - модель маршрутизации, политики, свойства AMQP для одного сообщения,
  ответы и транзакции.
- [Запрос-ответ][request-reply] - RPC поверх direct reply-to.
- [Документ AsyncAPI][documenting] - привязки AMQP, которые сервис сообщает о себе.
- [Тестирование][testing] - внутрипроцессный тестовый брокер под обвязкой `TestApp`.
- [Эксплуатация][operations] - TLS, настройки подключения, объявление по запросу и известные
  пробелы.

Установка, учебник и остальные брокеры - на сайте RustStream:
<https://powersemmi.github.io/ruststream/>.

[subscribing]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#subscribing
[batches]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#batches
[publishing]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#publishing
[properties]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#per-message-properties
[request-reply]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#request-and-reply
[documenting]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#the-asyncapi-document
[testing]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#testing
[operations]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#operations
