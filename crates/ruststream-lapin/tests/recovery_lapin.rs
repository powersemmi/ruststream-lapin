//! Recovery defaults and server-transaction compatibility in the in-process transport.
#![cfg(feature = "testing")]

use ruststream::testing::InProcess;
use ruststream::{
    ConnectedBroker, OutgoingMessage, PublishPolicy, Publisher, TransactionalPublisher,
};
use ruststream_lapin::{AmqpError, LapinBroker, LapinPublish};

#[tokio::test]
async fn enabled_recovery_refuses_every_server_transaction_operation() {
    let broker = LapinBroker::new("amqp://localhost")
        .auto_recover(true)
        .connect_in_process()
        .await
        .expect("connect");
    assert!(broker.is_connected());
    let publisher = broker.publisher(LapinPublish::default().server_tx());
    for result in [
        publisher.begin_transaction().await,
        publisher
            .publish(OutgoingMessage::new("queue", b"data"), None)
            .await,
        publisher.commit().await,
        publisher.abort().await,
    ] {
        let Err(AmqpError::InvalidOptions(message)) = result else {
            panic!("server transactions must reject automatic recovery: {result:?}")
        };
        assert!(message.contains("auto_recover(false)"));
        assert!(message.contains("ServerTxPublisher"));
        assert!(message.contains("auto_recover(true)"));
    }
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn server_tx_pairing_rejects_enabled_recovery_before_use() {
    let broker = LapinBroker::new("amqp://localhost")
        .auto_recover(true)
        .connect_in_process()
        .await
        .expect("connect");
    let error = LapinPublish::default()
        .server_tx()
        .pair(&broker)
        .await
        .expect_err("pairing must reject an unsupported publisher before it can be used");
    let message = error.to_string();
    assert!(message.contains("ServerTxPublisher"));
    assert!(message.contains("auto_recover(true)"));
    assert!(message.contains("auto_recover(false)"));
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn recovery_is_disabled_by_default_and_server_transactions_work() {
    let broker = LapinBroker::new("amqp://localhost")
        .connect_in_process()
        .await
        .expect("connect");
    let publisher = LapinPublish::default()
        .server_tx()
        .pair(&broker)
        .await
        .expect("default allows pairing server transactions");
    publisher
        .begin_transaction()
        .await
        .expect("default allows server transactions");
    publisher.abort().await.expect("abort");
    broker.shutdown().await.expect("shutdown");
}
