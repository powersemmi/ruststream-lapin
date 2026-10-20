//! Interrupts test connections without touching the `RabbitMQ` server or other tests.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use lapin::uri::AMQPUri;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::yield_now;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout as timeout_future;

pub(super) struct Proxy {
    pub(super) url: String,
    #[allow(dead_code)]
    control: watch::Sender<(bool, u64)>,
    task: JoinHandle<()>,
    accepted: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
}

#[allow(dead_code)] // Recovery suites use different proxy controls.
impl Proxy {
    pub(super) async fn new(url: &str) -> Self {
        Self::start(url, true).await
    }

    /// Refuse TCP connections until brought online, without resetting an AMQP handshake.
    pub(super) async fn new_offline(url: &str) -> Self {
        Self::start(url, false).await
    }

    async fn start(url: &str, online: bool) -> Self {
        let uri: AMQPUri = url.parse().expect("test AMQP URI");
        assert_eq!(
            uri.scheme.to_string(),
            "amqp",
            "the test proxy forwards plaintext AMQP"
        );
        let upstream = (uri.authority.host.clone(), uri.authority.port);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("proxy bind");
        let address = listener.local_addr().expect("proxy address");
        let port = address.port();
        let mut listener = online.then_some(listener);
        let mut proxied = uri;
        "127.0.0.1".clone_into(&mut proxied.authority.host);
        proxied.authority.port = port;
        let (control, mut changes) = watch::channel((online, 0));
        let accepted = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let totals = Arc::clone(&accepted);
        let open = Arc::clone(&active);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    result = async { listener.as_ref().expect("online listener").accept().await }, if listener.is_some() => {
                        let (mut client, _) = result.expect("proxy accept");
                        if !changes.borrow().0 { continue; }
                        let upstream = upstream.clone();
                        let mut disconnected = changes.clone();
                        connections.spawn(async move {
                            tokio::select! {
                                _ = disconnected.changed() => {}
                                () = async {
                                    let mut server = TcpStream::connect(upstream).await.expect("proxy upstream");
                                    let _ = copy_bidirectional(&mut client, &mut server).await;
                                } => {}
                            }
                        });
                        open.store(connections.len(), Ordering::Release);
                        totals.fetch_add(1, Ordering::Release);
                    }
                    notification = changes.changed() => {
                        if notification.is_err() { return; }
                        connections.abort_all();
                        if listener.is_none() && changes.borrow().0 {
                            listener = Some(TcpListener::bind(address).await.expect("proxy online bind"));
                        }
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
                open.store(connections.len(), Ordering::Release);
            }
        });
        Self {
            url: proxied.to_string(),
            control,
            task,
            accepted,
            active,
        }
    }

    pub(super) fn connections(&self) -> (usize, usize) {
        (
            self.accepted.load(Ordering::Acquire),
            self.active.load(Ordering::Acquire),
        )
    }

    pub(super) fn offline(&self) {
        self.control.send_modify(|(online, epoch)| {
            *online = false;
            *epoch += 1;
        });
    }

    pub(super) fn online(&self) {
        self.control.send_modify(|(online, epoch)| {
            *online = true;
            *epoch += 1;
        });
    }

    pub(super) async fn wait_for(&self, condition: impl Fn() -> bool + Send + Sync) {
        timeout_future(Duration::from_secs(30), async {
            while !condition() {
                yield_now().await;
            }
        })
        .await
        .expect("connection state within timeout");
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
