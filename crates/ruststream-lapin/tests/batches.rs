//! Pulling one batch off a subscriber's batch stream, the way the runtime lends a slot to fill.

use std::future::poll_fn;
use std::pin::Pin;

use ruststream::BatchStream;
use ruststream_lapin::LapinMessage;

/// Fills a fresh slot from `stream` and returns it, or `None` when the subscription ended.
pub(crate) async fn next_batch<Stream>(
    mut stream: Pin<&mut Stream>,
) -> Option<Result<Vec<LapinMessage>, Stream::Error>>
where
    Stream: BatchStream<Vec<LapinMessage>>,
{
    let mut slot = Vec::new();
    let step = poll_fn(|cx| stream.as_mut().poll_fill(cx, &mut slot)).await;
    step.map(|filled| filled.map(|()| slot))
}
