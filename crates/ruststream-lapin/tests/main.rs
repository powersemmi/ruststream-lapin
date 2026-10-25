//! The integration suite, one test binary: each file under `tests/` is a module of it.

mod live;
#[path = "live/proxy.rs"]
mod proxy;

mod asyncapi_lapin;
mod channel_recovery_live_lapin;
mod conformance_lapin;
mod connection_recovery_live_lapin;
mod dead_letter_live_lapin;
mod delay_live_lapin;
mod harness_lapin;
mod in_process_lapin;
mod integration_lapin;
mod manual_lapin;
mod plugins_lapin;
mod prelude_lapin;
mod publish_allocations;
mod publish_options_live_lapin;
mod recovery;
mod recovery_lapin;
mod request_reply_lapin;
mod request_reply_live_lapin;
mod retry_lapin;
mod retry_live_lapin;
