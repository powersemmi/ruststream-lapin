//! The gate the live suites share: when a run is supposed to reach a server, a skip is a defect.
//!
//! A live test skips when its address variable is unset, which is what keeps the suites usable
//! during development: `cargo test` on a laptop with no stand passes. The same skip in CI is a
//! lie, because the job started a broker first, and a suite that returns before its first
//! assertion reports `ok` exactly like one that ran. Each live job therefore sets
//! `RUSTSTREAM_REQUIRE_LIVE` beside its address, and under that flag every skip becomes a failure
//! naming the variable it wanted.
//!
//! There are two addresses here, one per stand: `AMQP_TEST_URL` for the plain broker and
//! `AMQP_PLUGINS_TEST_URL` for the plugin-enabled one. Each job sets the flag next to its own, so
//! a job that loses its address fails instead of passing empty.

/// The variable a job sets to say it started a broker, so skipping past it is a defect.
pub(crate) const REQUIRE_LIVE: &str = "RUSTSTREAM_REQUIRE_LIVE";

/// Whether this run is required to reach a live broker.
fn required() -> bool {
    std::env::var(REQUIRE_LIVE).is_ok_and(|value| !value.is_empty())
}

/// The broker address from `name`, or `None` to skip the test.
///
/// # Panics
///
/// Panics when [`REQUIRE_LIVE`] is set and `name` is not: a job that started a broker and then
/// lost its address is a broken job, and the tests behind it would have reported `ok` without
/// running.
pub(crate) fn url(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Some(value),
        _ => {
            assert!(
                !required(),
                "{REQUIRE_LIVE} is set, so this suite must run, but {name} is unset or empty",
            );
            None
        }
    }
}
