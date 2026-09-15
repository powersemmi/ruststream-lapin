set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

default: check

check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo check --workspace --all-targets --all-features
    cargo check --workspace --no-default-features
    # The build a service that only generates its document makes: `asyncapi` without `testing`.
    # Neither of the two legs above has that combination, and a binding hook reaching an accessor
    # the other feature gates compiles in both of them.
    cargo check --workspace --no-default-features --features asyncapi

test:
    cargo test --workspace --all-features

brokers-up:
    docker compose -f docker-compose.test.yml up -d --wait

brokers-down:
    docker compose -f docker-compose.test.yml down -v

# The whole live suite on both stands. RUSTSTREAM_REQUIRE_LIVE turns a skipped live test into a
# failure: the stands are up, so a suite that returns before its first assertion is a defect, and
# it reads exactly like a suite that ran.
test-brokers: brokers-up plugins-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just plugins-down' EXIT
    AMQP_TEST_URL=amqp://127.0.0.1:5672 \
    AMQP_PLUGINS_TEST_URL=amqp://127.0.0.1:5673 \
    RUSTSTREAM_REQUIRE_LIVE=1 \
        cargo test --workspace --all-features -- --test-threads=1

# The plugin-enabled broker (consistent-hash + delayed-message-exchange), off by default.
plugins-up:
    docker compose -f docker-compose.test.yml --profile plugins up -d --wait rabbitmq-plugins

plugins-down:
    docker compose -f docker-compose.test.yml --profile plugins down -v

# Run the plugin feature tests against the plugin-enabled broker.
test-plugins: plugins-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just plugins-down' EXIT
    AMQP_PLUGINS_TEST_URL=amqp://127.0.0.1:5673 \
    RUSTSTREAM_REQUIRE_LIVE=1 \
        cargo test -p ruststream-lapin --features plugin-consistent-hash,plugin-dme \
        --test plugins_lapin -- --test-threads=1

fmt:
    cargo fmt --all

build:
    cargo build --workspace --release

security: deny zizmor

# Dependency-graph checks (advisories, licenses, duplicates, sources).
# Needs cargo-deny: cargo install cargo-deny --locked
deny:
    cargo deny check

zizmor:
    uvx zizmor .github/workflows

typo:
    uvx codespell

clean:
    cargo clean
    rm -rf dist wheels

ci: check test typo security
