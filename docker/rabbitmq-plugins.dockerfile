# RabbitMQ with the plugins the `plugin-consistent-hash` and `plugin-dme` feature tests need:
# the bundled consistent-hash exchange and the community delayed-message-exchange (fetched here).
#
# Build and run through docker-compose.test.yml (the `plugins` profile), or directly:
#   docker build -f docker/rabbitmq-plugins.dockerfile -t ruststream-rabbitmq-plugins .
FROM rabbitmq:4.2-management-alpine

# Delayed-message-exchange is a community plugin, so its .ez is downloaded into the plugins dir.
# The DME plugin lags the broker (v4.2.0 supports only 4.2.x), so the base image is pinned to
# RabbitMQ 4.2 here even though the main test broker tracks the latest 4.x.
ARG DME_VERSION=4.2.0
RUN set -eux; \
    wget -O "$RABBITMQ_HOME/plugins/rabbitmq_delayed_message_exchange-${DME_VERSION}.ez" \
        "https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v${DME_VERSION}/rabbitmq_delayed_message_exchange-${DME_VERSION}.ez"; \
    rabbitmq-plugins enable --offline \
        rabbitmq_consistent_hash_exchange \
        rabbitmq_delayed_message_exchange
