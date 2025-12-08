FROM confluentinc/cp-kafka-connect:7.9.0

ARG CLICKHOUSE_KAFKA_CONNECT_VERSION=1.3.4

USER root
RUN microdnf update -y \
    && microdnf install -y curl unzip ca-certificates \
    && microdnf clean all \
    && curl -fSL "https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/v${CLICKHOUSE_KAFKA_CONNECT_VERSION}/clickhouse-kafka-connect-v${CLICKHOUSE_KAFKA_CONNECT_VERSION}.zip" -o /tmp/clickhouse-kafka-connect.zip \
    && unzip /tmp/clickhouse-kafka-connect.zip -d /usr/share/java/clickhouse-kafka-connect \
    && rm -rf /tmp/clickhouse-kafka-connect.zip

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components,/usr/share/java/clickhouse-kafka-connect"
USER appuser
