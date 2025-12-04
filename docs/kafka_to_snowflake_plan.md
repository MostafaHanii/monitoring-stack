# Implementation Plan - Kafka to Snowflake Pipeline

This plan outlines the implementation of a robust streaming pipeline from Kafka to Snowflake using **Kafka Connect**. This replaces the previous "Prometheus to Snowflake" plan to ensure data integrity and scalability.

## User Review Required

> [!IMPORTANT]
> **Snowflake Credentials**: You will need a Snowflake account. The connector requires:
> - `snowflake.url.name` (e.g., `xy12345.us-east-1.aws.snowflakecomputing.com`)
> - `snowflake.user.name`
> - `snowflake.private.key` (Best practice) or Password. **[See Setup Guide](file:///d:/monitoring-stack/docs/snowflake_key_setup.md)**
> - `snowflake.database.name`
> - `snowflake.schema.name`

## Proposed Changes

### Infrastructure

#### [MODIFY] [docker-compose.yml](file:///d:/monitoring-stack/docker-compose.yml)
- **Add Service**: `kafka-connect`
    - Image: `confluentinc/cp-kafka-connect:7.4.0`
    - Ports: `8083:8083`
    - Environment:
        - `CONNECT_BOOTSTRAP_SERVERS`: `kafka:29092`
        - `CONNECT_PLUGIN_PATH`: `/usr/share/java,/usr/share/confluent-hub-components`
    - **Volume**: Mount a local directory to install the Snowflake connector JAR.

### Topic Strategy
> [!NOTE]
> **Reuse Existing Topic**: We will use the existing `binance_kline` topic.
> - **Why?**: The `binance_exporter` already produces clean, flat JSON data to this topic. Kafka allows multiple consumers, so we can have both the Prometheus Consumer and the Snowflake Sink Connector reading from the same stream without conflict.
> - **Benefit**: No need to duplicate data or run a separate exporter.

### Configuration

#### [NEW] [connectors/snowflake-sink.json](file:///d:/monitoring-stack/connectors/snowflake-sink.json)
- **Configuration for the Snowflake Sink Connector**:
    ```json
    {
      "name": "snowflake-sink",
      "config": {
        "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
        "tasks.max": "1",
        "topics": "binance_kline",
        "buffer.count.records": "10000",
        "buffer.flush.time": "60",
        "buffer.size.bytes": "5000000",
        "snowflake.url.name": "<YOUR_SNOWFLAKE_URL>",
        "snowflake.user.name": "<YOUR_USER>",
        "snowflake.private.key": "<YOUR_PRIVATE_KEY>",
        "snowflake.database.name": "CRYPTO_DB",
        "snowflake.schema.name": "PUBLIC",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
      }
    }
    ```

## Verification Plan

### Automated Tests
- **Connector Status**: Check `curl http://localhost:8083/connectors/snowflake-sink/status` to ensure it's `RUNNING`.

### Manual Verification
1.  Start the stack: `docker-compose up -d`.
2.  Deploy the connector: `curl -X POST -H "Content-Type: application/json" --data @connectors/snowflake-sink.json http://localhost:8083/connectors`.
3.  Check Snowflake:
    ```sql
    SELECT * FROM CRYPTO_DB.PUBLIC.binance_kline LIMIT 10;
    ```
