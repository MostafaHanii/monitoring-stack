# 🚀 Real-time Cryptocurrency Monitoring & ELT Stack

A containerized pipeline for **real-time monitoring** and **historical analysis** of Binance cryptocurrency data (OHLCV).

## 🏗️ Architecture

1.  **Real-Time Flow**: streams 1-minute candles via **Kafka**, exposing metrics to **Prometheus** for live **Grafana** dashboards.
2.  **ELT Pipeline**: loads raw data into **Snowflake** via **Kafka Connect**, then transforms it using **dbt** and **Airflow** for historical reporting.

## 📂 Structure

### Real-Time & ELT
*   **`ohlc-exporter/`**: Python source streaming Binance WebSocket data to Kafka.
*   **`prometheus-consumer/`**: Aggregates Kafka data for real-time Prometheus metrics.
*   **`connectors/`**: Kafka Connect configs for Snowflake ingestion.
*   **`dbt_project/`**: Transformation models for the streaming pipeline.
*   **`grafana/`** & **`prometheus/`**: Monitoring and dashboard configurations.

### Batch Processing (`crypto_batchProcessing/`)
*   **`airflow/`**: Orchestration for batch workflows.
*   **`crypto_dbt/`**: Dedicated dbt project for batch data modeling.
*   **`snowflake_scripts/`**: SQL setup for the batch layer.

