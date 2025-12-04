# 🚀 Real-time Cryptocurrency Monitoring & ELT Stack

A robust, containerized pipeline for real-time monitoring and historical analysis of cryptocurrency OHLC (Open, High, Low, Close) candle data from Binance.

---

## 🏗️ System Architecture

The pipeline consists of two parallel flows:

### 1. Real-time Monitoring Flow
*   **Source**: `ohlc-exporter` connects to Binance WebSocket API.
*   **Broker**: Kafka acts as the central message bus.
*   **Aggregator**: `prometheus-consumer` reads from Kafka and exposes metrics.
*   **Visualization**: Grafana dashboards powered by Prometheus.

### 2. ELT Data Pipeline (Snowflake)
*   **Ingestion**: **Kafka Connect** streams raw JSON data from Kafka to Snowflake (`binance_kline` table).
*   **Transformation**: **dbt** (running in a Docker container) parses the raw JSON and creates a structured, deduplicated `STREAMED_OHLCV` table every hour.

---

## 🔌 Service Ports

| Service | Port | Description | Credentials |
|---------|------|-------------|-------------|
| **Grafana** | `3000` | Main Dashboard UI | `admin` / `password` |
| **Prometheus** | `9090` | Metrics Query UI | - |
| **Kafka Connect** | `8083` | Connector REST API | - |
| **Kafka** | `9092` | Message Broker | - |

---

## 📂 Project Structure & File Descriptions

### Root Directory
*   **`docker-compose.yml`**: The orchestration file. Defines all services (Kafka, Connect, dbt, Prometheus, Grafana).

### 📡 OHLC Exporter (`ohlc-exporter/`)
*   **`binance_exporter.py`**: The data source. Connects to Binance WebSocket, filters for closed 1-minute candles, and produces JSON messages to Kafka topic `binance_kline`.

### ⚡ Prometheus Consumer (`prometheus-consumer/`)
*   **`prometheus_consumer.py`**: Real-time processor. Consumes Kafka messages and updates Prometheus Gauge metrics.

### ❄️ Kafka Connect (`connectors/`)
*   **`snowflake-sink.json`**: Configuration for the Snowflake Sink Connector. Defines how data moves from Kafka to Snowflake.

### 🛠️ dbt Transformation (`dbt_project/`)
*   **`models/staging/stg_binance_kline.sql`**: View that parses raw JSON from Snowflake into columns.
*   **`models/marts/streamed_ohlcv.sql`**: Incremental table that deduplicates and stores the final analytical data.
*   **`run_loop.sh`**: Script that runs `dbt run` every hour inside the `dbt-runner` container.

### 📊 Configuration (`grafana/`, `prometheus/`)
*   **`prometheus/prometheus.yml`**: Scrape config for Prometheus.
*   **`grafana/dashboards/`**: Contains the JSON definition for the "Cryptocurrency OHLC Live Dashboard".

---

## 🚀 How to Run

### 1. Prerequisites
*   **Docker & Docker Compose** installed.
*   **Snowflake Account** with Key Pair Authentication configured.
*   **RSA Key**: Place your `rsa_key.p8` file in the root directory (it is gitignored).

### 2. Start the Stack
```bash
docker-compose up -d
```

### 3. Verify Data Flow
*   **Grafana**: [http://localhost:3000](http://localhost:3000) (Real-time charts)
*   **Kafka Connect**: Check status of the connector:
    ```bash
    curl localhost:8083/connectors/snowflake-sink/status
    ```
*   **Snowflake**: Query the `STREAMED_OHLCV` table to see structured data arriving.

### 4. Stop the Stack
```bash
docker-compose down
```
