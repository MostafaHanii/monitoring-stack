# Cryptocurrency Monitoring Pipeline

## Project Purpose
This project is a comprehensive data engineering solution designed to obtain, process, and visualize cryptocurrency market data. It serves two main purposes:
1.  **Real-Time Monitoring**: To provide low-latency visibility into live crypto market trends (OHLCV) using streaming technologies.
2.  **Historical Analysis**: To build a robust data warehouse for long-term trend analysis, reporting, and advanced analytics on top of the ingested data.

## Project Structure

The project is divided into two distinct processing paradigms:

### 1. Real-Time Streaming Pipeline
This component handles the immediate ingestion and processing of live data. It is designed for speed and instant observability.

*   **`ohlc-exporter/`**: A Python-based producer that connects to the Binance WebSocket API and pushes live 1-minute closed candle data into Kafka topics.
*   **`prometheus-consumer/`**: A lightweight consumer that reads directly from Kafka to aggregate data and expose real-time metrics (like price, volume, and latency) to Prometheus.
*   **`grafana/`**: Contains the provisioning and dashboards for visualizing the real-time data collected by Prometheus.
*   **`connectors/`**: Configuration for Kafka Connect, which acts as the bridge to stream raw data from Kafka directly into Snowflake for storage.

### 2. Batch Processing Pipeline (`crypto_batchProcessing`)
This component handles heavy-duty historical data processing, ensuring data quality and enabling complex analytical queries.

*   **`airflow/`**: The orchestration layer. It manages the scheduling and execution of batch workflows, ensuring dependent tasks run in the correct order.
*   **`crypto_dbt/`**: The transformation layer. A dedicated dbt project that takes the raw data loaded into Snowflake and transforms it into clean, modelled tables for analysis.
*   **`snowflake_scripts/`**: Contains the SQL scripts necessary to set up the data warehouse environment, including schemas, roles, and tables.

## Summary
This stack combines the power of **Apache Kafka** for real-time streaming with **Snowflake** and **dbt** for modern data warehousing. By orchestrating everything with **Docker** and **Airflow**, it provides a robust, end-to-end platform for both live market monitoring and deep historical analysis of cryptocurrency data.
