FROM apache/airflow:3.0.0

USER root

RUN apt-get update \
    && apt-get install -y git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
