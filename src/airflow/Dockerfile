# src/airflow/Dockerfile
FROM apache/airflow:2.10.5-python3.12@sha256:6499a680a93463846d3a6be980e85d601dc97b0d81e82eed9ef5e5cb9da31b79

USER airflow

RUN pip install --no-cache-dir \
    requests==2.34.2
