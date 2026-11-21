FROM python:3.14-slim@sha256:ce40764625a4ff50df3548277632e7f96c4e77fe75fa848aae9885476e7df5a4

WORKDIR /app

RUN pip install --no-cache-dir \
    confluent-kafka==2.15.0 \
    websockets==17.0.1

COPY src/producer/binance_producer.py .

CMD ["python", "binance_producer.py"]
