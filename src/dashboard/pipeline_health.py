import os
import threading
import time

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "binance-trades")
ACTIVE_ENGINE = os.getenv("PIPELINE_ACTIVE_ENGINE", "flink").strip().lower()
ACTIVE_CONSUMER_GROUP = os.getenv(
    "PIPELINE_CONSUMER_GROUP_ID",
    os.getenv("FLINK_CONSUMER_GROUP_ID", "flink-ohlc-consumer-by-engine-v2"),
)
PROGRESS_WINDOW_SECONDS = int(os.getenv("PIPELINE_PROGRESS_WINDOW_SECONDS", "30"))
MAX_LAG_RECORDS = int(os.getenv("PIPELINE_MAX_LAG_RECORDS", "10000"))
KAFKA_TIMEOUT_SECONDS = 3


class KafkaProgressTracker:
    def __init__(self, window_seconds: int) -> None:
        self.window_seconds = window_seconds
        self.baseline_offset: int | None = None
        self.baseline_time: float | None = None
        self.last_state = "warming_up"

    def observe(self, total_offset: int, now: float) -> dict:
        if (
            self.baseline_offset is None
            or self.baseline_time is None
            or total_offset < self.baseline_offset
        ):
            self.baseline_offset = total_offset
            self.baseline_time = now
            self.last_state = "warming_up"
            return {"state": "warming_up", "delta": None}
        elapsed = now - self.baseline_time
        if elapsed < self.window_seconds:
            return {
                "state": self.last_state,
                "delta": total_offset - self.baseline_offset,
            }
        delta = total_offset - self.baseline_offset
        self.baseline_offset = total_offset
        self.baseline_time = now
        self.last_state = "progressing" if delta > 0 else "stalled"
        return {"state": self.last_state, "delta": delta}


class PipelineHealthMonitor:
    def __init__(self) -> None:
        self.admin: AdminClient | None = None
        self.progress = KafkaProgressTracker(PROGRESS_WINDOW_SECONDS)
        self.lock = threading.Lock()

    def _topic_partitions_and_end_offsets(self) -> tuple[list[TopicPartition], int]:
        consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": "pipeline-health-inspector",
                "enable.auto.commit": False,
                "socket.timeout.ms": KAFKA_TIMEOUT_SECONDS * 1000,
            }
        )
        try:
            metadata = consumer.list_topics(KAFKA_TOPIC, timeout=KAFKA_TIMEOUT_SECONDS)
            topic = metadata.topics.get(KAFKA_TOPIC)
            if topic is None or topic.error is not None:
                raise RuntimeError(f"Kafka topic unavailable: {topic.error if topic else 'missing'}")
            partitions = [TopicPartition(KAFKA_TOPIC, partition) for partition in topic.partitions]
            total_end = sum(
                consumer.get_watermark_offsets(
                    partition,
                    timeout=KAFKA_TIMEOUT_SECONDS,
                    cached=False,
                )[1]
                for partition in partitions
            )
            return partitions, total_end
        finally:
            consumer.close()

    def _consumer_lag(self, partitions: list[TopicPartition], total_end: int) -> int | None:
        if ACTIVE_ENGINE != "flink":
            return None
        if self.admin is None:
            self.admin = AdminClient(
                {
                    "bootstrap.servers": KAFKA_BOOTSTRAP,
                    "socket.timeout.ms": KAFKA_TIMEOUT_SECONDS * 1000,
                }
            )
        request = ConsumerGroupTopicPartitions(ACTIVE_CONSUMER_GROUP)
        futures = self.admin.list_consumer_group_offsets(
            [request], request_timeout=KAFKA_TIMEOUT_SECONDS
        )
        result = futures[ACTIVE_CONSUMER_GROUP].result(timeout=KAFKA_TIMEOUT_SECONDS)
        committed = {
            partition.partition: partition.offset
            for partition in (result.topic_partitions or [])
            if partition.topic == KAFKA_TOPIC and partition.offset >= 0
        }
        if not committed:
            return None
        total_committed = sum(committed.get(partition.partition, 0) for partition in partitions)
        return max(total_end - total_committed, 0)

    def snapshot(self) -> dict:
        with self.lock:
            partitions, total_end = self._topic_partitions_and_end_offsets()
            progress = self.progress.observe(total_end, time.monotonic())
            lag = self._consumer_lag(partitions, total_end)
            lag_state = (
                "not_applicable"
                if ACTIVE_ENGINE != "flink"
                else "unknown"
                if lag is None
                else "ok"
                if lag <= MAX_LAG_RECORDS
                else "high"
            )
            return {
                "topic": KAFKA_TOPIC,
                "partitions": len(partitions),
                "log_end_offset": total_end,
                "progress": progress,
                "active_engine": ACTIVE_ENGINE,
                "consumer_group": ACTIVE_CONSUMER_GROUP if ACTIVE_ENGINE == "flink" else None,
                "consumer_lag_records": lag,
                "lag_state": lag_state,
                "max_lag_records": MAX_LAG_RECORDS,
            }


pipeline_health_monitor = PipelineHealthMonitor()
