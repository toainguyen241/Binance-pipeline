import argparse
import asyncio
import json
import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import websockets
from confluent_kafka import KafkaException, Producer
from websockets.exceptions import ConnectionClosed

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("binance_producer")

STREAMS = [
    "btcusdt@trade",
    "ethusdt@trade",
    "solusdt@trade",
    "bnbusdt@trade",
    "xrpusdt@trade",
    "adausdt@trade",
    "dogeusdt@trade",
    "avaxusdt@trade",
]
WS_URL = f"wss://stream.binance.com:9443/stream?streams={'/'.join(STREAMS)}"
KAFKA_TOPIC = "binance-trades"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RECONNECT_INITIAL_DELAY_SECONDS = 1
RECONNECT_MAX_DELAY_SECONDS = 30
KAFKA_METADATA_TIMEOUT_SECONDS = 5
KAFKA_MESSAGE_TIMEOUT_MS = int(os.getenv("KAFKA_MESSAGE_TIMEOUT_MS", "15000"))
KAFKA_QUEUE_HIGH_WATERMARK = int(os.getenv("KAFKA_QUEUE_HIGH_WATERMARK", "10000"))
WS_PING_INTERVAL_SECONDS = int(os.getenv("BINANCE_WS_PING_INTERVAL_SECONDS", "20"))
WS_PING_TIMEOUT_SECONDS = int(os.getenv("BINANCE_WS_PING_TIMEOUT_SECONDS", "20"))
WS_OPEN_TIMEOUT_SECONDS = int(os.getenv("BINANCE_WS_OPEN_TIMEOUT_SECONDS", "10"))
WS_CLOSE_TIMEOUT_SECONDS = int(os.getenv("BINANCE_WS_CLOSE_TIMEOUT_SECONDS", "5"))
INVALID_FRAME_LOG_EVERY = 1000
HEARTBEAT_FILE = os.getenv("PRODUCER_HEARTBEAT_FILE", "")
HEARTBEAT_INTERVAL_SECONDS = int(os.getenv("PRODUCER_HEARTBEAT_INTERVAL_SECONDS", "5"))


class HeartbeatWriter:
    """Rate-limit heartbeat writes so healthchecks do not create disk I/O per trade."""

    def __init__(self, path: str, interval_seconds: int) -> None:
        self.path = Path(path) if path else None
        self.interval_seconds = max(interval_seconds, 1)
        self.last_write = 0.0

    def update(self) -> None:
        if self.path is None:
            return
        now = time.monotonic()
        if now - self.last_write < self.interval_seconds:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch()
        self.last_write = now


class KafkaUnavailableError(RuntimeError):
    """Kafka is not ready, or delivery has failed."""


@dataclass
class KafkaDeliveryMonitor:
    first_error: object | None = None
    failure_count: int = 0

    def callback(self, err, msg) -> None:
        if err is None:
            return
        self.failure_count += 1
        if self.first_error is None:
            self.first_error = err
            log.error("Kafka delivery thất bại; sẽ tạo lại producer: %s", err)

    def raise_if_failed(self) -> None:
        if self.first_error is not None:
            raise KafkaUnavailableError(str(self.first_error))


def delivery_report(err, msg):
    if err is not None:
        log.error("Publish thất bại: %s", err)


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "linger.ms": 20,
            "compression.type": "lz4",
            "acks": "1",
            "message.timeout.ms": KAFKA_MESSAGE_TIMEOUT_MS,
            "queue.buffering.max.messages": KAFKA_QUEUE_HIGH_WATERMARK * 2,
        }
    )


def ensure_kafka_ready(producer: Producer) -> None:

    metadata = producer.list_topics(KAFKA_TOPIC, timeout=KAFKA_METADATA_TIMEOUT_SECONDS)
    topic = metadata.topics.get(KAFKA_TOPIC)
    if topic is None or topic.error is not None:
        detail = topic.error if topic is not None else "topic missing"
        raise KafkaUnavailableError(f"Kafka topic chưa sẵn sàng: {detail}")
    if not topic.partitions or any(p.leader < 0 for p in topic.partitions.values()):
        raise KafkaUnavailableError("Kafka topic chưa có partition leader sẵn sàng")


def parse_trade_message(raw: str) -> dict:

    envelope = json.loads(raw)
    payload = envelope["data"]
    return {
        "symbol": payload["s"],
        "trade_id": payload["t"],
        "price": payload["p"],
        "quantity": payload["q"],
        "trade_time": payload["T"],
        "is_buyer_maker": payload["m"],
    }


async def consume(
    producer: Producer,
    debug: bool = False,
    on_message: Callable[[], None] | None = None,
    delivery_monitor: KafkaDeliveryMonitor | None = None,
) -> None:
    invalid_frame_count = 0
    heartbeat = HeartbeatWriter(HEARTBEAT_FILE, HEARTBEAT_INTERVAL_SECONDS)
    async with websockets.connect(
        WS_URL,
        ping_interval=WS_PING_INTERVAL_SECONDS,
        ping_timeout=WS_PING_TIMEOUT_SECONDS,
        open_timeout=WS_OPEN_TIMEOUT_SECONDS,
        close_timeout=WS_CLOSE_TIMEOUT_SECONDS,
    ) as ws:
        log.info(
            "Đã kết nối Binance WebSocket, đang publish vào topic '%s'", KAFKA_TOPIC
        )
        while True:
            raw = await ws.recv()
            if on_message is not None:
                on_message()
            try:
                record = parse_trade_message(raw)  # type: ignore
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                invalid_frame_count += 1
                if (
                    invalid_frame_count == 1
                    or invalid_frame_count % INVALID_FRAME_LOG_EVERY == 0
                ):
                    log.warning(
                        "Bỏ qua Binance frame không hợp lệ (tổng=%d): %s",
                        invalid_frame_count,
                        exc,
                    )
                continue

            try:
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=record["symbol"],
                    value=json.dumps(record),
                    callback=(
                        delivery_monitor.callback
                        if delivery_monitor is not None
                        else delivery_report
                    ),
                )
            except BufferError as exc:
                raise KafkaUnavailableError(
                    "Kafka local queue đã đầy; dừng nhận thêm trade để recovery"
                ) from exc
            if debug:
                log.info("Sent: %s", record)
            producer.poll(0)
            if delivery_monitor is not None:
                delivery_monitor.raise_if_failed()
            heartbeat.update()

            while len(producer) >= KAFKA_QUEUE_HIGH_WATERMARK:
                log.warning(
                    "Kafka local queue đạt %d message; tạm dừng đọc WebSocket",
                    len(producer),
                )
                producer.poll(0.1)
                if delivery_monitor is not None:
                    delivery_monitor.raise_if_failed()
                await asyncio.sleep(0.05)


async def consume_with_reconnect(
    producer: Producer,
    debug: bool = False,
    delivery_monitor: KafkaDeliveryMonitor | None = None,
) -> None:

    delay = RECONNECT_INITIAL_DELAY_SECONDS

    def reset_backoff() -> None:
        nonlocal delay
        delay = RECONNECT_INITIAL_DELAY_SECONDS

    while True:
        try:
            if delivery_monitor is None:
                await consume(producer, debug=debug, on_message=reset_backoff)
            else:
                await consume(
                    producer,
                    debug=debug,
                    on_message=reset_backoff,
                    delivery_monitor=delivery_monitor,
                )
        except asyncio.CancelledError:
            raise
        except (ConnectionClosed, OSError, TimeoutError) as exc:
            log.warning(
                "Mất kết nối Binance WebSocket (%s). Kết nối lại sau %s giây.",
                exc,
                delay,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY_SECONDS)


async def run_with_kafka_recovery(
    debug: bool = False,
    producer_factory: Callable[[], Producer] = build_producer,
) -> None:

    delay = RECONNECT_INITIAL_DELAY_SECONDS

    while True:
        producer = producer_factory()
        monitor = KafkaDeliveryMonitor()
        try:
            ensure_kafka_ready(producer)
            log.info("Kafka đã sẵn sàng tại %s", KAFKA_BOOTSTRAP)
            delay = RECONNECT_INITIAL_DELAY_SECONDS
            await consume_with_reconnect(
                producer,
                debug=debug,
                delivery_monitor=monitor,
            )
        except asyncio.CancelledError:
            producer.flush(timeout=10)
            raise
        except (KafkaUnavailableError, KafkaException, OSError) as exc:
            log.warning(
                "Kafka unavailable (%s). Tạo lại producer sau %s giây; "
                "có thể mất các message đã timeout trong local queue.",
                exc,
                delay,
            )
            producer.flush(timeout=2)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY_SECONDS)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug",
        action="store_true",
        help="In log mỗi trade ra terminal — chỉ dùng khi debug/demo, không dùng khi chạy thật vì log tần suất cao làm chậm vòng lặp chính.",
    )
    args = parser.parse_args()

    await run_with_kafka_recovery(debug=args.debug)


if __name__ == "__main__":
    asyncio.run(main())
