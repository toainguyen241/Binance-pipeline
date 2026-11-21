import asyncio
import json
import logging
from unittest.mock import AsyncMock, MagicMock

import binance_producer
import pytest

SAMPLE_RAW_MESSAGE = json.dumps(
    {
        "stream": "btcusdt@trade",
        "data": {
            "s": "BTCUSDT",
            "t": 123456789,
            "p": "65000.50000000",
            "q": "0.00150000",
            "T": 1723123456789,
            "m": True,
        },
    }
)


def test_parse_trade_message_maps_fields_dung():
    record = binance_producer.parse_trade_message(SAMPLE_RAW_MESSAGE)

    assert record == {
        "symbol": "BTCUSDT",
        "trade_id": 123456789,
        "price": "65000.50000000",
        "quantity": "0.00150000",
        "trade_time": 1723123456789,
        "is_buyer_maker": True,
    }


def test_parse_trade_message_thieu_field_bao_loi():
    invalid_raw = json.dumps({"stream": "btcusdt@trade", "data": {"s": "BTCUSDT"}})

    with pytest.raises(KeyError):
        binance_producer.parse_trade_message(invalid_raw)


def test_delivery_report_log_loi_khi_that_bai(caplog):
    with caplog.at_level(logging.ERROR):
        binance_producer.delivery_report("mock kafka error", None)

    assert "Publish thất bại" in caplog.text


def test_delivery_report_khong_log_khi_thanh_cong(caplog):
    with caplog.at_level(logging.ERROR):
        binance_producer.delivery_report(None, MagicMock())

    assert caplog.text == ""


def test_build_producer_dung_config(monkeypatch):
    captured_conf = {}

    class FakeProducer:
        def __init__(self, conf):
            captured_conf.update(conf)

    monkeypatch.setattr(binance_producer, "Producer", FakeProducer)

    result = binance_producer.build_producer()

    assert isinstance(result, FakeProducer)
    assert captured_conf["bootstrap.servers"] == "localhost:9092"
    assert captured_conf["compression.type"] == "lz4"
    assert captured_conf["acks"] == "1"
    assert captured_conf["linger.ms"] == 20
    assert captured_conf["message.timeout.ms"] == 15000
    assert captured_conf["queue.buffering.max.messages"] == 20000


def test_heartbeat_writer_rate_limit(monkeypatch, tmp_path):
    heartbeat = tmp_path / "producer-heartbeat"
    writer = binance_producer.HeartbeatWriter(str(heartbeat), interval_seconds=5)
    clock = iter([10.0, 12.0, 16.0])
    monkeypatch.setattr(binance_producer.time, "monotonic", lambda: next(clock))

    writer.update()
    first_mtime = heartbeat.stat().st_mtime_ns
    writer.update()
    assert heartbeat.stat().st_mtime_ns == first_mtime
    writer.update()
    assert heartbeat.stat().st_mtime_ns >= first_mtime


def test_ensure_kafka_ready_kiem_tra_topic_va_partition_leader():
    producer = MagicMock()
    topic = MagicMock(error=None)
    topic.partitions = {0: MagicMock(leader=1), 1: MagicMock(leader=1)}
    producer.list_topics.return_value = MagicMock(topics={"binance-trades": topic})

    binance_producer.ensure_kafka_ready(producer)

    producer.list_topics.assert_called_once_with("binance-trades", timeout=5)


def test_ensure_kafka_ready_bao_loi_khi_topic_chua_san_sang():
    producer = MagicMock()
    producer.list_topics.return_value = MagicMock(topics={})

    with pytest.raises(binance_producer.KafkaUnavailableError):
        binance_producer.ensure_kafka_ready(producer)


def test_delivery_monitor_gom_loi_va_raise_mot_lan(caplog):
    monitor = binance_producer.KafkaDeliveryMonitor()

    with caplog.at_level(logging.ERROR):
        monitor.callback("timeout-1", None)
        monitor.callback("timeout-2", None)

    assert monitor.failure_count == 2
    assert caplog.text.count("Kafka delivery thất bại") == 1
    with pytest.raises(binance_producer.KafkaUnavailableError, match="timeout-1"):
        monitor.raise_if_failed()


@pytest.mark.asyncio
async def test_consume_publish_dung_topic_va_key(monkeypatch):
    call_count = {"n": 0}

    async def fake_recv():
        call_count["n"] += 1
        if call_count["n"] == 1:
            return SAMPLE_RAW_MESSAGE
        raise asyncio.CancelledError()

    fake_ws = AsyncMock()
    fake_ws.recv = fake_recv

    fake_ws_cm = AsyncMock()
    fake_ws_cm.__aenter__.return_value = fake_ws
    fake_ws_cm.__aexit__.return_value = False

    monkeypatch.setattr(
        binance_producer.websockets, "connect", lambda *a, **k: fake_ws_cm
    )

    mock_producer = MagicMock()

    with pytest.raises(asyncio.CancelledError):
        await binance_producer.consume(mock_producer)

    mock_producer.produce.assert_called_once()
    _, kwargs = mock_producer.produce.call_args

    assert kwargs["topic"] == "binance-trades"
    assert kwargs["key"] == "BTCUSDT"
    assert kwargs["callback"] == binance_producer.delivery_report

    published_record = json.loads(kwargs["value"])
    assert published_record["symbol"] == "BTCUSDT"
    assert published_record["trade_id"] == 123456789

    mock_producer.poll.assert_called_once_with(0)


@pytest.mark.asyncio
async def test_consume_bo_qua_frame_loi_va_tiep_tuc_publish(monkeypatch, caplog):
    messages = iter(["not-json", SAMPLE_RAW_MESSAGE])

    async def fake_recv():
        try:
            return next(messages)
        except StopIteration:
            raise asyncio.CancelledError() from None

    fake_ws = AsyncMock()
    fake_ws.recv = fake_recv
    fake_ws_cm = AsyncMock()
    fake_ws_cm.__aenter__.return_value = fake_ws
    fake_ws_cm.__aexit__.return_value = False
    connect = MagicMock(return_value=fake_ws_cm)
    monkeypatch.setattr(binance_producer.websockets, "connect", connect)
    producer = MagicMock()

    with caplog.at_level(logging.WARNING), pytest.raises(asyncio.CancelledError):
        await binance_producer.consume(producer)

    producer.produce.assert_called_once()
    assert "Bỏ qua Binance frame không hợp lệ (tổng=1)" in caplog.text
    assert connect.call_args.kwargs == {
        "ping_interval": 20,
        "ping_timeout": 20,
        "open_timeout": 10,
        "close_timeout": 5,
    }


@pytest.mark.asyncio
async def test_reconnect_dung_exponential_backoff_va_gioi_han(monkeypatch):
    attempts = 0

    async def fake_consume(producer, debug=False, on_message=None):
        nonlocal attempts
        attempts += 1
        if attempts <= 6:
            raise OSError("temporary disconnect")
        raise asyncio.CancelledError()

    mock_sleep = AsyncMock()
    monkeypatch.setattr(binance_producer, "consume", fake_consume)
    monkeypatch.setattr(binance_producer.asyncio, "sleep", mock_sleep)

    with pytest.raises(asyncio.CancelledError):
        await binance_producer.consume_with_reconnect(MagicMock())

    assert [call.args[0] for call in mock_sleep.await_args_list] == [1, 2, 4, 8, 16, 30]


@pytest.mark.asyncio
async def test_reconnect_reset_backoff_sau_khi_nhan_message(monkeypatch):
    attempts = 0

    async def fake_consume(producer, debug=False, on_message=None):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("disconnect before data")
        if attempts == 2:
            on_message()  # type: ignore
            raise OSError("disconnect after data")
        raise asyncio.CancelledError()

    mock_sleep = AsyncMock()
    monkeypatch.setattr(binance_producer, "consume", fake_consume)
    monkeypatch.setattr(binance_producer.asyncio, "sleep", mock_sleep)

    with pytest.raises(asyncio.CancelledError):
        await binance_producer.consume_with_reconnect(MagicMock())

    assert [call.args[0] for call in mock_sleep.await_args_list] == [1, 1]


@pytest.mark.asyncio
async def test_kafka_recovery_tao_lai_producer_voi_exponential_backoff(monkeypatch):
    producers = [MagicMock(), MagicMock(), MagicMock()]
    factory = MagicMock(side_effect=producers)
    readiness_attempts = 0

    def fake_ready(producer):
        nonlocal readiness_attempts
        readiness_attempts += 1
        if readiness_attempts <= 2:
            raise binance_producer.KafkaUnavailableError("broker down")

    async def fake_consume(*args, **kwargs):
        raise asyncio.CancelledError()

    mock_sleep = AsyncMock()
    monkeypatch.setattr(binance_producer, "ensure_kafka_ready", fake_ready)
    monkeypatch.setattr(binance_producer, "consume_with_reconnect", fake_consume)
    monkeypatch.setattr(binance_producer.asyncio, "sleep", mock_sleep)

    with pytest.raises(asyncio.CancelledError):
        await binance_producer.run_with_kafka_recovery(producer_factory=factory)

    assert factory.call_count == 3
    assert [call.args[0] for call in mock_sleep.await_args_list] == [1, 2]
    producers[0].flush.assert_called_once_with(timeout=2)
    producers[1].flush.assert_called_once_with(timeout=2)
    producers[2].flush.assert_called_once_with(timeout=10)
