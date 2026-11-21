import json
import logging
import os
from datetime import datetime, timezone

from clickhouse_http import post_json_each_row
from ohlc_logic import (
    add_trade,
    create_ohlc_accumulator,
    get_ohlc_result,
    merge_ohlc_accumulators,
    parse_trade,
)
from pyflink.common import Duration, Types, WatermarkStrategy  # type: ignore
from pyflink.common.serialization import SimpleStringSchema  # type: ignore
from pyflink.common.watermark_strategy import TimestampAssigner  # type: ignore
from pyflink.datastream import (  # type: ignore
    CheckpointingMode,
    OutputTag,
    StreamExecutionEnvironment,
)
from pyflink.datastream.checkpoint_config import (  # type: ignore
    ExternalizedCheckpointCleanup,
)
from pyflink.datastream.connectors.kafka import (  # type: ignore
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (  # type: ignore
    AggregateFunction,
    KeyedProcessFunction,
    MapFunction,
    ProcessWindowFunction,
    RuntimeContext,
    SinkFunction,
)
from pyflink.datastream.state import ListStateDescriptor  # type: ignore
from pyflink.datastream.window import Time, TumblingEventTimeWindows  # type: ignore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("flink_ohlc_consumer")

KAFKA_BOOTSTRAP = "kafka:19092"
KAFKA_TOPIC = "binance-trades"

CLICKHOUSE_URL = "http://clickhouse:8123/"
CLICKHOUSE_AUTH = (
    os.environ["CLICKHOUSE_USER"],
    os.environ["CLICKHOUSE_PASSWORD"],
)
WATERMARK_DELAY_SECONDS = int(os.getenv("FLINK_WATERMARK_DELAY_SECONDS", "3"))
WATERMARK_IDLENESS_SECONDS = int(os.getenv("FLINK_WATERMARK_IDLENESS_SECONDS", "30"))
WINDOW_SIZE_SECONDS = int(os.getenv("FLINK_WINDOW_SIZE_SECONDS", "10"))
ALLOWED_LATENESS_SECONDS = int(os.getenv("FLINK_ALLOWED_LATENESS_SECONDS", "10"))

CLICKHOUSE_INSERT_QUERY = "INSERT INTO binance.trades FORMAT JSONEachRow"
CLICKHOUSE_OHLC_INSERT_QUERY = (
    "INSERT INTO binance.trades_ohlc_10s_by_engine FORMAT JSONEachRow"
)
BUFFER_SIZE = int(os.getenv("FLINK_RAW_BUFFER_SIZE", "500"))
FLUSH_INTERVAL_MS = int(os.getenv("FLINK_RAW_FLUSH_INTERVAL_MS", "500"))
OHLC_BUFFER_SIZE = int(os.getenv("FLINK_OHLC_BUFFER_SIZE", "8"))
OHLC_FLUSH_INTERVAL_MS = int(os.getenv("FLINK_OHLC_FLUSH_INTERVAL_MS", "5000"))
LATE_TRADE_LOG_EVERY = int(os.getenv("FLINK_LATE_TRADE_LOG_EVERY", "10000"))
CHECKPOINT_INTERVAL_MS = int(os.getenv("FLINK_CHECKPOINT_INTERVAL_MS", "10000"))
CHECKPOINT_MIN_PAUSE_MS = int(os.getenv("FLINK_CHECKPOINT_MIN_PAUSE_MS", "5000"))
CHECKPOINT_TIMEOUT_MS = int(os.getenv("FLINK_CHECKPOINT_TIMEOUT_MS", "60000"))
CHECKPOINT_STORAGE_URI = os.getenv(
    "FLINK_CHECKPOINT_STORAGE_URI", "file:///opt/flink/checkpoints"
)
PARTITION_DISCOVERY_INTERVAL_MS = os.getenv(
    "FLINK_PARTITION_DISCOVERY_INTERVAL_MS", "10000"
)
KAFKA_CONSUMER_GROUP_ID = os.getenv(
    "FLINK_CONSUMER_GROUP_ID", "flink-ohlc-consumer-by-engine-v2"
)


def build_kafka_source() -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_CONSUMER_GROUP_ID)
        .set_starting_offsets(
            KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.LATEST)
        )
        .set_property(
            "partition.discovery.interval.ms", PARTITION_DISCOVERY_INTERVAL_MS
        )
        .set_property("commit.offsets.on.checkpoint", "true")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def configure_checkpointing(env: StreamExecutionEnvironment) -> None:

    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    config = env.get_checkpoint_config()
    config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    config.set_min_pause_between_checkpoints(CHECKPOINT_MIN_PAUSE_MS)
    config.set_checkpoint_timeout(CHECKPOINT_TIMEOUT_MS)
    config.set_max_concurrent_checkpoints(1)
    config.set_checkpoint_storage_dir(CHECKPOINT_STORAGE_URI)
    config.enable_externalized_checkpoints(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )


class TradeTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: dict, record_timestamp: int) -> int:
        return value["trade_time"]


class LateTradeMonitor(MapFunction):
    def open(self, runtime_context: RuntimeContext) -> None:
        self.count = 0

    def map(self, value: dict) -> str:
        self.count += 1
        if self.count == 1 or self.count % LATE_TRADE_LOG_EVERY == 0:
            log.warning(
                (
                    "Trade trễ quá allowed_lateness; "
                    "late_drop_total=%d (log mỗi %d record)"
                ),
                self.count,
                LATE_TRADE_LOG_EVERY,
            )
        return ""


class OhlcAggregateFunction(AggregateFunction):
    def create_accumulator(self) -> dict:
        return create_ohlc_accumulator()

    def add(self, value: dict, acc: dict) -> dict:
        return add_trade(value, acc)

    def get_result(self, acc: dict) -> dict:
        return get_ohlc_result(acc)

    def merge(self, acc_a: dict, acc_b: dict) -> dict:
        return merge_ohlc_accumulators(acc_a, acc_b)


class OhlcWindowResult(ProcessWindowFunction):
    def process(self, key, context: "ProcessWindowFunction.Context", elements):  # noqa: UP037
        ohlc = list(elements)[0]  # noqa: RUF015
        window = context.window()

        yield json.dumps(
            {
                "symbol": key,
                "window_start": self._format_ts(window.start),
                "window_end": self._format_ts(window.end),
                "open": str(ohlc["open"]),
                "high": str(ohlc["high"]),
                "low": str(ohlc["low"]),
                "close": str(ohlc["close"]),
                "volume": str(ohlc["volume"]),
                "vwap": str(ohlc["vwap"]),
                "trade_count": ohlc["trade_count"],
                "engine": "flink",
            }
        )

    @staticmethod
    def _format_ts(epoch_ms: int) -> str:
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)  # noqa: UP017
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


class ClickHouseTradeWriter(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext) -> None:
        self.buffer = []
        self.buffer_loaded = False
        self.buffer_state = runtime_context.get_list_state(
            ListStateDescriptor("pending-raw-clickhouse", Types.STRING())
        )
        self.timer_registered = False

    def _ensure_buffer_loaded(self) -> None:
        if not self.buffer_loaded:
            self.buffer = list(self.buffer_state.get())
            self.buffer_loaded = True

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):  # noqa: UP037
        self._ensure_buffer_loaded()
        record = parse_trade(value)
        record_out = {
            "symbol": record["symbol"],
            "trade_id": record["trade_id"],
            "price": record["price"],
            "quantity": record["quantity"],
            "trade_time": self._format_trade_time(record["trade_time"]),
            "is_buyer_maker": record["is_buyer_maker"],
            "engine": "flink",
        }
        serialized = json.dumps(record_out)
        self.buffer.append(serialized)
        self.buffer_state.add(serialized)

        if not self.timer_registered:
            next_fire = (
                ctx.timer_service().current_processing_time() + FLUSH_INTERVAL_MS
            )
            ctx.timer_service().register_processing_time_timer(next_fire)
            self.timer_registered = True

        if len(self.buffer) >= BUFFER_SIZE:
            self._flush()

        return []

    def on_timer(self, timestamp: int, ctx: "KeyedProcessFunction.OnTimerContext"):  # noqa: UP037
        self._ensure_buffer_loaded()
        self._flush()
        next_fire = ctx.timer_service().current_processing_time() + FLUSH_INTERVAL_MS
        ctx.timer_service().register_processing_time_timer(next_fire)
        return []

    @staticmethod
    def _format_trade_time(epoch_ms: int) -> str:
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)  # noqa: UP017
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _flush(self, clear_state: bool = True) -> None:
        if not self.buffer:
            return
        body = "\n".join(self.buffer).encode("utf-8")
        post_json_each_row(
            url=CLICKHOUSE_URL,
            auth=CLICKHOUSE_AUTH,
            query=CLICKHOUSE_INSERT_QUERY,
            body=body,
            logger=log,
        )
        log.info("Flink sink: ghi %d record vào ClickHouse", len(self.buffer))
        self.buffer = []
        if clear_state:
            self.buffer_state.clear()

    def close(self) -> None:
        self._flush(clear_state=False)


class ClickHouseOhlcWriter(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext) -> None:
        self.buffer = []
        self.buffer_loaded = False
        self.buffer_state = runtime_context.get_list_state(
            ListStateDescriptor("pending-ohlc-clickhouse", Types.STRING())
        )
        self.timer_registered = False

    def _ensure_buffer_loaded(self) -> None:
        if not self.buffer_loaded:
            self.buffer = list(self.buffer_state.get())
            self.buffer_loaded = True

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):  # noqa: UP037
        self._ensure_buffer_loaded()
        self.buffer.append(value)
        self.buffer_state.add(value)

        if not self.timer_registered:
            next_fire = (
                ctx.timer_service().current_processing_time() + OHLC_FLUSH_INTERVAL_MS
            )
            ctx.timer_service().register_processing_time_timer(next_fire)
            self.timer_registered = True

        if len(self.buffer) >= OHLC_BUFFER_SIZE:
            self._flush()

        return []

    def on_timer(self, timestamp: int, ctx: "KeyedProcessFunction.OnTimerContext"):  # noqa: UP037
        self._ensure_buffer_loaded()
        self._flush()
        next_fire = (
            ctx.timer_service().current_processing_time() + OHLC_FLUSH_INTERVAL_MS
        )
        ctx.timer_service().register_processing_time_timer(next_fire)
        return []

    def _flush(self, clear_state: bool = True) -> None:
        if not self.buffer:
            return
        body = "\n".join(self.buffer).encode("utf-8")
        post_json_each_row(
            url=CLICKHOUSE_URL,
            auth=CLICKHOUSE_AUTH,
            query=CLICKHOUSE_OHLC_INSERT_QUERY,
            body=body,
            logger=log,
        )
        log.info("Flink OHLC sink: ghi %d window vào ClickHouse", len(self.buffer))
        self.buffer = []
        if clear_state:
            self.buffer_state.clear()

    def close(self) -> None:
        self._flush(clear_state=False)


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    configure_checkpointing(env)
    log.info(
        (
            "Flink config: window=%ss watermark=%ss watermark_idleness=%ss "
            "allowed_lateness=%ss "
            "raw_buffer=%s raw_flush=%sms ohlc_buffer=%s ohlc_flush=%sms "
            "consumer_group=%s"
        ),
        WINDOW_SIZE_SECONDS,
        WATERMARK_DELAY_SECONDS,
        WATERMARK_IDLENESS_SECONDS,
        ALLOWED_LATENESS_SECONDS,
        BUFFER_SIZE,
        FLUSH_INTERVAL_MS,
        OHLC_BUFFER_SIZE,
        OHLC_FLUSH_INTERVAL_MS,
        KAFKA_CONSUMER_GROUP_ID,
    )

    kafka_source = build_kafka_source()

    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "binance-kafka-source",
    )

    parsed_stream = raw_stream.map(
        lambda raw: json.dumps(parse_trade(raw)),
        output_type=Types.STRING(),
    )

    (
        parsed_stream.key_by(lambda _: 0, key_type=Types.INT())
        .process(ClickHouseTradeWriter(), output_type=Types.STRING())
        .add_sink(
            SinkFunction("org.apache.flink.streaming.api.functions.sink.DiscardingSink")
        )
    )

    late_data_tag = OutputTag("late-trades", Types.PICKLED_BYTE_ARRAY())

    parsed_dict_stream = raw_stream.map(
        parse_trade,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    watermarked_stream = parsed_dict_stream.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(WATERMARK_DELAY_SECONDS)
        )
        .with_idleness(Duration.of_seconds(WATERMARK_IDLENESS_SECONDS))
        .with_timestamp_assigner(TradeTimestampAssigner())
    )

    ohlc_stream = (
        watermarked_stream.key_by(lambda x: x["symbol"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
        .allowed_lateness(ALLOWED_LATENESS_SECONDS * 1000)
        .side_output_late_data(late_data_tag)
        .aggregate(
            OhlcAggregateFunction(),
            window_function=OhlcWindowResult(),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=Types.STRING(),
        )
    )

    (
        ohlc_stream.key_by(lambda _: 0, key_type=Types.INT())
        .process(ClickHouseOhlcWriter(), output_type=Types.STRING())
        .add_sink(
            SinkFunction("org.apache.flink.streaming.api.functions.sink.DiscardingSink")
        )
    )

    late_stream = ohlc_stream.get_side_output(late_data_tag)
    (
        late_stream.map(LateTradeMonitor(), output_type=Types.STRING()).add_sink(
            SinkFunction("org.apache.flink.streaming.api.functions.sink.DiscardingSink")
        )
    )

    env.execute("binance-flink-ohlc-consumer")


if __name__ == "__main__":
    main()
